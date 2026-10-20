//! The export path: what the run writes out, and the files it must not write.

use std::collections::HashMap;

use nemo::{
    diagnostics::AboutPredicate,
    engine::{Engine, ExportSpec},
};
use nemo_io::compression::Compression;
use nemo_physical::diagnostics::Diagnostic;

use crate::{
    arguments::Report,
    error::CliError,
    run::{Run, document::REPORT_PAGE, selection::Selection, stdout_conflict},
};

/// The example a hint about an export's file shows.
const OWN_FILE: &str = "`@export p to csv { resource = \"p.csv\" } .`";

/// What writes a file into the export directory.
#[derive(Debug, Clone, Copy)]
enum Claimant<'a> {
    /// The export of the predicate.
    Export(&'a str),
    /// The HTML report.
    Report,
}

impl Run<'_> {
    /// Returns what the run writes out, settled as soon as the program has
    /// loaded: a run that must not overwrite an export file or the HTML
    /// report fails at once rather than after the work. Printing writes no
    /// export files, so only the report is checked then.
    pub(super) fn exports(
        &self,
        engine: &Engine,
        level: Report,
    ) -> Result<Vec<ExportSpec>, Diagnostic> {
        let specifications = if self.arguments.printing().is_some() {
            Vec::new()
        } else {
            self.specifications(engine)
        };
        if level == Report::Json
            && let Some(specification) = specifications
                .iter()
                .find(|specification| specification.is_stdout())
        {
            return Err(stdout_conflict(&format!(
                "the export of `{}` to standard output",
                specification.predicate
            ))
            .with_hint(format!("give the export a file, as {OWN_FILE}")));
        }
        self.vacancy(&specifications, level)?;
        Ok(specifications)
    }

    /// Returns what the run writes out, as the export mode asks:
    /// the declared exports the selection reaches, as written,
    /// then the predicates it adds as comma-separated values.
    ///
    /// What the program says about a predicate it exports still holds,
    /// since a role-driven selection adds the rest, it does not overrule what was written.
    /// What the selection skips is reported once.
    fn specifications(&self, engine: &Engine) -> Vec<ExportSpec> {
        let reached = Selection::of(self.arguments.export).reaches(engine);
        if let Some(warning) = reached.warning() {
            self.show(&warning);
        }
        let selected = reached.predicates;
        let declared = |predicate: &str| {
            engine
                .exports()
                .iter()
                .any(|specification| specification.predicate == predicate)
        };
        let mut specifications: Vec<ExportSpec> = engine
            .exports()
            .iter()
            .filter(|specification| selected.contains(&specification.predicate))
            .cloned()
            .collect();
        for predicate in selected {
            if !declared(&predicate) {
                specifications.push(ExportSpec::csv(predicate));
            }
        }
        if self.arguments.gzip {
            for specification in &mut specifications {
                // The name follows from the compression, so saying it is
                // gzipped is the whole of it.
                specification.compression = Compression::Gzip;
            }
        }
        specifications
    }

    /// Checks that no two exports, nor an export and the HTML report, write into one file,
    /// and that none of them writes over an existing file unless the run was told to.
    fn vacancy(&self, specifications: &[ExportSpec], level: Report) -> Result<(), Diagnostic> {
        let mut taken: HashMap<String, Claimant<'_>> = HashMap::new();
        if level == Report::Html {
            taken.insert(file_key(REPORT_PAGE), Claimant::Report);
            self.unclaimed(REPORT_PAGE, None)?;
        }
        for specification in specifications {
            // Standard output is not a file: nothing to collide with or
            // to protect.
            if specification.is_stdout() {
                continue;
            }
            let name = specification.file_name();
            let predicate = specification.predicate.as_str();
            // Two writers sharing a file would leave whichever was
            // written first with nothing to show for it. This holds
            // even under --overwrite: it permits replacing an earlier
            // run's output, not discarding this one's.
            if let Some(other) = taken.insert(file_key(&name), Claimant::Export(predicate)) {
                return Err(collision(other, predicate, &name));
            }
            self.unclaimed(&name, Some(predicate))?;
        }
        Ok(())
    }

    /// Returns an error if a file of the export directory exists already
    /// and the run was not told to replace it,
    /// naming the predicate whose export would write it, if one would.
    fn unclaimed(&self, name: &str, predicate: Option<&str>) -> Result<(), Diagnostic> {
        if self.arguments.overwrite {
            return Ok(());
        }
        let path = self.arguments.export_dir.join(name);
        if !path.exists() {
            return Ok(());
        }
        let problem = CliError::ExportExists {
            path: path.display().to_string(),
        }
        .diagnostic();
        Err(match predicate {
            Some(predicate) => problem.with_predicate(predicate.to_owned()),
            None => problem,
        })
    }
}

/// Returns the error for an export of the given predicate
/// that would write the file an earlier claimant writes.
fn collision(earlier: Claimant<'_>, predicate: &str, file: &str) -> Diagnostic {
    let (claimants, hint) = match earlier {
        Claimant::Report => (
            format!("`{predicate}` and the HTML report"),
            format!("give the export another file, as {OWN_FILE}"),
        ),
        Claimant::Export(other) if other == predicate => (
            format!("two exports of `{predicate}`"),
            format!("give one of them its own file, as {OWN_FILE}"),
        ),
        Claimant::Export(other) => (
            format!("`{other}` and `{predicate}`"),
            format!("give one of them its own file, as {OWN_FILE}"),
        ),
    };
    CliError::ExportCollision {
        claimants,
        file: file.to_owned(),
    }
    .diagnostic()
    .with_hint(hint)
}

/// Returns the key under which two export file names reach the same file:
/// `.` segments dropped, `..` segments resolved,
/// and the case folded on platforms whose file systems ignore it by default.
fn file_key(name: &str) -> String {
    let mut segments: Vec<&str> = Vec::new();
    for segment in name.split(['/', '\\']) {
        match segment {
            "" | "." => {}
            ".." if segments.last().is_some_and(|last| *last != "..") => {
                segments.pop();
            }
            other => segments.push(other),
        }
    }
    let key = segments.join("/");
    if cfg!(any(windows, target_os = "macos")) {
        key.to_lowercase()
    } else {
        key
    }
}
