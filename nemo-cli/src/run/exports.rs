//! Settling the exports of a run: which predicates it writes, and which files it must not
//! write.

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

/// An export with a file of its own, which hints about an export's file show as an example.
const OWN_FILE: &str = "`@export p to csv { resource = \"p.csv\" } .`";

/// A writer of a file in the export directory.
#[derive(Debug, Clone, Copy)]
enum Claimant<'a> {
    /// The export of the named predicate.
    Export(&'a str),
    /// The HTML report.
    Report,
}

impl Run<'_> {
    /// Returns the exports of the run, checked before reasoning.
    ///
    /// The run fails here rather than after the work if an export would write to standard
    /// output beside the JSON report, if two exports, or an export and the HTML report,
    /// would write the same file, or if one of them would replace an existing file without
    /// `--overwrite`.
    /// A run that prints its results has no exports, so only the HTML report is checked then.
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

    /// Returns the exports of the predicates that the export mode selects.
    ///
    /// These are the program's exports of the selected predicates, as declared,
    /// followed by an export to comma-separated values for each selected predicate without one.
    /// A selection by role only adds exports, so a declared export is kept as written.
    /// The predicates that the selection skips are reported in one warning.
    /// With `--gzip`, every export is compressed.
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
                // The file name follows from the compression, so setting the compression is
                // enough.
                specification.compression = Compression::Gzip;
            }
        }
        specifications
    }

    /// Checks that no two exports, and no export and the HTML report, write the same file,
    /// and that none of them replaces an existing file without `--overwrite`.
    fn vacancy(&self, specifications: &[ExportSpec], level: Report) -> Result<(), Diagnostic> {
        let mut taken: HashMap<String, Claimant<'_>> = HashMap::new();
        if level == Report::Html {
            taken.insert(file_key(REPORT_PAGE), Claimant::Report);
            self.unclaimed(REPORT_PAGE, None)?;
        }
        for specification in specifications {
            // An export to standard output writes no file, so it cannot share a file with
            // another writer or replace one.
            if specification.is_stdout() {
                continue;
            }
            let name = specification.file_name();
            let predicate = specification.predicate.as_str();
            // If two writers shared a file, the output written first would be lost.
            // This is an error even with `--overwrite`, which allows replacing the output of
            // an earlier run, but not the output of this one.
            if let Some(other) = taken.insert(file_key(&name), Claimant::Export(predicate)) {
                return Err(collision(other, predicate, &name));
            }
            self.unclaimed(&name, Some(predicate))?;
        }
        Ok(())
    }

    /// Returns an error if the file `name` already exists in the export directory
    /// and `--overwrite` is not given.
    ///
    /// `predicate` is the predicate whose export would write the file,
    /// or `None` for the HTML report.
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

/// Returns the error for an export of `predicate` that would write `file`,
/// which `earlier` already writes.
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

/// Returns a key that is the same for two export file names that name the same file.
///
/// The key drops `.` segments and resolves `..` segments.
/// On Windows and macOS, whose file systems ignore case by default, it is also lowercased.
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
