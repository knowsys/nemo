//! The print path: the results as facts on the standard output.

use std::io::{BufWriter, Write};

use nemo::engine::{Engine, PredicateCursor};
use nemo_physical::diagnostics::Diagnostic;

use crate::{
    arguments::Mode,
    run::{Run, selection::Selection},
    stdout,
};

impl Run<'_> {
    /// Prints the results to standard output, one fact per row in term syntax,
    /// so what a run prints reads back as a program.
    ///
    /// The print mode picks the predicates by role.
    /// Bare, the export mode picks them as it does for files,
    /// except that `keep` prints the program's outputs:
    /// what it declares `@output` or exports, or every derived predicate if it declares neither.
    /// A program a run only looks at declares what it is about, it does not name files.
    pub(super) fn print(&self, engine: &mut Engine, mode: Mode) -> Result<(), Diagnostic> {
        let selection = Selection::of(match mode {
            Mode::Keep => self.arguments.export,
            mode => mode,
        });
        // The outputs, then what the selection reaches, each predicate
        // once.
        let mut predicates: Vec<String> = match selection {
            Selection::Declared => engine.outputs(),
            _ => Vec::new(),
        };
        let reached = selection.reaches(engine);
        if let Some(warning) = reached.warning() {
            self.show(&warning);
        }
        for predicate in reached.predicates {
            if !predicates.contains(&predicate) {
                predicates.push(predicate);
            }
        }

        let stdout = std::io::stdout().lock();
        let mut out = BufWriter::new(stdout);
        for predicate in &predicates {
            // A declared predicate no statement used holds no table:
            // nothing to print, as its export is an empty file.
            if engine.predicate(predicate).is_none() {
                continue;
            }
            // Names and values print as the program reads them back,
            // so that a printed fact can be traced as printed.
            let name = engine.predicate_text(predicate);
            let mut cursor = engine.cursor(predicate)?;
            while !cursor.is_done() {
                let chunk = engine.read_chunk(&mut cursor, PredicateCursor::BUDGET)?;
                for row in chunk.rows() {
                    let terms: Vec<String> = row
                        .iter()
                        .map(|value| engine.term_text(value.as_ref()))
                        .collect();
                    if let Err(error) = writeln!(out, "{name}({}) .", terms.join(", ")) {
                        return stdout::interrupted(error);
                    }
                }
            }
        }
        out.flush().or_else(stdout::interrupted)
    }
}
