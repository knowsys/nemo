//! Printing the results to standard output as facts.

use std::io::{BufWriter, Write};

use nemo::engine::{Engine, PredicateCursor};
use nemo_physical::diagnostics::Diagnostic;

use crate::{
    arguments::Mode,
    run::{Run, selection::Selection},
    stdout,
};

impl Run<'_> {
    /// Prints the results to standard output as facts in term syntax, one per row,
    /// so that the printed text parses back as a program.
    ///
    /// `mode` selects the predicates by role, except for [Mode::Keep],
    /// with which the export mode selects them as it does for files.
    /// If the export mode is [Mode::Keep] too, the program's [outputs](Engine::outputs)
    /// are printed as well, since a program whose results are printed declares the
    /// predicates it is about with `@output` rather than with exports.
    pub(super) fn print(&self, engine: &mut Engine, mode: Mode) -> Result<(), Diagnostic> {
        let selection = Selection::of(match mode {
            Mode::Keep => self.arguments.export,
            mode => mode,
        });
        // The outputs come first, then the selected predicates, each predicate once.
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
            // A declared predicate that no statement uses has no table. Nothing is printed
            // for it, just as its export writes an empty file.
            if engine.predicate(predicate).is_none() {
                continue;
            }
            // Names and values are written as the program would parse them,
            // so that a printed fact can be passed to `--trace` as it is.
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
