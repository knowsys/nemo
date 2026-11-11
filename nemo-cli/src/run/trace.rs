//! Tracing what the trace options ask for, and writing the derivations and query responses.

use std::{
    io::{self, BufWriter, Write},
    path::Path,
};

use nemo::{
    engine::Engine,
    model::Fact,
    trace::{
        TracePart, TracedFact, VERSION, WrittenRule,
        query::{TableEntriesForTreeNodesQuery, TreeForTableQuery},
    },
};
use nemo_physical::diagnostics::Diagnostic;
use serde::Serialize;

use crate::{
    error::CliError,
    run::{Part, Run, selection::Reached},
    stdout,
};

/// Reads the query of `--trace-tree` or `--trace-node` from `given`.
///
/// `given` is either the query's JSON text, recognized by an opening `{` or `[`,
/// or the path of a file that contains it.
fn read_query<Q: for<'de> serde::Deserialize<'de>>(given: &str) -> Result<Q, Diagnostic> {
    let text = if given.trim_start().starts_with(['{', '[']) {
        Ok(given.to_owned())
    } else {
        std::fs::read_to_string(given).map_err(|error| error.to_string())
    };
    text.and_then(|text| serde_json::from_str::<Q>(&text).map_err(|error| error.to_string()))
        .map_err(|reason| {
            CliError::TraceQueryUnreadable {
                path: given.to_owned(),
                reason,
            }
            .diagnostic()
        })
}

/// Returns the diagnostic for a `--trace-output` file that could not be written.
fn unwritable(path: &Path, error: io::Error) -> Diagnostic {
    CliError::TraceUnwritable {
        path: path.display().to_string(),
        reason: error.to_string(),
    }
    .diagnostic()
}

/// The destination to which `--trace-all` writes the trace as its parts arrive.
///
/// - With `--trace-output`, the parts are written to that file as one trace response,
///   with one node per line.
/// - Otherwise, they are printed to standard output, one line per node.
///   A derived fact is printed as its rule instance, and a loaded fact as a fact statement.
struct TraceStream<'a> {
    /// The file, if the trace is written rather than printed.
    path: Option<&'a Path>,
    /// The writer of the file or of standard output.
    out: Box<dyn Write>,
    /// The number of nodes written.
    written: usize,
    /// The fact of every printed node, indexed by node id, to write the premises of later
    /// nodes.
    facts: Vec<String>,
    /// The rules of the parts written to the file so far,
    /// which are listed at the end of the file's response.
    rules: Vec<WrittenRule>,
    /// Whether a write failed because standard output was closed,
    /// as happens when its reader wants no more output.
    ///
    /// The failed write stops the trace, so no further part is derived,
    /// and the trace then ends without an error.
    closed: bool,
}

impl<'a> TraceStream<'a> {
    /// Opens the stream on the file at `path`, or on standard output if there is none.
    ///
    /// For a file, it writes the beginning of the trace response.
    fn open(path: Option<&'a Path>) -> Result<Self, Diagnostic> {
        let out: Box<dyn Write> = match path {
            None => Box::new(BufWriter::new(io::stdout().lock())),
            Some(path) => Box::new(BufWriter::new(
                std::fs::File::create(path).map_err(|error| unwritable(path, error))?,
            )),
        };
        let mut stream = Self {
            path,
            out,
            written: 0,
            facts: Vec::new(),
            rules: Vec::new(),
            closed: false,
        };
        if let Some(path) = path {
            write!(stream.out, r#"{{"version":{VERSION},"nodes":["#)
                .map_err(|error| unwritable(path, error))?;
        }
        Ok(stream)
    }

    /// Writes one part of the trace, and returns a diagnostic if the write fails.
    fn take(&mut self, part: TracePart) -> Result<(), Diagnostic> {
        let written = self.write(part);
        written.map_err(|error| self.failed(error))
    }

    /// Writes the nodes of `part`, each on its own line.
    ///
    /// In a file, a node is written as JSON.
    /// On standard output, a derived fact is written as its rule instance,
    /// and a loaded fact as a fact statement.
    fn write(&mut self, part: TracePart) -> io::Result<()> {
        for node in part.nodes {
            if self.path.is_some() {
                let separator = if self.written == 0 { "\n" } else { ",\n" };
                write!(self.out, "{separator}{}", json(&node))?;
            } else {
                let line = match &node.derivation {
                    Some(derivation) => node.inference(
                        derivation
                            .premises
                            .iter()
                            .map(|&premise| self.facts[premise].as_str()),
                    ),
                    None => format!("{} .", node.fact),
                };
                writeln!(self.out, "{line}")?;
                self.facts.push(node.fact);
            }
            self.written += 1;
        }
        // Only the file's response lists the rules,
        // since on standard output each rule instance already shows its rule.
        if self.path.is_some() {
            self.rules.extend(part.rules);
        }
        Ok(())
    }

    /// Ends the stream with `named`, the results for the facts given to `--trace` and
    /// `--trace-input`.
    ///
    /// In a file, the response lists them after its rules.
    /// On standard output, a line is printed for each of them that does not hold.
    fn finish(mut self, named: Vec<TracedFact>) -> Result<(), Diagnostic> {
        let Some(path) = self.path else {
            for traced in named.iter().filter(|traced| traced.node.is_none()) {
                writeln!(self.out, "{} does not hold", traced.fact)
                    .map_err(|error| self.failed(error))?;
            }
            return self.out.flush().or_else(stdout::interrupted);
        };
        let rules = json(&self.rules);
        let facts = json(&named);
        write!(self.out, "\n],\"rules\":{rules},\"facts\":{facts}}}\n")
            .and_then(|()| self.out.flush())
            .map_err(|error| unwritable(path, error))
    }

    /// Ends the stream of a trace that failed, and removes the file with its unfinished
    /// response.
    fn abandon(self) {
        let Self { path, out, .. } = self;
        // The file is closed before it is removed.
        drop(out);
        if let Some(path) = path {
            // The run reports the trace's failure, not a failure to remove the file.
            std::fs::remove_file(path).ok();
        }
    }

    /// Returns the diagnostic for a failed write.
    ///
    /// A write to standard output that failed on a closed pipe also sets
    /// [closed](Self::closed).
    fn failed(&mut self, error: io::Error) -> Diagnostic {
        match self.path {
            Some(path) => unwritable(path, error),
            None => {
                self.closed = error.kind() == io::ErrorKind::BrokenPipe;
                CliError::PrintFailed {
                    reason: error.to_string(),
                }
                .diagnostic()
            }
        }
    }
}

/// Returns a value of the trace format as JSON.
fn json(value: &impl Serialize) -> String {
    serde_json::to_string(value).expect("trace responses serialize")
}

impl Run<'_> {
    /// Traces what the trace options ask for, and writes the results.
    ///
    /// It traces `facts`, which are those of `--trace` and `--trace-input`,
    /// every derived fact with `--trace-all`,
    /// and the queries of `--trace-tree` and `--trace-node`.
    /// The derivations and responses are printed to standard output,
    /// or written as JSON to the `--trace-output` file.
    /// It does nothing if no trace option is given.
    pub(super) fn trace(&self, engine: &mut Engine, facts: &[Fact]) -> Result<(), Diagnostic> {
        if self.arguments.trace_all {
            self.trace_all(engine, facts)?;
        } else if !facts.is_empty() {
            let response = engine.trace(facts)?;
            match &self.arguments.trace_output {
                None => {
                    self.begin(Part::Trace);
                    stdout::write(&response.ascii())?
                }
                Some(path) => self.write_trace(path, json(&response).as_bytes())?,
            }
        }
        if let Some(given) = &self.arguments.trace_tree {
            let query: TreeForTableQuery = read_query(given)?;
            let response = engine.tree_for_table(query)?;
            self.write_query_response(&response)?;
        }
        if let Some(given) = &self.arguments.trace_node {
            let query: TableEntriesForTreeNodesQuery = read_query(given)?;
            let response = engine.table_entries_for_tree_nodes(query)?;
            self.write_query_response(&response)?;
        }
        Ok(())
    }

    /// Returns the parsed facts of `--trace` and of every `--trace-input` file.
    ///
    /// Fails if a file cannot be read, or if a text is not a fact.
    pub(super) fn traced_facts(&self) -> Result<Vec<Fact>, Diagnostic> {
        let mut texts: Vec<String> = self
            .arguments
            .trace
            .iter()
            .flat_map(|text| split_facts(text))
            .collect();
        for path in &self.arguments.trace_input {
            let content = std::fs::read_to_string(path).map_err(|error| {
                CliError::TraceInputUnreadable {
                    path: path.display().to_string(),
                    reason: error.to_string(),
                }
                .diagnostic()
            })?;
            texts.extend(split_facts(&content));
        }
        texts.retain(|text| !text.is_empty());
        texts
            .iter()
            .map(|text| {
                Fact::parse_loose(text).map_err(|problem| {
                    CliError::NotAFact {
                        text: text.clone(),
                        reason: problem.message().to_owned(),
                    }
                    .diagnostic()
                })
            })
            .collect()
    }

    /// Traces every fact of every derived predicate, and also `facts`,
    /// and writes the trace through a [TraceStream] as it grows.
    ///
    /// The derived predicates that the engine does not store whole are skipped,
    /// and reported in one warning.
    fn trace_all(&self, engine: &mut Engine, facts: &[Fact]) -> Result<(), Diagnostic> {
        let derived = Reached::split(
            engine,
            engine
                .predicates()
                .into_iter()
                .filter(|info| info.role.is_derived())
                .map(|info| info.name)
                .collect(),
        );
        if let Some(warning) = derived.warning() {
            self.show(&warning);
        }
        let file = self.arguments.trace_output.as_deref();
        if file.is_none() {
            self.begin(Part::Trace);
        }
        let mut stream = TraceStream::open(file)?;
        let named = engine.trace_predicates(&derived.predicates, facts, |part| stream.take(part));
        if stream.closed {
            return Ok(());
        }
        match named {
            Ok(named) => {
                stream.finish(named)?;
                if let Some(path) = file {
                    self.note_trace_file(path);
                }
                Ok(())
            }
            Err(problem) => {
                stream.abandon();
                Err(problem)
            }
        }
    }

    /// Writes a query's response as JSON, to standard output or to the `--trace-output` file.
    fn write_query_response(&self, response: &impl Serialize) -> Result<(), Diagnostic> {
        let json = json(response);
        match &self.arguments.trace_output {
            None => {
                self.begin(Part::Trace);
                stdout::write(&format!("{json}\n"))
            }
            Some(path) => self.write_trace(path, json.as_bytes()),
        }
    }

    /// Writes a trace document to the `--trace-output` file at `path`,
    /// and prints the path to the error stream.
    fn write_trace(&self, path: &Path, bytes: &[u8]) -> Result<(), Diagnostic> {
        std::fs::write(path, bytes).map_err(|error| unwritable(path, error))?;
        self.note_trace_file(path);
        Ok(())
    }

    /// Prints the path of the trace file that the run wrote to the error stream.
    fn note_trace_file(&self, path: &Path) {
        self.begin(Part::Summary);
        eprintln!("trace: {}", path.display());
    }
}

/// Splits a text at `;` into trimmed facts, and drops the empty ones.
///
/// A `;` inside a quoted string or inside angle brackets does not separate facts.
fn split_facts(text: &str) -> Vec<String> {
    let mut facts = Vec::new();
    let (mut start, mut quoted, mut escaped, mut bracketed) = (0, false, false, false);
    for (position, character) in text.char_indices() {
        match character {
            _ if escaped => escaped = false,
            '\\' if quoted => escaped = true,
            '"' if !bracketed => quoted = !quoted,
            '<' if !quoted => bracketed = true,
            '>' if !quoted => bracketed = false,
            ';' if !quoted && !bracketed => {
                facts.push(&text[start..position]);
                start = position + 1;
            }
            _ => {}
        }
    }
    facts.push(&text[start..]);
    facts
        .into_iter()
        .map(str::trim)
        .filter(|fact| !fact.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::split_facts;

    /// A `;` separates facts, except inside a quoted string or an IRI in angle brackets.
    #[test]
    fn semicolons_in_values_separate_nothing() {
        assert_eq!(split_facts("p(a);q(b) ; "), ["p(a)", "q(b)"]);
        assert_eq!(
            split_facts(r#"t(1, "a;b");t(2, "\";");u(<http://e/a;b>)"#),
            [r#"t(1, "a;b")"#, r#"t(2, "\";")"#, "u(<http://e/a;b>)"]
        );
    }
}
