//! The traces a run asks for, and the destinations of the derivations and responses.

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
    run::{Run, selection::Reached},
    stdout,
};

/// Reads the query of `--trace-tree` or `--trace-node`: the JSON text
/// itself, an object or an array, or the file holding it.
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

/// Where `--trace-all` streams the response as its parts arrive:
/// into the `--trace-output` file as one trace response, its nodes one per line,
/// or to the standard output as one line per node:
/// the rule instance of a derived fact, and a loaded fact as a fact statement.
struct TraceStream<'a> {
    /// The file, if the trace is written rather than printed.
    path: Option<&'a Path>,
    /// The open destination.
    out: Box<dyn Write>,
    /// The number of nodes written.
    written: usize,
    /// The fact of every node printed, by id, to name the premises of later ones.
    facts: Vec<String>,
    /// The rules of the parts so far, for the end of the file's response.
    rules: Vec<WrittenRule>,
    /// Whether the standard output closed under the stream: the reader
    /// has all it wants, so no further part is derived for it.
    closed: bool,
}

impl<'a> TraceStream<'a> {
    /// Opens the stream on the file if one is named, and on the standard output otherwise,
    /// and begins the file's response.
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

    /// Writes a part of the response.
    fn take(&mut self, part: TracePart) -> Result<(), Diagnostic> {
        let written = self.write(part);
        written.map_err(|error| self.failed(error))
    }

    /// Writes a part's nodes: each on its own line of the file's response,
    /// or printed as a rule instance or a fact statement.
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
        self.rules.extend(part.rules);
        Ok(())
    }

    /// Ends the stream with the traced facts the run named:
    /// the file's response lists them after its rules,
    /// and the standard output notes those that do not hold.
    /// Says where the trace went.
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
            .map_err(|error| unwritable(path, error))?;
        eprintln!("trace: {}", path.display());
        Ok(())
    }

    /// Returns what a failed write amounts to:
    /// the file could not be written,
    /// or the standard output failed, closed if the reader left.
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
    /// Traces what the run asked about: the facts of `--trace` and
    /// `--trace-input`, every derived fact under `--trace-all`, and the
    /// queries of `--trace-tree` and `--trace-node`. Derivations and
    /// responses go to standard output, or into `--trace-output` as
    /// JSON. Does nothing when nothing was asked.
    pub(super) fn trace(&self, engine: &mut Engine) -> Result<(), Diagnostic> {
        let facts = self.traced_facts()?;
        if self.arguments.trace_all {
            self.trace_all(engine, &facts)?;
        } else if !facts.is_empty() {
            let response = engine.trace(&facts)?;
            match &self.arguments.trace_output {
                None => stdout::write(&response.ascii())?,
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

    /// Returns the facts `--trace` and every `--trace-input` file name, parsed.
    fn traced_facts(&self) -> Result<Vec<Fact>, Diagnostic> {
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

    /// Traces every fact of every derived predicate, and the named facts,
    /// streaming the response as it grows:
    /// one line per fact to standard output,
    /// or one trace response, a node per line, into `--trace-output`.
    ///
    /// What the engine does not store whole is skipped, and reported once.
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
        let mut stream = TraceStream::open(self.arguments.trace_output.as_deref())?;
        let named = engine.trace_predicates(&derived.predicates, facts, |part| stream.take(part));
        if stream.closed {
            return Ok(());
        }
        stream.finish(named?)
    }

    /// Writes a query's response, as JSON, to the standard output or
    /// into `--trace-output`.
    fn write_query_response(&self, response: &impl Serialize) -> Result<(), Diagnostic> {
        let json = json(response);
        match &self.arguments.trace_output {
            None => stdout::write(&format!("{json}\n")),
            Some(path) => self.write_trace(path, json.as_bytes()),
        }
    }

    /// Writes a trace document to the `--trace-output` file, and says where it went.
    fn write_trace(&self, path: &Path, bytes: &[u8]) -> Result<(), Diagnostic> {
        std::fs::write(path, bytes).map_err(|error| unwritable(path, error))?;
        eprintln!("trace: {}", path.display());
        Ok(())
    }
}

/// Splits a text into the facts it separates by `;`, trimmed, without empty ones.
///
/// A `;` inside a quoted string or an IRI in angle brackets separates nothing.
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

    #[test]
    fn semicolons_in_values_separate_nothing() {
        assert_eq!(split_facts("p(a);q(b) ; "), ["p(a)", "q(b)"]);
        assert_eq!(
            split_facts(r#"t(1, "a;b");t(2, "\";");u(<http://e/a;b>)"#),
            [r#"t(1, "a;b")"#, r#"t(2, "\";")"#, "u(<http://e/a;b>)"]
        );
    }
}
