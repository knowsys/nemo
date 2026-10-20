//! The traces a run asks for, and the destinations of the trees and responses.

use std::{
    collections::HashSet,
    io::{self, BufWriter, Write},
    path::Path,
};

use nemo::{
    engine::Engine,
    model::Fact,
    trace::{
        TraceResponse,
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

/// Rows traced per page under `--trace-all`.
///
/// A page's proofs are held at once, and each page searches anew:
/// a smaller page holds less, a larger one proves shared premises fewer times.
const TRACE_BUDGET: usize = 1 << 6;

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

/// Where `--trace-all` streams its pages: the `--trace-output` file as
/// JSON lines, or the standard output as trees.
struct TraceStream<'a> {
    /// The file, if the trace is written rather than printed.
    path: Option<&'a Path>,
    /// The open destination.
    out: Box<dyn Write>,
    /// The conclusions written, so a fact traced as a premise and again
    /// as a root goes into the file once,
    /// or the roots printed, so a fact named and derived prints once.
    seen: HashSet<String>,
    /// Whether the standard output closed under the stream: the reader
    /// has all it wants, so no further page is derived for it.
    closed: bool,
}

impl<'a> TraceStream<'a> {
    /// Opens the stream: the file if one is named, the standard output otherwise.
    fn open(path: Option<&'a Path>) -> Result<Self, Diagnostic> {
        let out: Box<dyn Write> = match path {
            None => Box::new(BufWriter::new(io::stdout().lock())),
            Some(path) => Box::new(BufWriter::new(
                std::fs::File::create(path).map_err(|error| unwritable(path, error))?,
            )),
        };
        Ok(Self {
            path,
            out,
            seen: HashSet::new(),
            closed: false,
        })
    }

    /// Writes one page:
    /// its inferences as JSON lines, each conclusion once, into the file,
    /// and its trees to the standard output, each root once.
    fn take(&mut self, mut response: TraceResponse) -> Result<(), Diagnostic> {
        let written = match self.path {
            Some(_) => response
                .inferences()
                .inferences
                .into_iter()
                .filter(|inference| self.seen.insert(inference.conclusion.clone()))
                .try_for_each(|inference| {
                    let line = serde_json::to_string(&inference).expect("an inference serializes");
                    writeln!(self.out, "{line}")
                }),
            None => {
                response
                    .facts
                    .retain(|traced| self.seen.insert(traced.fact.to_string()));
                write!(self.out, "{}", response.ascii())
            }
        };
        written.map_err(|error| self.failed(error))
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

    /// Flushes the stream, and says where the trace went.
    fn finish(mut self) -> Result<(), Diagnostic> {
        match self.path {
            None => self.out.flush().or_else(stdout::interrupted),
            Some(path) => {
                self.out.flush().map_err(|error| unwritable(path, error))?;
                eprintln!("trace: {}", path.display());
                Ok(())
            }
        }
    }
}

impl Run<'_> {
    /// Traces what the run asked about: the facts of `--trace` and
    /// `--trace-input`, every derived fact under `--trace-all`, and the
    /// queries of `--trace-tree` and `--trace-node`. Proof trees and
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
                Some(path) => {
                    let json = serde_json::to_string(&response.inferences())
                        .expect("the inference list serializes");
                    self.write_trace(path, json.as_bytes())?;
                }
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

    /// Traces every fact of every derived predicate (and the named facts first),
    /// streaming page by page:
    /// ASCII trees to standard output,
    /// or JSON lines (one inference per line, each conclusion once) into `--trace-output`.
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
        let derived = derived.predicates;
        let mut stream = TraceStream::open(self.arguments.trace_output.as_deref())?;
        let named = if facts.is_empty() {
            Ok(())
        } else {
            engine
                .trace(facts)
                .and_then(|response| stream.take(response))
        };
        let streamed = named.and_then(|()| {
            engine.trace_predicates(&derived, TRACE_BUDGET, |response| stream.take(response))
        });
        if stream.closed {
            return Ok(());
        }
        streamed?;
        stream.finish()
    }

    /// Writes a query's response, as JSON, to the standard output or
    /// into `--trace-output`.
    fn write_query_response(&self, response: &impl Serialize) -> Result<(), Diagnostic> {
        let json = serde_json::to_string(response).expect("the response serializes");
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
