//! The traces a run asks for, and the destinations of the derivations and responses.

use std::{
    collections::HashMap,
    io::{self, BufWriter, Write},
    path::Path,
};

use nemo::{
    engine::Engine,
    model::Fact,
    trace::{
        Derivation, TraceNode, TraceResponse, TracedFact, VERSION, WrittenRule,
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

/// Where `--trace-all` streams its responses as they arrive:
/// into the `--trace-output` file as one trace response, its nodes one per line,
/// or to the standard output as one inference per line.
///
/// Each derived fact is written once, however many responses hold it.
struct TraceStream<'a> {
    /// The file, if the trace is written rather than printed.
    path: Option<&'a Path>,
    /// The open destination.
    out: Box<dyn Write>,
    /// The facts written, each with its id in the file's response.
    ids: HashMap<String, usize>,
    /// The rules of the file's response.
    rules: Vec<WrittenRule>,
    /// The position of each rule in the file's response, by its statement id.
    listed: HashMap<usize, usize>,
    /// Whether the standard output closed under the stream: the reader
    /// has all it wants, so no further page is derived for it.
    closed: bool,
}

impl<'a> TraceStream<'a> {
    /// Opens the stream on the file if one is named, and on the standard output otherwise.
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
            ids: HashMap::new(),
            rules: Vec::new(),
            listed: HashMap::new(),
            closed: false,
        })
    }

    /// Writes the traced facts the run named, before any page.
    fn start(&mut self, named: TraceResponse) -> Result<(), Diagnostic> {
        let written = match self.path {
            Some(_) => self.begin(named),
            None => self.print(&named, true),
        };
        written.map_err(|error| self.failed(error))
    }

    /// Writes one page of traced rows.
    fn take(&mut self, response: TraceResponse) -> Result<(), Diagnostic> {
        let written = match self.path {
            Some(_) => self.append(response).map(|_| ()),
            None => self.print(&response, false),
        };
        written.map_err(|error| self.failed(error))
    }

    /// Prints each derived fact of a response not printed before as its inference,
    /// and, if asked, each requested fact that does not hold as a note.
    fn print(&mut self, response: &TraceResponse, notes: bool) -> io::Result<()> {
        for id in 0..response.nodes.len() {
            let proof = response.node(id);
            if proof.derivation().is_some() && !self.ids.contains_key(proof.fact()) {
                self.ids.insert(proof.fact().to_owned(), self.ids.len());
                writeln!(self.out, "{proof}")?;
            }
        }
        if notes {
            for traced in response.facts.iter().filter(|traced| traced.node.is_none()) {
                writeln!(self.out, "{} does not hold", traced.fact)?;
            }
        }
        Ok(())
    }

    /// Begins the file's response with the named facts and their derivations.
    fn begin(&mut self, named: TraceResponse) -> io::Result<()> {
        let facts = named.facts.clone();
        write!(self.out, r#"{{"version":{VERSION},"facts":"#)?;
        let ids = self.renumbered(&named);
        let facts: Vec<TracedFact> = facts
            .into_iter()
            .map(|traced| TracedFact {
                node: traced.node.map(|node| ids[node]),
                ..traced
            })
            .collect();
        write!(self.out, "{}", json(&facts))?;
        write!(self.out, r#","nodes":["#)?;
        self.append(named).map(|_| ())
    }

    /// Returns the file's id for every node of a response:
    /// the id of the fact if it was written before, and the next free one otherwise.
    fn renumbered(&self, response: &TraceResponse) -> Vec<usize> {
        let mut next = self.ids.len();
        let mut fresh = HashMap::new();
        response
            .nodes
            .iter()
            .map(|node| {
                self.ids.get(&node.fact).copied().unwrap_or_else(|| {
                    *fresh.entry(node.fact.as_str()).or_insert_with(|| {
                        next += 1;
                        next - 1
                    })
                })
            })
            .collect()
    }

    /// Writes the nodes of a response the file does not hold yet, each on its own line,
    /// its premises and rule renumbered into the file's response, and returns the file's ids.
    fn append(&mut self, response: TraceResponse) -> io::Result<Vec<usize>> {
        let ids = self.renumbered(&response);
        for (node, &id) in response.nodes.into_iter().zip(&ids) {
            if self.ids.contains_key(&node.fact) {
                continue;
            }
            let derivation = node.derivation.map(|derivation| Derivation {
                rule: self.listed(&response.rules[derivation.rule]),
                premises: derivation
                    .premises
                    .iter()
                    .map(|&premise| ids[premise])
                    .collect(),
                ..derivation
            });
            let separator = if id == 0 { "\n" } else { ",\n" };
            let node = TraceNode {
                fact: node.fact,
                derivation,
            };
            write!(self.out, "{separator}{}", json(&node))?;
            self.ids.insert(node.fact, id);
        }
        Ok(ids)
    }

    /// Returns the position of a rule in the file's response, listing it if it is new.
    fn listed(&mut self, rule: &WrittenRule) -> usize {
        *self.listed.entry(rule.rule).or_insert_with(|| {
            self.rules.push(rule.clone());
            self.rules.len() - 1
        })
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

    /// Ends the response in the file and flushes the stream, and says where the trace went.
    fn finish(mut self) -> Result<(), Diagnostic> {
        match self.path {
            None => self.out.flush().or_else(stdout::interrupted),
            Some(path) => {
                let rules = json(&self.rules);
                write!(self.out, "\n],\"rules\":{rules}}}\n")
                    .and_then(|()| self.out.flush())
                    .map_err(|error| unwritable(path, error))?;
                eprintln!("trace: {}", path.display());
                Ok(())
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

    /// Traces every fact of every derived predicate (and the named facts first),
    /// streaming page by page:
    /// one inference per line to standard output,
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
        let derived = derived.predicates;
        let named = if facts.is_empty() {
            TraceResponse {
                version: VERSION,
                facts: Vec::new(),
                nodes: Vec::new(),
                rules: Vec::new(),
            }
        } else {
            engine.trace(facts)?
        };
        let mut stream = TraceStream::open(self.arguments.trace_output.as_deref())?;
        let streamed = stream.start(named).and_then(|()| {
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
