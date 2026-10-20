//! What the tests share: programs parsed from source, engines reasoned
//! to their fixpoint, a predicate's rows as text, a responder to
//! the requests an engine suspends for, and temporary directories.

mod temp_directory;

pub(crate) use temp_directory::TempDirectory;

use std::{cell::RefCell, sync::Arc};

use nemo::{
    engine::{
        Engine, PredicateRole,
        remote::{Failure, Request},
    },
    model::{FileId, Program},
};
use nemo_physical::diagnostics::Diagnostic;

/// Parses a program, panicking on diagnostics.
pub(crate) fn parse(source: &str) -> Program {
    let (program, diagnostics) = Program::parse(source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{source}: {diagnostics:?}");
    program
}

/// Parses a program against a source registered with the engine,
/// panicking on diagnostics.
///
/// Registering is what lets the spans of results and reports resolve to
/// a file.
pub(crate) fn parse_into(engine: &mut Engine, source: &str) -> Program {
    let file = engine
        .add_source("test.rls", source)
        .expect("the text is small");
    let (program, diagnostics) = Program::parse(source, file);
    assert!(diagnostics.is_empty(), "{source}: {diagnostics:?}");
    program
}

/// Parses, loads, and reasons a program to its fixpoint.
pub(crate) fn run(source: &str) -> Engine {
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, source);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    engine
}

/// Returns the rows of a predicate as displayed values, sorted.
pub(crate) fn rows(engine: &mut Engine, predicate: &str) -> Vec<String> {
    let mut rows: Vec<String> = engine
        .rows(predicate)
        .expect("the predicate exists")
        .into_iter()
        .map(|row| {
            row.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    rows.sort();
    rows
}

/// What a responder returns for a request: the body, or why there is none.
type Response = dyn Fn(&Request) -> Result<String, Failure>;

/// A make-believe remote side: the responses to the requests a suspended engine waits for,
/// remembering every request it responded to.
pub(crate) struct Responder {
    /// The response to a request.
    respond: Box<Response>,
    /// Every request responded to, in order.
    seen: RefCell<Vec<Request>>,
}

impl Responder {
    /// Creates a responder returning the given function's result for each request.
    pub(crate) fn new(respond: impl Fn(&Request) -> Result<String, Failure> + 'static) -> Self {
        Self {
            respond: Box::new(respond),
            seen: RefCell::new(Vec::new()),
        }
    }

    /// Creates a responder returning a URL's body for that URL, and a 404 for any other URL.
    pub(crate) fn canned(bodies: &[(&str, &str)]) -> Self {
        let bodies: Vec<(String, String)> = bodies
            .iter()
            .map(|&(url, body)| (url.to_owned(), body.to_owned()))
            .collect();
        Self::new(move |request| {
            bodies
                .iter()
                .find(|(url, _)| *url == request.url)
                .map(|(_, body)| body.clone())
                .ok_or_else(|| Failure::response(404, "no canned body"))
        })
    }

    /// Creates a SPARQL endpoint responding to each query text with the given function.
    #[cfg(feature = "sparql")]
    pub(crate) fn endpoint(respond: impl Fn(&str) -> String + 'static) -> Self {
        Self::new(move |request| Ok(respond(&query_text(request))))
    }

    /// Creates a SPARQL endpoint responding to each query text with the given function,
    /// failures included.
    #[cfg(feature = "sparql")]
    pub(crate) fn failing_endpoint(
        respond: impl Fn(&str) -> Result<String, Failure> + 'static,
    ) -> Self {
        Self::new(move |request| respond(&query_text(request)))
    }

    /// Calls an engine method, responding to the requests it suspends for and resuming it,
    /// until it completes or fails.
    pub(crate) fn drive(
        &self,
        engine: &mut Engine,
        call: impl FnOnce(&mut Engine) -> Result<(), Diagnostic>,
    ) -> Result<(), Diagnostic> {
        let mut outcome = call(engine);
        loop {
            let pending = engine.pending();
            if pending.is_empty() {
                return outcome;
            }
            for request in pending {
                let response =
                    (self.respond)(&request.request).map(|body| Arc::from(body.as_bytes()));
                self.seen.borrow_mut().push(request.request);
                assert!(engine.respond(request.id, response), "the request waits");
            }
            outcome = engine.resume();
        }
    }

    /// Returns the requests responded to so far, in order.
    pub(crate) fn seen(&self) -> Vec<Request> {
        self.seen.borrow().clone()
    }

    /// Returns the query texts of the requests responded to so far, in order.
    #[cfg(feature = "sparql")]
    pub(crate) fn queries(&self) -> Vec<String> {
        self.seen.borrow().iter().map(query_text).collect()
    }
}

/// Returns the query a request carries, as a GET parameter or a form
/// field.
#[cfg(feature = "sparql")]
pub(crate) fn query_text(request: &Request) -> String {
    request
        .parameters
        .iter()
        .chain(request.form.iter().flatten())
        .find(|(key, _)| key == "query")
        .map(|(_, text)| text.clone())
        .unwrap_or_default()
}

/// Returns the engine's predicates as name and arity.
pub(crate) fn arities(engine: &Engine) -> Vec<(String, usize)> {
    engine
        .predicates()
        .into_iter()
        .map(|info| (info.name, info.arity))
        .collect()
}

/// Returns the engine's predicates as name and role.
pub(crate) fn roles(engine: &Engine) -> Vec<(String, PredicateRole)> {
    engine
        .predicates()
        .into_iter()
        .map(|info| (info.name, info.role))
        .collect()
}
