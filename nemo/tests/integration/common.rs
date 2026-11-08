//! What the tests share: programs parsed from source, engines reasoned
//! to their fixpoint, a predicate's rows as text, a client responding to
//! an engine's requests, and temporary directories.

mod temp_directory;

pub(crate) use temp_directory::TempDirectory;

use std::sync::{Arc, Mutex};

use nemo::{
    engine::{
        Engine, PredicateRole,
        remote::{Client, Failure, Progress, Request, Response},
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
type RespondFn = dyn Fn(&Request) -> Result<String, Failure> + Send + Sync;

/// A make-believe remote side: a client responding to an engine's requests,
/// remembering every request it responded to.
///
/// Clones share what they remember.
#[derive(Clone)]
pub(crate) struct Responder(Arc<ResponderState>);

/// What the clones of a responder share.
struct ResponderState {
    /// The response to a request.
    respond: Box<RespondFn>,
    /// Every request responded to, in order.
    seen: Mutex<Vec<Request>>,
}

impl Client for Responder {
    fn fetch_all(&self, requests: &[Request], _: &dyn Fn() -> bool) -> Option<Vec<Response>> {
        Some(
            requests
                .iter()
                .map(|request| self.respond_to(request))
                .collect(),
        )
    }
}

impl Responder {
    /// Creates a responder returning the given function's result for each request.
    pub(crate) fn new(
        respond: impl Fn(&Request) -> Result<String, Failure> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(ResponderState {
            respond: Box::new(respond),
            seen: Mutex::new(Vec::new()),
        }))
    }

    /// Responds to one request, remembering it.
    pub(crate) fn respond_to(&self, request: &Request) -> Response {
        self.0
            .seen
            .lock()
            .expect("no test panics holding the lock")
            .push(request.clone());
        (self.0.respond)(request).map(|body| Arc::from(body.as_bytes()))
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
    pub(crate) fn endpoint(respond: impl Fn(&str) -> String + Send + Sync + 'static) -> Self {
        Self::new(move |request| Ok(respond(&query_text(request))))
    }

    /// Creates a SPARQL endpoint responding to each query text with the given function,
    /// failures included.
    #[cfg(feature = "sparql")]
    pub(crate) fn failing_endpoint(
        respond: impl Fn(&str) -> Result<String, Failure> + Send + Sync + 'static,
    ) -> Self {
        Self::new(move |request| respond(&query_text(request)))
    }

    /// Responds to the requests of a suspended engine method, and to those it is suspended
    /// for next, through `resume`, until the method completes or fails.
    pub(crate) fn complete(
        &self,
        engine: &mut Engine,
        mut progress: Progress,
    ) -> Result<(), Diagnostic> {
        while let Progress::Suspended(pending) = progress {
            let responses: Vec<_> = pending
                .iter()
                .map(|pending| (pending.id, self.respond_to(&pending.request)))
                .collect();
            progress = engine.resume(responses)?;
        }
        Ok(())
    }

    /// Calls an engine method with this responder as the engine's client,
    /// and gives the engine its own client back afterwards.
    pub(crate) fn serve(
        &self,
        engine: &mut Engine,
        call: impl FnOnce(&mut Engine) -> Result<(), Diagnostic>,
    ) -> Result<(), Diagnostic> {
        let own = engine.set_client(Some(Box::new(self.clone())));
        let outcome = call(engine);
        engine.set_client(own);
        outcome
    }

    /// Returns the requests responded to so far, in order.
    pub(crate) fn seen(&self) -> Vec<Request> {
        self.0
            .seen
            .lock()
            .expect("no test panics holding the lock")
            .clone()
    }

    /// Returns the query texts of the requests responded to so far, in order.
    #[cfg(feature = "sparql")]
    pub(crate) fn queries(&self) -> Vec<String> {
        self.seen().iter().map(query_text).collect()
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
