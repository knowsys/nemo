//! Remote imports without a network: a client set with `set_client`,
//! and a load that returns its requests and takes the responses.
//!
//! Usage: `cargo run -p nemo --example remote`

use std::sync::Arc;

use nemo::engine::{
    Engine,
    remote::{Client, Failure, Progress, Request, Response},
};

/// A program that imports from a URL.
const PROGRAM: &str = r#"
    @import city from csv { resource = "https://example.org/cities.csv" } .
    large(?name) :- city(?name, ?people), ?people > 1000000 .
"#;

/// What the URL serves.
const CITIES: &str = "Dresden,563000\nPrague,1357000\nVienna,1973000\n";

/// Returns the response to a request, from memory.
fn respond(request: &Request) -> Response {
    match request.url.as_str() {
        "https://example.org/cities.csv" => Ok(Arc::from(CITIES.as_bytes())),
        _ => Err(Failure::response(404, "nothing is served here")),
    }
}

/// A client that responds from memory.
///
/// Another client may respond from a cache, or send the requests through a proxy.
struct Fixtures;

impl Client for Fixtures {
    fn fetch_all(&self, requests: &[Request], _: &dyn Fn() -> bool) -> Option<Vec<Response>> {
        Some(requests.iter().map(respond).collect())
    }
}

fn main() {
    // The plain methods send their requests through the engine's client.
    // Built with the `http` feature, a new engine has an HTTP client. This one gets its own.
    let mut engine = Engine::new();
    engine.set_client(Some(Box::new(Fixtures)));
    engine
        .load_source("cities.rls", PROGRAM)
        .expect("the client serves the import");
    engine.reason().expect("reasoning reaches the fixpoint");
    println!(
        "through the client: {} large cities",
        engine.count("large").expect("the program derives `large`")
    );

    // The `start_*` methods send nothing. They return the requests, and the method is
    // suspended, having stored nothing, until `resume` has a response to each of them.
    // In between, the requests can be fetched in any way, such as from an event loop.
    let mut engine = Engine::new();
    let mut progress = Progress::Done;
    engine
        .with_source("cities.rls", PROGRAM, |engine, program| {
            progress = engine.start_load(program)?;
            Ok(())
        })
        .expect("the program parses");
    while let Progress::Suspended(pending) = progress {
        for waiting in &pending {
            let request = &waiting.request;
            println!("the load waits for {} {}", request.method(), request.url);
        }
        let responses: Vec<_> = pending
            .iter()
            .map(|waiting| (waiting.id, respond(&waiting.request)))
            .collect();
        progress = engine.resume(responses).expect("the load continues");
    }
    engine.reason().expect("reasoning reaches the fixpoint");
    println!(
        "through resume: {} large cities",
        engine.count("large").expect("the program derives `large`")
    );
}
