//! Remote imports the HTTP driver fetches from a listener on a loopback port: CSV and SPARQL
//! imports, failed fetches, registered bytes, retries, and cancellation.

use std::sync::Arc;

use nemo::{
    engine::Engine,
    model::{FileId, Program},
};
use nemo_physical::value::DataValue;

use crate::common::parse;

use super::{load_into, run};

/// A one-shot HTTP server on a loopback port: answers `responses` many
/// requests with the given body, sending each request's full text back
/// through the channel.
fn serve(
    body: &str,
    status: &str,
    responses: usize,
) -> (String, std::sync::mpsc::Receiver<String>) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    serve_responses(vec![response; responses])
}

/// An HTTP server on a loopback port answering one request after the
/// other with the given raw responses, sending each request's full
/// text back through the channel.
fn serve_responses(responses: Vec<String>) -> (String, std::sync::mpsc::Receiver<String>) {
    use std::io::{Read, Write};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port is free");
    let address = listener.local_addr().expect("the port is known");
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for response in responses {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            // Read to the end of the headers, then any body the
            // headers announce.
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                match stream.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(count) => request.extend_from_slice(&buffer[..count]),
                }
            }
            let text = String::from_utf8_lossy(&request).to_string();
            if let Some(length) = text.lines().find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            }) {
                let header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4)
                    .unwrap_or(request.len());
                while request.len() < header_end + length {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(count) => request.extend_from_slice(&buffer[..count]),
                    }
                }
            }
            sender
                .send(String::from_utf8_lossy(&request).to_string())
                .ok();
            stream.write_all(response.as_bytes()).ok();
        }
    });
    (format!("http://{address}"), receiver)
}

/// A remote CSV import fetches over HTTP, with the declared headers on
/// the request.
#[test]
fn csv_imports_fetch_over_http() {
    let (base, requests) = serve("1,alpha\n2,beta\n", "200 OK", 1);
    let engine = run(&format!(
        r#"@import p from csv {{ resource = "{base}/data.csv",
             http_headers = {{ Authorization = "Bearer token-123" }} }} .
"#
    ));
    let mut engine = engine;
    let mut rows = engine.rows("p").expect("the predicate loaded");
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![DataValue::integer(1), DataValue::iri("alpha")],
            vec![DataValue::integer(2), DataValue::iri("beta")],
        ]
    );
    let request = requests.recv().expect("one request was made");
    let lowered = request.to_ascii_lowercase();
    assert!(
        lowered.contains("authorization: bearer token-123"),
        "the declared header travels: {request}"
    );
    assert!(
        lowered.contains("accept: text/csv"),
        "the format asks for its media type: {request}"
    );
    assert!(
        lowered.contains("user-agent: nemo/"),
        "the client says who it is: {request}"
    );
}

/// A failed fetch is a load error naming the URL, never quietly empty.
#[test]
fn a_failed_fetch_is_an_error() {
    let (base, _requests) = serve("gone", "404 Not Found", 1);
    let source = format!(r#"@import p from csv {{ resource = "{base}/missing.csv" }} ."#);
    let (program, diagnostics) = Program::parse(&source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let problem = nemo::driver::HttpDriver::new()
        .drive(&mut Engine::new(), |engine| engine.load(program))
        .expect_err("the fetch failed");
    assert_eq!(problem.code(), nemo::codes::UNFETCHABLE, "{problem:?}");
    assert!(
        problem
            .field("url")
            .is_some_and(|url| url.ends_with("/missing.csv")),
        "{problem:?}"
    );
    assert!(
        problem
            .field("reason")
            .is_some_and(|reason| reason.contains("404")),
        "{problem:?}"
    );
}

/// Registered bytes win over the network: an embedder can hand in what
/// a build should not fetch.
#[test]
fn registered_bytes_beat_the_network() {
    let mut engine = Engine::new();
    // No server exists at this address; the registered bytes are used.
    engine.add_resource("http://127.0.0.1:1/never.csv", "42\n".as_bytes().to_vec());
    load_into(
        &mut engine,
        r#"@import p from csv { resource = "http://127.0.0.1:1/never.csv" } ."#,
    );
    assert_eq!(
        engine.rows("p").expect("the predicate loaded"),
        vec![vec![DataValue::integer(42)]]
    );
}

/// A SPARQL import sends the validated query, file-level prefixes resolved,
/// and reads the TSV solutions as the predicate's rows.
#[cfg(all(feature = "sparql", feature = "http"))]
#[test]
fn sparql_imports_query_their_endpoint() {
    let (base, requests) = serve(
        "?a\t?b\n<http://example.org/x>\t42\n<http://example.org/y>\t\"text\"@en\n",
        "200 OK",
        1,
    );
    let mut engine = run(&format!(
        r#"@prefix ex: <http://example.org/> .
@import q from sparql {{ endpoint = "{base}/sparql",
    query = """SELECT ?a ?b WHERE {{ ?a ex:p ?b }}""" }} .
"#
    ));
    let rows = engine.rows("q").expect("the predicate loaded");
    assert_eq!(
        rows,
        vec![
            vec![
                DataValue::iri("http://example.org/x"),
                DataValue::integer(42)
            ],
            vec![
                DataValue::iri("http://example.org/y"),
                DataValue::language_string("text", "en").expect("valid tag"),
            ],
        ]
    );
    let request = requests.recv().expect("one request was made");
    assert!(
        request.starts_with("GET /sparql?query="),
        "a short query travels as a GET parameter: {request}"
    );
    assert!(
        request.contains("example.org"),
        "the file's prefix resolved into the query: {request}"
    );
    let lowered = request.to_ascii_lowercase();
    assert!(
        lowered.contains("accept: text/tab-separated-values"),
        "solutions are requested as TSV: {request}"
    );
}

/// A SPARQL import resolves its query against a directive `base`, and
/// `format` keeps or skips solution columns.
#[cfg(all(feature = "sparql", feature = "http"))]
#[test]
fn sparql_imports_take_a_base_and_column_formats() {
    let (base, requests) = serve("?a\t?b\n<http://example.org/x>\t42\n", "200 OK", 1);
    let mut engine = run(&format!(
        r#"@import q from sparql {{ endpoint = "{base}/sparql", base = <http://example.org/>,
    format = (skip, term),
    query = """SELECT ?a ?b WHERE {{ ?a <p> ?b }}""" }} .
"#
    ));
    assert_eq!(
        engine.rows("q").expect("the predicate loaded"),
        vec![vec![DataValue::integer(42)]],
        "the skipped column is gone"
    );
    let request = requests.recv().expect("one request was made");
    assert!(
        request.contains("example.org%2Fp") || request.contains("example.org/p"),
        "the relative IRI resolved against the base: {request}"
    );
}

/// A SPARQL import's columns hold any term an endpoint returns,
/// so a narrower declaration on it is refused before any request is sent,
/// and the solutions load as they come, the malformed ones counted.
#[cfg(all(feature = "sparql", feature = "http"))]
#[test]
fn sparql_imports_hold_any_term() {
    let mut checker = Engine::new();
    let problems = checker
        .check_source(
            "declared.rls",
            r#"@declare q(a: iri, b: int) .
@import q from sparql { endpoint = <http://example.org/sparql>,
    query = """SELECT ?a ?b WHERE { ?a <http://example.org/p> ?b }""" } .
"#,
        )
        .into_diagnostics();
    assert!(
        problems
            .iter()
            .any(|problem| problem.code() == nemo::codes::IMPORT_OUTSIDE_DECLARATION),
        "{problems:?}"
    );

    let (base, _requests) = serve(
        "?a\t?b\n<http://example.org/x>\t42\n<http://example.org/y>\t\"text\"\nbroken\n",
        "200 OK",
        1,
    );
    let mut engine = run(&format!(
        r#"@import q from sparql {{ endpoint = "{base}/sparql",
    query = """SELECT ?a ?b WHERE {{ ?a <http://example.org/p> ?b }}""" }} .
"#
    ));
    assert_eq!(engine.rows("q").expect("the predicate loaded").len(), 2);
    let codes: Vec<_> = engine
        .reports()
        .iter()
        .map(|report| report.code())
        .collect();
    assert_eq!(codes, [nemo::codes::MALFORMED_ROWS]);
}

/// The conditions of a SPARQL import's `where` clause go to the endpoint as a `FILTER`,
/// each as far as it has a SPARQL form, and apply to the solutions regardless,
/// so the import's columns can be declared.
#[cfg(all(feature = "sparql", feature = "http"))]
#[test]
fn sparql_import_conditions_reach_the_endpoint() {
    let (base, requests) = serve(
        "?a\t?b\n<http://example.org/x>\t42\n<http://example.org/y>\t\"text\"\n",
        "200 OK",
        1,
    );
    let mut engine = run(&format!(
        r#"@declare q(a: iri, b: int) .
@import q(?a, ?b) from sparql {{ endpoint = "{base}/sparql",
    query = """SELECT ?a ?b WHERE {{ ?a <http://example.org/p> ?b }}""" }}
    where ISIRI(?a), ISINTEGER(?b) .
"#
    ));
    assert_eq!(
        engine.rows("q").expect("the predicate loaded"),
        vec![vec![
            DataValue::iri("http://example.org/x"),
            DataValue::integer(42)
        ]],
    );
    let request = requests.recv().expect("one request was made");
    assert!(
        request.contains("isIRI"),
        "the condition reached the query: {request}"
    );
}

/// The driver retries an overloaded server and a rate limit before it
/// answers, and a request that still fails is the load's error.
#[test]
fn the_driver_retries_overload_and_rate_limits() {
    use nemo::{driver::HttpDriver, engine::remote::Retry};

    let fast = HttpDriver::with_retry(Retry {
        retries: 3,
        backoff: std::time::Duration::from_millis(10),
        longest_wait: std::time::Duration::from_secs(1),
    });
    let response = |status: &str, headers: &str, body: &str| {
        format!(
            "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    };
    let (base, requests) = serve_responses(vec![
        response("503 Service Unavailable", "", "busy"),
        response("429 Too Many Requests", "Retry-After: 0\r\n", "slow down"),
        response("200 OK", "", "1\n2\n"),
    ]);
    let source = format!(r#"@import p from csv {{ resource = "{base}/data.csv" }} ."#);
    let mut engine = Engine::new();
    fast.drive(&mut engine, |engine| engine.load(parse(&source)))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(requests.try_iter().count(), 3, "two retries, then the body");
    let mut rows = engine.rows("p").expect("the predicate loaded");
    rows.sort();
    assert_eq!(
        rows,
        vec![vec![DataValue::integer(1)], vec![DataValue::integer(2)]]
    );

    let (base, requests) = serve_responses(vec![response("502 Bad Gateway", "", "down"); 4]);
    let source = format!(r#"@import p from csv {{ resource = "{base}/data.csv" }} ."#);
    let problem = fast
        .drive(&mut Engine::new(), |engine| engine.load(parse(&source)))
        .expect_err("the server stays down");
    assert_eq!(problem.code(), nemo::codes::UNFETCHABLE, "{problem:?}");
    assert!(
        problem
            .field("reason")
            .is_some_and(|reason| reason.contains("502")),
        "{problem:?}"
    );
    assert_eq!(
        requests.try_iter().count(),
        4,
        "three retries, then the failure"
    );
}

/// A cancelled drive stops waiting and leaves the method suspended, and a
/// later drive sends the requests again.
#[test]
fn a_cancelled_drive_leaves_the_method_suspended() {
    // A server that accepts and never answers, then one that does.
    let silent = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let url = format!(
        "http://{}/data.csv",
        silent.local_addr().expect("an address")
    );
    let source = format!(r#"@import p from csv {{ resource = "{url}" }} ."#);
    let mut engine = Engine::new();
    let token = engine.cancel_token();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(300));
        token.cancel();
    });
    let started = std::time::Instant::now();
    let problem = nemo::driver::HttpDriver::new()
        .drive(&mut engine, |engine| engine.load(parse(&source)))
        .expect_err("the drive is cancelled");
    canceller.join().unwrap();
    assert_eq!(problem.code(), nemo::codes::WAIT_CANCELLED, "{problem:?}");
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    assert_eq!(engine.pending().len(), 1, "the load stays suspended");

    let request = engine.pending().remove(0);
    engine.respond(request.id, Ok(Arc::from(&b"5\n"[..])));
    engine
        .resume()
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(
        engine.rows("p").expect("the predicate loaded"),
        vec![vec![DataValue::integer(5)]]
    );
    drop(silent);
}
