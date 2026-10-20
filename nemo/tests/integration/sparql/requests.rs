//! The requests of a run: suspending and resuming, halving requests that are too large or cut
//! off, failures under `on_error = warn`, and the same run driven over HTTP.

use nemo::{
    engine::{Engine, remote::Failure},
    model::Fact,
};
use nemo_physical::value::DataValue;

use crate::common::{Responder, parse_into, query_text};

use super::{ANCESTORS, parent_of, parents, values_blocks};

/// The ancestry endpoint behind an HTTP server: roots for a query
/// without bindings, parents for the bindings of one.
#[cfg(feature = "http")]
fn ancestry(text: &str) -> String {
    let blocks = values_blocks(text);
    if blocks.is_empty() {
        return String::from("?r\n<urn:a>\n");
    }
    let mut body = String::from("?c\t?p\n");
    for iri in blocks.into_iter().flatten() {
        if let Some(parent) = parent_of(&iri) {
            body.push_str(&format!("<{iri}>\t<{parent}>\n"));
        }
    }
    body
}

/// Decodes a form-urlencoded value.
#[cfg(feature = "http")]
fn form_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => decoded.push(b' '),
            b'%' if index + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("");
                decoded.push(u8::from_str_radix(hex, 16).unwrap_or(b'?'));
                index += 2;
            }
            byte => decoded.push(byte),
        }
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// A SPARQL endpoint on a loopback port,
/// responding to every request's query (a GET parameter or a POST form field)
/// with a function of its text.
#[cfg(feature = "http")]
fn serve_endpoint(respond: fn(&str) -> String) -> String {
    use std::io::{BufRead, BufReader, Read, Write};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let address = listener.local_addr().expect("an address");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).ok();
            let target = line.split_whitespace().nth(1).unwrap_or("").to_owned();
            let mut length = 0;
            loop {
                let mut header = String::new();
                if reader.read_line(&mut header).unwrap_or(0) == 0 || header.trim().is_empty() {
                    break;
                }
                if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap_or(0);
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).ok();
            let form = String::from_utf8_lossy(&body).into_owned();
            let parameters = target.split_once('?').map_or("", |(_, query)| query);
            let query = parameters
                .split('&')
                .chain(form.split('&'))
                .find_map(|pair| pair.strip_prefix("query="))
                .map(form_decode)
                .unwrap_or_default();
            let results = respond(&query);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/tab-separated-values\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{results}",
                results.len()
            );
            reader.get_mut().write_all(response.as_bytes()).ok();
        }
    });
    format!("http://{address}/sparql")
}

/// A reasoning run suspends in the fetch application for its responses: the
/// store reads as it is, a trace fails, `reason()` lists the same
/// requests again, and the responses continue the run where it was suspended.
#[test]
fn a_suspended_run_continues_at_its_application() {
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, ANCESTORS);
    engine
        .load(program)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let suspended = engine.reason().expect_err("the fetch suspends the run");
    assert_eq!(
        suspended.code(),
        nemo::codes::AWAITING_REMOTE,
        "{suspended:?}"
    );
    let pending = engine.pending();
    assert_eq!(pending.len(), 1, "{pending:?}");
    assert_eq!(
        values_blocks(&query_text(&pending[0].request)),
        vec![vec!["urn:a".to_owned()]]
    );

    assert_eq!(engine.count("anc").expect("readable"), 1);
    let fact = Fact::parse("anc(<urn:a>) .").expect("parses");
    let refused = engine.trace(&[fact]).expect_err("the run is unfinished");
    assert_eq!(refused.code(), nemo::codes::UNFINISHED, "{refused:?}");

    engine.reason().expect_err("still waiting");
    assert_eq!(engine.pending(), pending, "the same requests, the same ids");

    let endpoint = parents();
    endpoint
        .drive(&mut engine, Engine::resume)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(engine.count("anc").expect("derived"), 4);
    assert_eq!(
        endpoint.queries().len(),
        4,
        "the suspended request was sent once, then each new binding"
    );
}

/// A change to the store abandons a suspended run: nothing advanced, so
/// the next run requests the application's window again.
#[test]
fn a_change_abandons_a_suspended_run() {
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, ANCESTORS);
    engine
        .load(program)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    engine.reason().expect_err("the fetch suspends the run");
    engine
        .import_rows("anc", vec![vec![DataValue::iri("urn:x")]])
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert!(engine.pending().is_empty(), "the suspended run is gone");

    let endpoint = parents();
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(engine.count("anc").expect("derived"), 5);
    let first = values_blocks(&endpoint.queries()[0]);
    assert_eq!(
        first,
        vec![vec!["urn:a".to_owned(), "urn:x".to_owned()]],
        "the window was asked again, the new row with it"
    );
}

/// A request the endpoint finds too large splits its `VALUES` block in halves until each fits,
/// and a single binding that is still too large fails the run.
#[test]
fn too_large_requests_split_until_they_fit() {
    let endpoint = Responder::failing_endpoint(|text| {
        let blocks = values_blocks(text);
        if blocks[0].len() > 1 {
            return Err(Failure::response(413, "Payload Too Large"));
        }
        Ok(format!("?x\t?y\n<{}>\t<urn:seen>\n", blocks[0][0]))
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
@import seen(?x, ?y) from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?x ?y WHERE { ?x <urn:p> ?y }"
} where item(?x) .
item(<urn:1>) . item(<urn:2>) . item(<urn:3>) .
"#,
    );
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(engine.count("seen").expect("fetched"), 3);
    let sizes: Vec<usize> = endpoint
        .queries()
        .iter()
        .map(|text| values_blocks(text)[0].len())
        .collect();
    assert_eq!(sizes, vec![3, 1, 2, 1, 1], "halved, then halved again");

    let endpoint =
        Responder::failing_endpoint(|_| Err(Failure::response(413, "Payload Too Large")));
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, ANCESTORS);
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let problem = endpoint
        .drive(&mut engine, Engine::reason)
        .expect_err("one binding cannot shrink");
    assert_eq!(problem.code(), nemo::codes::UNFETCHABLE, "{problem:?}");
    assert!(
        problem
            .field("reason")
            .is_some_and(|reason| reason.contains("413")),
        "{problem:?}"
    );
}

#[test]
fn cut_off_responses_split_until_they_fit() {
    // As Wikidata's endpoint does at its time limit: a success whose rows
    // end in the exception, here for every request of more than one binding.
    let endpoint = Responder::endpoint(|text| {
        let blocks = values_blocks(text);
        if blocks[0].len() > 1 {
            return String::from("?x\t?y\njava.util.concurrent.TimeoutException\n");
        }
        format!("?x\t?y\n<{}>\t<urn:seen>\n", blocks[0][0])
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
@import seen(?x, ?y) from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?x ?y WHERE { ?x <urn:p> ?y }"
} where item(?x) .
item(<urn:1>) . item(<urn:2>) . item(<urn:3>) .
"#,
    );
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(engine.count("seen").expect("fetched"), 3);
    let sizes: Vec<usize> = endpoint
        .queries()
        .iter()
        .map(|text| values_blocks(text)[0].len())
        .collect();
    assert_eq!(sizes, vec![3, 1, 2, 1, 1], "halved, then halved again");

    // A single binding cut off cannot shrink: the cut is the error.
    let endpoint =
        Responder::endpoint(|_| String::from("?c\t?p\njava.util.concurrent.TimeoutException\n"));
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, ANCESTORS);
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let problem = endpoint
        .drive(&mut engine, Engine::reason)
        .expect_err("one binding cannot shrink");
    assert_eq!(
        problem.code(),
        nemo::codes::SPARQL_RESPONSE_CUT,
        "{problem:?}"
    );
}

/// Under `on_error = warn`, a bound import keeps the solutions of the
/// requests that succeeded, never asks a failed request's bindings
/// again, and warns once with the counts.
#[test]
fn failed_requests_under_warn_keep_what_arrived() {
    let endpoint = Responder::failing_endpoint(|text| {
        if values_blocks(text)
            .concat()
            .iter()
            .any(|iri| iri == "urn:b")
        {
            return Err(Failure::response(503, "Service Unavailable"));
        }
        let mut body = String::from("?c\t?p\n");
        for iri in values_blocks(text).into_iter().flatten() {
            if let Some(parent) = parent_of(&iri) {
                body.push_str(&format!("<{iri}>\t<{parent}>\n"));
            }
        }
        Ok(body)
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        &ANCESTORS.replace(
            "endpoint = <http://example.org/sparql>,",
            "endpoint = <http://example.org/sparql>, on_error = warn,",
        ),
    );
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let mut ancestors = engine.rows("anc").expect("derived");
    ancestors.sort();
    assert_eq!(
        ancestors,
        ["urn:a", "urn:b"]
            .map(|iri| vec![DataValue::iri(iri)])
            .to_vec()
    );
    let warnings: Vec<_> = engine
        .reports()
        .iter()
        .filter(|report| report.code() == nemo::codes::REQUESTS_FAILED)
        .collect();
    let [warning] = warnings.as_slice() else {
        panic!("one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.field("failed"), Some("1"));
    assert_eq!(warning.field("sent"), Some("2"));
    assert_eq!(warning.field("unasked"), Some("1"));

    // The failed binding is not asked again.
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(endpoint.queries().len(), 2);
}

/// Imports whose failures are handled differently are not merged into
/// one query.
#[test]
fn imports_merge_only_under_one_failure_behavior() {
    let source = |second: &str| {
        format!(
            r#"
@import t from sparql {{ endpoint = <http://example.org/sparql>,
    query = "SELECT ?s ?o WHERE {{ ?s <urn:p> ?o }}" }} .
@import u from sparql {{ endpoint = <http://example.org/sparql>, {second}
    query = "SELECT ?s ?o WHERE {{ ?s <urn:q> ?o }}" }} .
item(<urn:1>) .
r(?x, ?z) :- item(?x), t(?x, ?y), u(?y, ?z) .
@output r .
"#
        )
    };
    let queries = |second: &str| {
        let endpoint = Responder::endpoint(|_| String::from("?x\t?z\n"));
        let mut engine = Engine::new();
        let program = parse_into(&mut engine, &source(second));
        endpoint
            .drive(&mut engine, |engine| engine.load(program))
            .unwrap_or_else(|problem| panic!("{problem:?}"));
        endpoint
            .drive(&mut engine, Engine::reason)
            .unwrap_or_else(|problem| panic!("{problem:?}"));
        endpoint.queries().len()
    };
    assert_eq!(queries(""), 1, "one endpoint, one query");
    assert_eq!(queries("on_error = fail,"), 1, "fail is the default");
    assert!(queries("on_error = warn,") > 1, "the behaviors keep apart");
}

/// A run whose responses are passed to `respond` by the test ends with the same store as a run
/// the driver runs over HTTP, for load-time imports, bound imports, and the `sparql` stage's
/// rewrites alike.
#[cfg(feature = "http")]
#[test]
fn a_run_with_suspensions_ends_as_a_driven_run() {
    let endpoint_url = serve_endpoint(ancestry);
    let source = format!(
        r#"
@import root from sparql {{ endpoint = <{endpoint_url}>,
    query = "SELECT ?r WHERE {{ ?r <urn:root> ?t }}" }} .
@import parent(?c, ?p) from sparql {{ endpoint = <{endpoint_url}>,
    query = "SELECT ?c ?p WHERE {{ ?c <urn:parent> ?p }}" }} where anc(?c) .
@import triple from sparql {{ endpoint = <{endpoint_url}>,
    query = "SELECT ?s ?p ?o WHERE {{ ?s ?p ?o }}" }} .
anc(?r) :- root(?r) .
anc(?p) :- parent(?c, ?p) .
desc(?c) :- anc(?p), triple(?c, <urn:parent>, ?p) .
@output root . @output anc . @output parent . @output desc .
"#
    );
    let store = |engine: &mut Engine| {
        let mut rows = Vec::new();
        for predicate in ["root", "anc", "parent", "desc"] {
            let mut table = engine.rows(predicate).expect("stored");
            table.sort();
            rows.push((predicate, table));
        }
        rows
    };

    let mut driven = Engine::new();
    let program = parse_into(&mut driven, &source);
    let driver = nemo::driver::HttpDriver::new();
    driver
        .drive(&mut driven, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    driver
        .drive(&mut driven, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    let mut suspended = Engine::new();
    let program = parse_into(&mut suspended, &source);
    let responder = Responder::endpoint(ancestry);
    responder
        .drive(&mut suspended, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    responder
        .drive(&mut suspended, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    let expected = store(&mut suspended);
    assert_eq!(store(&mut driven), expected);
    assert_eq!(
        expected[1].1.len(),
        4,
        "the ancestry was followed: {expected:?}"
    );
    assert_eq!(
        expected[3].1.len(),
        3,
        "the rewritten rule ran: {expected:?}"
    );
}
