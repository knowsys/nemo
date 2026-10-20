//! The engine never fetches: a load, an extension, or a reload suspends for its remote
//! resources, and the responses given complete or fail it.

use std::sync::Arc;

use nemo::engine::Engine;
use nemo_physical::value::DataValue;

use crate::common::{Responder, TempDirectory, parse};

use super::load_into;

/// The engine never fetches: a load needing data from the network suspends before
/// anything is stored, listing the requests with the format's media
/// type and the declared headers, and the responses complete it.
#[test]
fn remote_imports_suspend_the_load_until_the_responses_are_given() {
    let program = parse(
        r#"@import p from csv { resource = "https://example.org/data.csv",
             http_headers = { Authorization = "Bearer token-123" } } ."#,
    );
    let mut engine = Engine::new();
    let suspended = engine.load(program).expect_err("the load suspends");
    assert_eq!(
        suspended.code(),
        nemo::codes::AWAITING_REMOTE,
        "{suspended:?}"
    );
    assert_eq!(
        suspended.field("first"),
        Some("https://example.org/data.csv")
    );
    assert!(engine.rows("p").is_err(), "a suspended load stores nothing");
    let pending = engine.pending();
    let [request] = pending.as_slice() else {
        panic!("one request per remote import: {pending:?}");
    };
    assert_eq!(request.request.url, "https://example.org/data.csv");
    assert_eq!(request.request.method(), "GET");
    assert_eq!(request.request.accept, Some("text/csv"));
    assert_eq!(
        request.request.headers,
        vec![("Authorization".to_owned(), "Bearer token-123".to_owned())]
    );

    // Resuming without a response suspends again, for the same request.
    let again = engine.resume().expect_err("still without a response");
    assert_eq!(again.code(), nemo::codes::AWAITING_REMOTE, "{again:?}");
    assert_eq!(engine.pending(), pending);

    assert!(engine.respond(request.id, Ok(Arc::from(&b"1,alpha\n2,beta\n"[..]))));
    assert!(
        !engine.respond(request.id, Ok(Arc::from(&b""[..]))),
        "a request with a response takes no second response"
    );
    assert!(engine.pending().is_empty());
    engine
        .resume()
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    engine
        .reason()
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let mut rows = engine.rows("p").expect("the predicate loaded");
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![DataValue::integer(1), DataValue::iri("alpha")],
            vec![DataValue::integer(2), DataValue::iri("beta")],
        ]
    );
}

/// A failed response fails the load, naming the resource, and leaves no
/// suspended call behind.
#[test]
fn a_failed_response_fails_the_load() {
    let mut engine = Engine::new();
    let program = parse(r#"@import p from csv { resource = "https://example.org/missing.csv" } ."#);
    let problem = Responder::canned(&[])
        .drive(&mut engine, |engine| engine.load(program))
        .expect_err("nothing answers this URL");
    assert_eq!(problem.code(), nemo::codes::UNFETCHABLE, "{problem:?}");
    assert_eq!(
        problem.field("url"),
        Some("https://example.org/missing.csv")
    );
    assert!(
        problem
            .field("reason")
            .is_some_and(|reason| reason.contains("404")),
        "{problem:?}"
    );
    assert!(engine.pending().is_empty());
}

/// Under `on_error = warn`, a failed import reads nothing and warns;
/// the rest of the program runs.
#[test]
fn a_failed_import_that_warns_reads_nothing() {
    let mut engine = Engine::new();
    let program = parse(
        r#"@import p from csv { resource = "https://example.org/missing.csv", on_error = warn } .
@import q from csv { resource = "https://example.org/data.csv", on_error = warn } .
r(?x) :- p(?x) .
s(?x) :- q(?x, ?y) ."#,
    );
    let canned = Responder::canned(&[("https://example.org/data.csv", "1,one\n")]);
    canned
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    engine
        .reason()
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(
        engine.rows("r").expect("the rule compiled"),
        Vec::<Vec<DataValue>>::new()
    );
    assert_eq!(
        engine.rows("s").expect("the rule derived"),
        vec![vec![DataValue::integer(1)]]
    );
    let warnings: Vec<_> = engine
        .reports()
        .iter()
        .filter(|report| report.code() == nemo::codes::IMPORT_FAILED)
        .collect();
    let [warning] = warnings.as_slice() else {
        panic!("one warning, for the failed import: {:?}", engine.reports());
    };
    assert_eq!(
        warning.field("resource"),
        Some("https://example.org/missing.csv")
    );
}

/// A load stops for its unregistered remote resources only: registered
/// ones and local files are read without a stop, and the answers
/// complete the load.
#[test]
fn a_load_asks_for_its_unregistered_remote_resources() {
    let mut engine = Engine::new();
    engine.add_resource("https://example.org/known.csv", "7\n".as_bytes().to_vec());
    let program = parse(
        r#"@prefix ex: <https://example.org/> .
@import p from csv { resource = "https://example.org/data.csv" } .
@import q from ntriples { resource = "https://example.org/graph.nt" } .
@import known from csv { resource = "https://example.org/known.csv" } .
@import local from csv { resource = "local.csv" } .
r(?x) :- p(?x, ?y), known(?x), local(?x) .
s(?o) :- q(?s, ?p, ?o) ."#,
    );
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    std::fs::write(directory.join("local.csv"), "7\n").unwrap();

    let canned = Responder::canned(&[
        ("https://example.org/data.csv", "7,seven\n"),
        (
            "https://example.org/graph.nt",
            "<https://example.org/7> <https://example.org/p> \"seven\" .\n",
        ),
    ]);
    engine.set_import_directory(directory);
    canned
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let seen = canned.seen();
    let asked: Vec<(&str, Option<&str>)> = seen
        .iter()
        .map(|request| (request.url.as_str(), request.accept))
        .collect();
    assert_eq!(
        asked,
        vec![
            ("https://example.org/data.csv", Some("text/csv")),
            (
                "https://example.org/graph.nt",
                Some("application/n-triples")
            ),
        ]
    );
    engine
        .reason()
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(
        engine.rows("r").expect("the rule derived"),
        vec![vec![DataValue::integer(7)]]
    );
    assert_eq!(
        engine.rows("s").expect("the rule derived"),
        vec![vec![DataValue::string("seven")]]
    );
}

/// A method called while a load is suspended abandons the load: the rows
/// added through `import_rows` before it come back for the next one.
#[test]
fn a_new_load_abandons_a_suspended_one() {
    let mut engine = Engine::new();
    engine
        .import_rows("h", vec![vec![DataValue::integer(1)]])
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let remote = parse(r#"@import p from csv { resource = "https://example.org/p.csv" } ."#);
    engine.load(remote).expect_err("the load suspends");
    assert_eq!(engine.pending().len(), 1);

    load_into(&mut engine, "g(?x) :- h(?x) .");
    assert!(engine.pending().is_empty(), "the suspended load is gone");
    assert_eq!(
        engine.rows("g").expect("the rule derived"),
        vec![vec![DataValue::integer(1)]]
    );
}

/// An extension and a reload suspend for their remote resources like a
/// load, the loaded program untouched until they complete.
#[test]
fn extensions_and_reloads_suspend_like_loads() {
    let mut engine = Engine::new();
    load_into(&mut engine, "a(1) .");
    let canned = Responder::canned(&[
        ("https://example.org/b.csv", "2\n"),
        ("https://example.org/b2.csv", "1\n3\n"),
    ]);
    let extension = parse(
        r#"@import b from csv { resource = "https://example.org/b.csv" } .
c(?x) :- b(?x) ."#,
    );
    engine
        .extend(extension.clone())
        .expect_err("the extension suspends");
    assert!(
        engine.rows("b").is_err(),
        "a suspended extension stores nothing"
    );
    engine
        .reason()
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert!(
        engine.pending().is_empty(),
        "reasoning abandons the suspended extension"
    );

    canned
        .drive(&mut engine, |engine| engine.extend(extension))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    engine
        .reason()
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(
        engine.rows("c").expect("the rule derived"),
        vec![vec![DataValue::integer(2)]]
    );

    let reloaded = parse(
        r#"a(1) .
@import b from csv { resource = "https://example.org/b2.csv" } .
c(?x) :- b(?x), a(?x) ."#,
    );
    canned
        .drive(&mut engine, |engine| engine.reload(reloaded))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    engine
        .reason()
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(
        engine.rows("c").expect("the rule derived"),
        vec![vec![DataValue::integer(1)]]
    );
    assert_eq!(
        canned.seen().len(),
        2,
        "the extension and the reload asked once each"
    );
}

/// A SPARQL import suspends for its query like any remote import, and the
/// solutions in the response are its rows.
#[cfg(feature = "sparql")]
#[test]
fn sparql_imports_suspend_for_their_query() {
    let program = parse(
        r#"@prefix ex: <http://example.org/> .
@import q from sparql { endpoint = "https://example.org/sparql",
    query = """SELECT ?a ?b WHERE { ?a ex:p ?b }""" } ."#,
    );
    let mut engine = Engine::new();
    engine.load(program).expect_err("the load suspends");
    let pending = engine.pending();
    let [request] = pending.as_slice() else {
        panic!("one request: {pending:?}");
    };
    assert_eq!(request.request.url, "https://example.org/sparql");
    assert_eq!(request.request.method(), "GET");
    assert_eq!(request.request.accept, Some("text/tab-separated-values"));
    let (_, query) = &request.request.parameters[0];
    assert!(
        query.contains("http://example.org/p"),
        "the prefix resolved into the query text: {query}"
    );

    engine.respond(
        request.id,
        Ok(Arc::from(&b"?a\t?b\n<http://example.org/x>\t42\n"[..])),
    );
    engine
        .resume()
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(
        engine.rows("q").expect("the predicate loaded"),
        vec![vec![
            DataValue::iri("http://example.org/x"),
            DataValue::integer(42)
        ]]
    );
}
