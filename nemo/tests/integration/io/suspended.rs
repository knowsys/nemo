//! Loads suspended for their remote data: a load, an extension, or a reload prepares its
//! translation, is suspended until its requests have responses, and the responses complete or
//! fail it. The plain methods send the requests through the engine's client, and the `start_*`
//! methods return them, for `resume` to take their responses.

use std::sync::Arc;

use nemo::engine::{
    Engine,
    remote::{PendingRequest, Progress, RequestId},
};
use nemo_physical::value::DataValue;

use crate::common::{Responder, TempDirectory, parse};

use super::load_into;

/// Returns the requests a call left its method suspended for, panicking if the method is done.
fn suspended(progress: Progress) -> Vec<PendingRequest> {
    match progress {
        Progress::Suspended(pending) => pending,
        Progress::Done => panic!("the step needs its remote data"),
    }
}

/// A load needing data from the network is suspended before anything is stored, listing the
/// requests with the format's media type and the declared headers, and the responses complete it.
#[test]
fn a_load_of_remote_imports_is_suspended_until_their_responses_come() {
    let program = parse(
        r#"@import p from csv { resource = "https://example.org/data.csv",
             http_headers = { Authorization = "Bearer token-123" } } ."#,
    );
    let mut engine = Engine::new();
    let pending = suspended(
        engine
            .start_load(program)
            .unwrap_or_else(|problem| panic!("{problem:?}")),
    );
    assert!(engine.rows("p").is_err(), "a suspended load stores nothing");
    assert_eq!(engine.pending(), pending);
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

    // Resuming without a response keeps the load suspended, for the same request.
    let again = engine
        .resume([])
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(again, Progress::Suspended(pending.clone()));

    let progress = engine
        .resume([(request.id, Ok(Arc::from(&b"1,alpha\n2,beta\n"[..])))])
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(progress, Progress::Done);
    assert!(engine.pending().is_empty());

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

/// A response under an id that `pending()` does not list is a bug of the code passing it.
#[test]
#[should_panic(expected = "which `pending()` does not list")]
fn a_response_under_an_id_pending_does_not_list_panics() {
    let program = parse(r#"@import p from csv { resource = "https://example.org/p.csv" } ."#);
    let mut engine = Engine::new();
    let pending = suspended(
        engine
            .start_load(program)
            .unwrap_or_else(|problem| panic!("{problem:?}")),
    );
    let unknown = RequestId::from(pending[0].id.value() + 100);
    let _ = engine.resume([(unknown, Ok(Arc::from(&b"1\n"[..])))]);
}

/// Two responses under one id are a bug of the code passing them.
#[test]
#[should_panic(expected = "two responses under the id")]
fn two_responses_under_one_id_panic() {
    let program = parse(r#"@import p from csv { resource = "https://example.org/p.csv" } ."#);
    let mut engine = Engine::new();
    let pending = suspended(
        engine
            .start_load(program)
            .unwrap_or_else(|problem| panic!("{problem:?}")),
    );
    let _ = engine.resume([
        (pending[0].id, Ok(Arc::from(&b"1\n"[..]))),
        (pending[0].id, Ok(Arc::from(&b"2\n"[..]))),
    ]);
}

/// Without a client, a plain load that needs data from the network fails and changes nothing.
#[test]
fn a_load_without_a_client_changes_nothing() {
    let mut engine = Engine::new();
    engine.set_client(None);
    let problem = engine
        .load(parse(
            r#"@import p from csv { resource = "https://example.org/p.csv" } ."#,
        ))
        .expect_err("nothing sends the request");
    assert_eq!(problem.code(), nemo::codes::NO_CLIENT, "{problem:?}");
    assert_eq!(problem.field("first"), Some("https://example.org/p.csv"));
    assert!(engine.pending().is_empty(), "nothing is suspended");
    assert!(!engine.has_program(), "nothing was loaded");
}

/// A failed response fails the load, naming the resource, and leaves nothing suspended.
#[test]
fn a_failed_response_fails_the_load() {
    let mut engine = Engine::new();
    let program = parse(r#"@import p from csv { resource = "https://example.org/missing.csv" } ."#);
    let problem = Responder::canned(&[])
        .serve(&mut engine, |engine| engine.load(program))
        .expect_err("no body is canned for this URL");
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
        .serve(&mut engine, |engine| engine.load(program))
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

/// A load asks for its unregistered remote resources only: registered
/// ones and local files are read without a request, and the responses
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
        .serve(&mut engine, |engine| engine.load(program))
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

/// While a load is suspended, the methods that would change the program or the data are refused
/// rather than drop it, and discarding it gives the rows added through `import_rows` back for the
/// next load.
#[test]
fn a_suspended_load_refuses_changes_until_discarded() {
    let mut engine = Engine::new();
    engine
        .import_rows("h", vec![vec![DataValue::integer(1)]])
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let remote = parse(r#"@import p from csv { resource = "https://example.org/p.csv" } ."#);
    let pending = suspended(
        engine
            .start_load(remote)
            .unwrap_or_else(|problem| panic!("{problem:?}")),
    );
    assert_eq!(pending.len(), 1);

    let refusals = [
        ("load", engine.load(parse("g(?x) :- h(?x) ."))),
        (
            "load",
            engine.start_load(parse("g(?x) :- h(?x) .")).map(drop),
        ),
        (
            "import_rows",
            engine.import_rows("h", vec![vec![DataValue::integer(2)]]),
        ),
        ("reason", engine.reason()),
        ("reason", engine.start_reason().map(drop)),
    ];
    for (method, refused) in refusals {
        let problem = refused.expect_err("a load is suspended");
        assert_eq!(problem.code(), nemo::codes::METHOD_SUSPENDED, "{problem:?}");
        assert_eq!(problem.field("refused"), Some(method), "{problem:?}");
        assert_eq!(problem.field("suspended"), Some("load"), "{problem:?}");
    }
    assert_eq!(engine.pending(), pending, "the load is still suspended");

    assert!(engine.discard_pending());
    assert!(!engine.discard_pending(), "nothing is suspended any more");
    load_into(&mut engine, "g(?x) :- h(?x) .");
    assert_eq!(
        engine.rows("g").expect("the rule derived"),
        vec![vec![DataValue::integer(1)]]
    );
}

/// An extension and a reload are suspended for their remote resources like a load,
/// the loaded program readable and untouched until they complete.
#[test]
fn extensions_and_reloads_are_suspended_like_loads() {
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
    let progress = engine
        .start_extend(extension)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert!(matches!(progress, Progress::Suspended(_)), "{progress:?}");
    assert!(
        engine.rows("b").is_err(),
        "a suspended extension stores nothing"
    );
    assert_eq!(
        engine.rows("a").expect("the loaded program reads"),
        vec![vec![DataValue::integer(1)]]
    );
    let refused = engine.reason().expect_err("an extension is suspended");
    assert_eq!(refused.code(), nemo::codes::METHOD_SUSPENDED, "{refused:?}");
    assert_eq!(refused.field("suspended"), Some("extend"), "{refused:?}");

    canned
        .complete(&mut engine, progress)
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
        .serve(&mut engine, |engine| engine.reload(reloaded))
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

/// A resource name computed with a nondeterministic function is requested once, and its
/// response read, however often the load's translation would compute the name.
#[test]
fn a_computed_resource_name_is_requested_once() {
    let canned = Responder::new(|request| {
        assert!(
            request
                .url
                .starts_with("https://example.org/data.csv?nonce=")
        );
        Ok(String::from("1\n"))
    });
    let mut engine = Engine::new();
    let program = parse(
        r#"@import p from csv { resource = f"https://example.org/data.csv?nonce={?n}" }, ?n = STRUUID() ."#,
    );
    canned
        .serve(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(canned.seen().len(), 1);
    assert_eq!(
        engine.rows("p").expect("the predicate loaded"),
        vec![vec![DataValue::integer(1)]]
    );
}

/// A SPARQL import is suspended for its query like any remote import, and the
/// solutions in the response are its rows.
#[cfg(feature = "sparql")]
#[test]
fn sparql_imports_are_suspended_for_their_query() {
    let program = parse(
        r#"@prefix ex: <http://example.org/> .
@import q from sparql { endpoint = "https://example.org/sparql",
    query = """SELECT ?a ?b WHERE { ?a ex:p ?b }""" } ."#,
    );
    let mut engine = Engine::new();
    let pending = suspended(
        engine
            .start_load(program)
            .unwrap_or_else(|problem| panic!("{problem:?}")),
    );
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

    let progress = engine
        .resume([(
            request.id,
            Ok(Arc::from(&b"?a\t?b\n<http://example.org/x>\t42\n"[..])),
        )])
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(progress, Progress::Done);
    assert_eq!(
        engine.rows("q").expect("the predicate loaded"),
        vec![vec![
            DataValue::iri("http://example.org/x"),
            DataValue::integer(42)
        ]]
    );
}

/// Queries the SPARQL parser renders with fresh, randomly named variables or blank nodes
/// (an aggregate, an anonymous blank node, a sequence path) are requested once each.
#[cfg(feature = "sparql")]
#[test]
fn queries_with_fresh_names_are_requested_once() {
    for (query, body) in [
        (
            "SELECT ?h (COUNT(?i) AS ?n) WHERE { ?h <http://e/indicator> ?i } GROUP BY ?h",
            "?h\t?n\n<http://e/a>\t1\n",
        ),
        (
            "SELECT ?h ?n WHERE { ?h <http://e/p> [ <http://e/q> ?n ] }",
            "?h\t?n\n<http://e/a>\t1\n",
        ),
        (
            "SELECT ?h ?n WHERE { ?h <http://e/p>/<http://e/q> ?n }",
            "?h\t?n\n<http://e/a>\t1\n",
        ),
    ] {
        let endpoint = Responder::endpoint(move |_| body.to_owned());
        let mut engine = Engine::new();
        let program = parse(&format!(
            r#"@import q from sparql {{ endpoint = <https://example.org/sparql>, query = "{query}" }} ."#
        ));
        endpoint
            .serve(&mut engine, |engine| engine.load(program))
            .unwrap_or_else(|problem| panic!("{query}: {problem:?}"));
        assert_eq!(endpoint.seen().len(), 1, "{query}");
        assert_eq!(
            engine.rows("q").expect("the predicate loaded"),
            vec![vec![DataValue::iri("http://e/a"), DataValue::integer(1)]],
            "{query}"
        );
    }
}

/// Two imports of the same query text under different bases are two queries,
/// each requested and responded to on its own.
#[cfg(feature = "sparql")]
#[test]
fn the_same_text_under_two_bases_is_two_queries() {
    let endpoint = Responder::endpoint(|text| {
        let iri = if text.contains("http://a.example/p") {
            "<http://a.example/x>"
        } else {
            "<http://b.example/x>"
        };
        format!("?x\n{iri}\n")
    });
    let program = parse(
        r#"@import first from sparql { endpoint = <https://example.org/sparql>,
    base = <http://a.example/>, query = "SELECT ?x WHERE { ?x <p> ?y }" } .
@import second from sparql { endpoint = <https://example.org/sparql>,
    base = <http://b.example/>, query = "SELECT ?x WHERE { ?x <p> ?y }" } ."#,
    );
    let mut engine = Engine::new();
    endpoint
        .serve(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(endpoint.seen().len(), 2);
    assert_eq!(
        engine.rows("first").expect("loaded"),
        vec![vec![DataValue::iri("http://a.example/x")]]
    );
    assert_eq!(
        engine.rows("second").expect("loaded"),
        vec![vec![DataValue::iri("http://b.example/x")]]
    );
}
