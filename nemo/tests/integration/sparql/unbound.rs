//! Unbound values: a query that may leave a variable unbound is fetched once, so every rule
//! reading it sees the same null for an unbound value, and no solution is lost to a binding.

use nemo::engine::Engine;

use crate::common::{Responder, parse_into, rows};

/// Loads and reasons a program, responding to its requests with a fixed result.
fn run_against(source: &str, result: &'static str) -> (Engine, Responder) {
    let fetcher = Responder::endpoint(move |_| result.to_owned());
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, source);
    fetcher
        .serve(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .serve(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    (engine, fetcher)
}

/// Three solutions, the second leaving `?c` unbound.
const SOLUTIONS: &str = "?h\t?c\t?x\n<urn:h1>\t<urn:c1>\t<urn:x1>\n<urn:h2>\t\t<urn:x2>\n<urn:h3>\t<urn:c1>\t<urn:x3>\n";

#[test]
fn readers_of_one_import_share_its_unbound_values() {
    let (mut engine, fetcher) = run_against(
        r#"
@import solutions from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?h ?c ?x WHERE { ?h <urn:p> ?x OPTIONAL { ?h <urn:q> ?c } }"
} .
withoutX(?h, ?c) :- solutions(?h, ?c, _) .
whole(?h, ?c, ?x) :- solutions(?h, ?c, ?x) .
meets(?h) :- withoutX(?h, ?c), whole(?h, ?c, _) .
@output meets .
"#,
        SOLUTIONS,
    );
    assert_eq!(fetcher.queries().len(), 1, "{:?}", fetcher.queries());
    assert_eq!(
        rows(&mut engine, "meets"),
        ["<urn:h1>", "<urn:h2>", "<urn:h3>"]
    );
}

#[test]
fn a_join_with_a_predicate_derived_from_the_import_keeps_unbound_values() {
    let (mut engine, fetcher) = run_against(
        r#"
@import solutions from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?h ?c ?x WHERE { ?h <urn:p> ?x OPTIONAL { ?h <urn:q> ?c } }"
} .
seen(?c) :- solutions(_, ?c, _) .
shown(?c, ?c) :- seen(?c), NOT(ISNULL(?c)) .
shown(?c, "none") :- seen(?c), ISNULL(?c) .
joined(?h, ?d) :- solutions(?h, ?c, _), shown(?c, ?d) .
@output joined .
"#,
        SOLUTIONS,
    );
    assert_eq!(fetcher.queries().len(), 1, "{:?}", fetcher.queries());
    assert_eq!(
        rows(&mut engine, "joined"),
        [
            "<urn:h1>,<urn:c1>",
            "<urn:h2>,\"none\"",
            "<urn:h3>,<urn:c1>"
        ]
    );
}

#[test]
fn one_column_results_keep_their_unbound_solutions() {
    // The unbound solution is an empty line.
    let (mut engine, _) = run_against(
        r#"
@import comments from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?c WHERE { ?h <urn:p> ?x OPTIONAL { ?h <urn:q> ?c } }"
} .
kind("comment", ?c) :- comments(?c), NOT(ISNULL(?c)) .
kind("none", "none") :- comments(?c), ISNULL(?c) .
@output kind .
"#,
        "?c\n\"first\"\n\n",
    );
    assert_eq!(
        rows(&mut engine, "kind"),
        ["\"comment\",\"first\"", "\"none\",\"none\""]
    );
}

#[test]
fn solutions_an_endpoint_folds_into_a_binding_are_left_out() {
    // An endpoint folding numbers of every type together, as QLever does with the literals of a
    // query, responds to the binding 3 with a double too, which Nemo tells apart from the integer.
    let (mut engine, fetcher) = run_against(
        r#"
@import q(?x, ?v) from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?x ?v WHERE { ?x <urn:value> ?v }"
} where wanted(?v) .
wanted(3) .
@output q .
"#,
        "?x\t?v\n<urn:a>\t3\n<urn:b>\t\"3.0E0\"^^<http://www.w3.org/2001/XMLSchema#double>\n<urn:c>\t\"3\"^^<http://www.w3.org/2001/XMLSchema#int>\n",
    );
    assert_eq!(fetcher.queries().len(), 1);
    assert_eq!(rows(&mut engine, "q"), ["<urn:a>,3", "<urn:c>,3"]);
}

#[test]
fn a_variable_no_pattern_binds_keeps_the_query_as_written_and_warns() {
    // As written, `t` reads a null for `?x`, which `c` does not hold, so `b` stays empty.
    // Bound by `c`, the endpoint would bind `?x` to `<urn:v>`, so the query is not rewritten.
    let (mut engine, fetcher) = run_against(
        r#"
@import t from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?s ?x WHERE { ?s <urn:p> <urn:o> }"
} .
c(<urn:v>) .
b(?s, ?x) :- c(?x), t(?s, ?x) .
@output b .
"#,
        "?s\t?x\n<urn:s>\t\n",
    );
    let queries = fetcher.queries();
    assert_eq!(queries.len(), 1, "{queries:?}");
    assert!(!queries[0].contains("VALUES"), "{queries:?}");
    assert!(rows(&mut engine, "b").is_empty());
    let warned: Vec<_> = engine
        .reports()
        .iter()
        .filter(|report| report.code() == nemo::codes::NEVER_BOUND_VARIABLE)
        .collect();
    assert_eq!(warned.len(), 1, "{warned:?}");
    assert_eq!(warned[0].field("variable"), Some("x"), "{warned:?}");
}
