//! Bound imports follow the program's bindings to the fixpoint, one `VALUES` block per binding
//! atom, are checked without fetching, and bind generic imports through the rewrite stage.

use nemo::{
    engine::Engine,
    model::{FileId, Program},
};
use nemo_physical::value::DataValue;

use crate::common::{Responder, parse_into};

use super::{ANCESTORS, ANCESTORS_GENERIC, parent_of, parents, values_blocks};

#[test]
fn bound_imports_follow_their_bindings_to_the_fixpoint() {
    let fetcher = parents();
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, ANCESTORS);
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    // Nothing was asked at load: a bound import is a rule.
    assert!(fetcher.queries().is_empty());
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    let mut ancestors = engine.rows("anc").expect("derived");
    ancestors.sort();
    assert_eq!(
        ancestors,
        ["urn:a", "urn:b", "urn:c", "urn:d"]
            .map(|iri| vec![DataValue::iri(iri)])
            .to_vec()
    );
    // Each binding was asked for exactly once, as it appeared, so the
    // recursion reached the endpoint one iteration at a time.
    let asked: Vec<Vec<Vec<String>>> = fetcher
        .queries()
        .iter()
        .map(|text| values_blocks(text))
        .collect();
    assert_eq!(
        asked,
        vec![
            vec![vec!["urn:a".to_owned()]],
            vec![vec!["urn:b".to_owned()]],
            vec![vec!["urn:c".to_owned()]],
            vec![vec!["urn:d".to_owned()]],
        ]
    );
    // The fetched rows are the import's, derived rows to the engine.
    let mut fetched = engine.rows("parent").expect("fetched");
    fetched.sort();
    assert_eq!(fetched.len(), 3);
    assert_eq!(engine.derived_facts(), 3 + 3);
}

/// A bound import's columns hold any term an endpoint returns,
/// so a narrower declaration of its head predicate is refused before any request is sent,
/// and a rule narrows the solutions instead.
#[test]
fn bound_imports_hold_any_term() {
    let mut checker = Engine::new();
    let problems = checker
        .check_source(
            "declared.rls",
            format!("@declare parent(c: iri, p: iri) .\n{ANCESTORS}"),
        )
        .into_diagnostics();
    assert!(
        problems
            .iter()
            .any(|problem| problem.code() == nemo::codes::IMPORT_OUTSIDE_DECLARATION),
        "{problems:?}"
    );

    let fetcher = Responder::endpoint(|text| {
        let mut body = String::from("?c\t?p\n");
        for iri in values_blocks(text).into_iter().flatten() {
            if let Some(parent) = parent_of(&iri) {
                body.push_str(&format!("<{iri}>\t<{parent}>\n"));
                // A stray literal where a node is expected.
                body.push_str(&format!("<{iri}>\t\"nobody\"\n"));
            }
        }
        body
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        &ANCESTORS.replace(
            "anc(?p) :- parent(?c, ?p) .",
            "anc(?p) :- parent(?c, ?p), ISIRI(?p) .",
        ),
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(engine.rows("parent").expect("fetched").len(), 6);
    assert_eq!(engine.rows("anc").expect("derived").len(), 4);
}

/// The malformed solutions of a bound import are reported once per run,
/// however many requests returned them.
#[test]
fn malformed_solutions_of_a_bound_import_are_reported_once() {
    let fetcher = Responder::endpoint(|text| {
        let mut body = String::from("?c\t?p\n");
        for iri in values_blocks(text).into_iter().flatten() {
            if let Some(parent) = parent_of(&iri) {
                body.push_str(&format!("<{iri}>\t<{parent}>\n"));
            }
        }
        // A line that is not a solution, in every response.
        body.push_str("broken\n");
        body
    });
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, ANCESTORS);
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(engine.rows("anc").expect("derived").len(), 4);
    let malformed: Vec<_> = engine
        .reports()
        .iter()
        .filter(|report| report.code() == nemo::codes::MALFORMED_ROWS)
        .collect();
    assert_eq!(malformed.len(), 1, "{malformed:?}");
    assert_eq!(malformed[0].field("count"), Some("4"), "{malformed:?}");
    assert!(!malformed[0].labels().is_empty(), "{malformed:?}");
}

#[test]
fn two_binding_atoms_send_one_values_block_each_and_page() {
    let fetcher = Responder::endpoint(|text| {
        let blocks = values_blocks(text);
        assert_eq!(blocks.len(), 2, "one block per binding atom: {text}");
        let mut body = String::from("?x\t?y\n");
        for x in &blocks[0] {
            for y in &blocks[1] {
                body.push_str(&format!("<{x}>\t<{y}>\n"));
            }
        }
        body
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
@import pair(?x, ?y) from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?x ?y WHERE { ?x <urn:linked> ?y }",
    page = 1
} where left(?x), right(?y, _) .
left(<urn:1>) . left(<urn:2>) .
right(<urn:3>, 0) . right(<urn:4>, 0) . right(<urn:4>, 1) .
"#,
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(engine.rows("pair").expect("fetched").len(), 4);
    // A page of one binding row per atom: one request per combination
    // of the two left and the two distinct right bindings, and no
    // second iteration, since nothing new bound.
    let seen = fetcher.queries();
    assert_eq!(seen.len(), 4, "{seen:?}");
    let mut combinations: Vec<(String, String)> = seen
        .iter()
        .map(|text| {
            let blocks = values_blocks(text);
            (blocks[0][0].clone(), blocks[1][0].clone())
        })
        .collect();
    combinations.sort();
    assert_eq!(
        combinations,
        vec![
            ("urn:1".to_owned(), "urn:3".to_owned()),
            ("urn:1".to_owned(), "urn:4".to_owned()),
            ("urn:2".to_owned(), "urn:3".to_owned()),
            ("urn:2".to_owned(), "urn:4".to_owned()),
        ]
    );
}

#[test]
fn checks_validate_bound_imports_without_fetching() {
    let fetcher = parents();
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, ANCESTORS);
    let problems = engine.check(program).into_diagnostics();
    assert!(problems.is_empty(), "{problems:?}");
    assert!(fetcher.queries().is_empty());

    // The binding atoms speak of the named columns.
    let mut engine = Engine::new();
    let (program, _) = Program::parse(
        r#"@import q(?a) from sparql { endpoint = <http://e/sparql>,
            query = "SELECT ?a WHERE { ?a ?b ?c }" } where b(?zzz) ."#,
        FileId::new(0),
    );
    let problems = engine.check(program).into_diagnostics();
    assert!(
        problems
            .iter()
            .any(|problem| problem.code() == nemo::codes::BINDING_VARIABLE_UNKNOWN),
        "{problems:?}"
    );

    // A bounded query has no restriction that means the same.
    let mut engine = Engine::new();
    let (program, _) = Program::parse(
        r#"@import q(?a) from sparql { endpoint = <http://e/sparql>,
            query = "SELECT ?a WHERE { ?a ?b ?c } LIMIT 10" } where b(?a) ."#,
        FileId::new(0),
    );
    let problems = engine.check(program).into_diagnostics();
    assert!(
        problems
            .iter()
            .any(|problem| problem.code() == nemo::codes::OPAQUE_QUERY_BOUND),
        "{problems:?}"
    );

    // Column names count the projection.
    let mut engine = Engine::new();
    let (program, _) = Program::parse(
        r#"@import q(?a, ?b) from sparql { endpoint = <http://e/sparql>,
            query = "SELECT ?a WHERE { ?a ?b ?c }" } ."#,
        FileId::new(0),
    );
    let problems = engine.check(program).into_diagnostics();
    assert!(
        problems
            .iter()
            .any(|problem| problem.code() == nemo::codes::IMPORT_COLUMN_COUNT),
        "{problems:?}"
    );
}

#[test]
fn the_rewrite_stage_binds_generic_imports_before_reasoning() {
    let fetcher = Responder::endpoint(|text| {
        // The stage specialised the query to the parent property and
        // projected the two variables the rule reads.
        assert!(text.contains("<urn:parent>"), "{text}");
        assert!(text.starts_with("SELECT ?c ?p"), "{text}");
        let mut body = String::from("?c\t?p\n");
        for iri in values_blocks(text).into_iter().flatten() {
            if let Some(parent) = parent_of(&iri) {
                body.push_str(&format!("<{iri}>\t<{parent}>\n"));
            }
        }
        body
    });
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, ANCESTORS_GENERIC);
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    let mut ancestors = engine.rows("anc").expect("derived");
    ancestors.sort();
    assert_eq!(
        ancestors,
        ["urn:a", "urn:b", "urn:c", "urn:d"]
            .map(|iri| vec![DataValue::iri(iri)])
            .to_vec()
    );
    assert_eq!(fetcher.queries().len(), 4);
    // The generic import is gone: nothing asked for every triple.
    assert!(engine.rows("triple").is_err());
    // The rewritten program is what the engine ran, and it prints.
    let texts = engine.ran_rules();
    assert!(
        texts
            .iter()
            .any(|text| text.starts_with("@import triple_query1(?c, ?p) from sparql")),
        "{texts:?}"
    );
}
