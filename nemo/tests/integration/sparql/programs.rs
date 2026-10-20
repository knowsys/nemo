//! Programs with SPARQL imports from check to report: reloads and extensions, declarations,
//! stratification, failed fetches, warnings, and the predicates they list and store.

use nemo::engine::{Engine, PredicateRole, remote::Failure};
use nemo_physical::value::DataValue;

use crate::common::{Responder, arities, parse_into, roles};

use super::{
    ANCESTORS, ANCESTORS_GENERIC, generic_ancestors, parents, parents_generic, values_blocks,
};

/// A reload projecting a bound import differently gives its fetch a
/// scratch table of the new width.
#[test]
fn reloads_replace_a_bound_imports_scratch_with_its_width() {
    let fetcher = Responder::endpoint(|text| {
        let wide = text.contains("SELECT ?x ?y");
        let mut body = String::from(if wide { "?x\t?y\n" } else { "?x\n" });
        for iri in values_blocks(text).into_iter().flatten() {
            if wide {
                body.push_str(&format!("<{iri}>\t<urn:y>\n"));
            } else {
                body.push_str(&format!("<{iri}>\n"));
            }
        }
        body
    });
    let wide = r#"
        @import q(?x, ?y) from sparql {
            endpoint = <http://example.org/sparql>,
            query = "SELECT ?x ?y WHERE { ?x <urn:p> ?y }"
        } where b(?x) .
        b(<urn:a>) .
        r(?y) :- q(?x, ?y) .
        @output r .
    "#;
    let narrow = r#"
        @import q(?x) from sparql {
            endpoint = <http://example.org/sparql>,
            query = "SELECT ?x WHERE { ?x <urn:p> ?y }"
        } where b(?x) .
        b(<urn:a>) .
        r(?x) :- q(?x) .
        @output r .
    "#;
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, wide);
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(
        engine.rows("r").expect("derived"),
        vec![vec![DataValue::iri("urn:y")]]
    );
    let program = parse_into(&mut engine, narrow);
    fetcher
        .drive(&mut engine, |engine| engine.reload(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(
        engine.rows("r").expect("derived"),
        vec![vec![DataValue::iri("urn:a")]]
    );
}

/// A negated remote atom is not bound by a local group that depends on
/// the rule's head: the program still stratifies, and the atom is
/// fetched by its own pattern.
#[test]
fn negated_remote_atoms_do_not_close_a_negative_cycle() {
    let fetcher = Responder::endpoint(|_| String::from("?s\t?z\n"));
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
        @import t from sparql {
            endpoint = <http://example.org/sparql>,
            query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
        } .
        edge(<urn:a>, <urn:b>) . edge(<urn:b>, <urn:c>) .
        reach(<urn:a>) .
        reach(?y) :- reach(?x), edge(?x, ?y), ~t(?y, <urn:blocked>, ?z) .
        @output reach .
        "#,
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(engine.count("reach").expect("derived"), 3);
}

/// A declaration on an import the rewrite stage specializes is refused like on the import
/// itself, since its columns hold any term an endpoint returns.
#[test]
fn rewritten_imports_carry_their_declarations() {
    let mut checker = Engine::new();
    let problems = checker
        .check_source(
            "declared.rls",
            r#"
        @declare t(s: iri, p: iri, o: iri) .
        @import t from sparql {
            endpoint = <http://example.org/sparql>,
            query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
        } .
        known(?o) :- t(<urn:a>, <urn:knows>, ?o) .
        @output known .
        "#,
        )
        .into_diagnostics();
    assert!(
        problems
            .iter()
            .any(|problem| problem.code() == nemo::codes::IMPORT_OUTSIDE_DECLARATION),
        "{problems:?}"
    );
}

/// An extension's rules over the loaded program's imports are rewritten
/// in its scope: the import the loaded rewrite took out comes back for
/// them, fresh names avoid the loaded ones, and each rule reads its own
/// query.
#[test]
fn extensions_rewrite_in_the_scope_of_the_loaded_program() {
    let fetcher = Responder::endpoint(|text| {
        let mut body = String::from("?o\n");
        if text.contains("<urn:a>") {
            body.push_str("<urn:1>\n");
        }
        if text.contains("<urn:b>") {
            body.push_str("<urn:2>\n");
        }
        body
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
        @import t from sparql {
            endpoint = <http://example.org/sparql>,
            query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
        } .
        next(?o) :- t(<urn:a>, <urn:p>, ?o) .
        @output next .
        "#,
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let extension = parse_into(
        &mut engine,
        "other(?o) :- t(<urn:b>, <urn:p>, ?o) . @output other .",
    );
    fetcher
        .drive(&mut engine, |engine| engine.extend(extension))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(
        engine.rows("next").expect("derived"),
        vec![vec![DataValue::iri("urn:1")]]
    );
    assert_eq!(
        engine.rows("other").expect("derived"),
        vec![vec![DataValue::iri("urn:2")]]
    );
}

/// A failed application, an endpoint that responds with an error, is reported,
/// and the program stays loaded for the next attempt.
#[test]
fn a_failed_fetch_keeps_the_program() {
    let refusing =
        Responder::failing_endpoint(|_| Err(Failure::response(503, "the endpoint is down")));
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, ANCESTORS);
    refusing
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let problem = refusing
        .drive(&mut engine, Engine::reason)
        .expect_err("the fetch fails");
    assert_eq!(problem.code(), nemo::codes::UNFETCHABLE, "{problem:?}");
    assert!(
        problem
            .field("reason")
            .is_some_and(|reason| reason.contains("503")),
        "{problem:?}"
    );
    assert!(
        engine.pending().is_empty(),
        "a failed run leaves no suspension"
    );
    assert!(
        !engine.ran_rules().is_empty(),
        "the program is still loaded"
    );
    let again = refusing
        .drive(&mut engine, Engine::reason)
        .expect_err("the fetch fails again");
    assert_eq!(again.code(), nemo::codes::UNFETCHABLE, "{again:?}");
    parents()
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(engine.count("anc").expect("derived"), 4);
}

#[test]
fn a_warning_a_binding_rule_copies_is_reported_once() {
    let mut engine = Engine::new();
    let endpoint = parents_generic();
    let program = parse_into(
        &mut engine,
        r#"
@import triple from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
} .
word("a") . value(1) .
dead(?x, ?y) :- word(?x), value(?x), triple(?x, <urn:parent>, ?y) .
@output dead .
"#,
    );
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let warnings: Vec<_> = engine
        .reports()
        .iter()
        .filter(|report| report.code() == nemo::codes::NEVER_FIRES)
        .collect();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    let json = format!("{warnings:?}");
    assert!(!json.contains("_bindings"), "{json}");
}

#[test]
fn an_import_matched_at_its_endpoint_is_listed_but_not_stored() {
    let mut engine = generic_ancestors();
    assert_eq!(arities(&engine), [("anc".into(), 1), ("triple".into(), 3)]);
    assert_eq!(
        roles(&engine),
        [
            ("anc".into(), PredicateRole::Both),
            ("triple".into(), PredicateRole::Extensional),
        ]
    );
    let refusal = engine.rows("triple").expect_err("triple is not stored");
    assert_eq!(refusal.code(), nemo::codes::NOT_STORED_WHOLE);
    assert_eq!(
        refusal.message(),
        "`triple` is not stored whole: it is not stored, since the rules reading it \
         query it at its endpoint"
    );
    assert_eq!(refusal.hints(), ["declare it `@output`"]);
    // The fetched bindings live under hidden names and count toward
    // neither figure.
    assert_eq!((engine.loaded_facts(), engine.derived_facts()), (1, 3));
}

#[test]
fn a_derived_predicate_a_fold_imports_is_stored_whole() {
    let fetcher = Responder::endpoint(|text| {
        assert!(text.starts_with("SELECT ?e ?t"), "{text}");
        String::from("?e\t?t\n<urn:x>\t<urn:Event>\n<urn:y>\t<urn:Event>\n")
    });
    let mut engine = Engine::new();
    let endpoint = fetcher;
    let program = parse_into(
        &mut engine,
        r#"
@import triple from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
} .
event(?e, ?t) :- triple(?e, <urn:type>, ?t) .
kind(?t) :- event(_, ?t) .
@output kind .
"#,
    );
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    // `kind` reads one column of `event`, and the import standing for
    // its rule still stores both.
    let mut events = engine.rows("event").expect("stored whole");
    events.sort();
    assert_eq!(
        events,
        [
            vec![DataValue::iri("urn:x"), DataValue::iri("urn:Event")],
            vec![DataValue::iri("urn:y"), DataValue::iri("urn:Event")],
        ]
    );
    assert_eq!(
        roles(&engine),
        [
            ("event".into(), PredicateRole::Derived),
            ("kind".into(), PredicateRole::Derived),
            ("triple".into(), PredicateRole::Extensional),
        ]
    );
    assert_eq!((engine.loaded_facts(), engine.derived_facts()), (0, 3));
}

#[test]
fn an_import_the_stage_answers_at_its_endpoint_is_checked_as_written() {
    // The stage queries `triple` at its endpoint and removes the import,
    // whose options are still checked.
    let source = ANCESTORS_GENERIC.replace(
        "?s ?p ?o WHERE { ?s ?p ?o }\"",
        "?s ?p ?o WHERE { ?s ?p ?o }\",\n    page = 0",
    );
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, &source);
    let checked = engine.check(program.clone()).into_diagnostics();
    assert!(
        checked
            .iter()
            .any(|problem| problem.code() == nemo::codes::PAGE_NOT_A_COUNT),
        "{checked:?}"
    );
    let problem = engine.load(program).expect_err("the option is refused");
    assert_eq!(problem.code(), nemo::codes::PAGE_NOT_A_COUNT, "{problem:?}");
    assert_eq!(problem.labels().len(), 1, "{problem:?}");
}

#[test]
fn a_rewritten_program_reports_what_reading_finds() {
    // The file's width is found by reading it, in a program the stage
    // rewrote.
    let source = format!(
        "{ANCESTORS_GENERIC}@import extra from csv {{ resource = \"missing.csv\" }} .\n\
         @output extra .\n"
    );
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, &source);
    let problem = engine.load(program).expect_err("the file is missing");
    assert_eq!(problem.code(), nemo::codes::UNREADABLE, "{problem:?}");
}
