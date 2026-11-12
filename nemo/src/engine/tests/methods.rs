//! What the loading methods leave in the engine: its registered predicates, its statements,
//! its sources, and the instant of the next run.

use super::*;

/// A diagnostic that names a predicate that a stage made names the written literals that
/// the predicate stands for instead, and its fields name the written predicate of these
/// literals.
#[test]
fn diagnostics_name_the_literals_that_a_stage_made_predicate_stands_for() {
    let program = parse(
        "@import triple from sparql { endpoint = <http://example.org/sparql>, \
         query = \"SELECT ?s ?p ?o WHERE { ?s ?p ?o }\" } .\n\
         anc(<urn:a>) .\nanc(?p) :- anc(?c), triple(?c, <urn:parent>, ?p) .\n@output anc .",
    );
    let mut engine = Engine::new();
    engine.load(program).expect("the program loads");
    let written = &engine
        .loaded
        .as_ref()
        .expect("the program is loaded")
        .written;
    let presented = written.present(
        Diagnostic::warning(codes::NEVER_FIRES, "a rule reads `triple_query1`")
            .with_field("predicate", "triple_query1"),
    );
    let literals = "the literals `triple(?c, <urn:parent>, ?p)`";
    assert_eq!(presented.message(), format!("a rule reads {literals}"));
    assert_eq!(presented.field("predicate"), Some("triple"));

    // So is the predicate of the binding rule.
    let presented = written.present(
        Diagnostic::warning(codes::NEVER_FIRES, "a rule reads `triple_bindings1`")
            .with_field("predicate", "triple_bindings1"),
    );
    assert_eq!(presented.message(), "a rule reads the literals `anc(?c)`");
    assert_eq!(presented.field("predicate"), Some("anc"));

    // A field that names another predicate keeps its value.
    let presented = written.present(
        Diagnostic::warning(codes::NEVER_FIRES, "a rule reads `anc`")
            .with_field("predicate", "anc"),
    );
    assert_eq!(presented.field("predicate"), Some("anc"));
}

/// A program's outputs are the predicates it declares `@output` or exports.
///
/// A program that declares neither has every derived predicate as an output,
/// so its imports are loaded partially as in any other program.
#[test]
fn outputs_are_declared_or_derived() {
    let data = [("p.csv", "1,x\n2,y\n")];
    let declared = load_resources(
        "@import p from csv { resource = \"p.csv\" } .\nq(?a) :- p(?a, _) .\n\
         r(?a) :- q(?a) .\n@output r .\n@export q to csv {} .",
        &data,
    );
    assert_eq!(declared.outputs(), ["r", "q"]);

    let mut undeclared = load_resources(
        "@import p from csv { resource = \"p.csv\" } .\nq(?a) :- p(?a, _) .",
        &data,
    );
    assert_eq!(undeclared.outputs(), ["q"]);
    assert_eq!(count(&mut undeclared, "q"), 2);

    // The import is not an output, so only the column that the rule reads is stored.
    assert_eq!(stored(&undeclared, "p"), (2, 1));
}

/// A loading method that fails, or is suspended and discarded, leaves the choice of the
/// instant to the next one, while a failure after a successful load keeps the load's instant.
#[test]
fn only_a_successful_loading_call_chooses_the_instant() {
    let missing = || parse("@import p from csv { resource = \"no-such-file.csv\" } .");
    let mut engine = Engine::new();
    assert!(engine.load(missing()).is_err(), "the resource is missing");
    assert!(
        engine.run.next_context.is_none(),
        "a failed load chooses no instant"
    );

    let progress = engine
        .start_load(parse(
            "@import p from csv { resource = \"http://example.org/p.csv\" } .",
        ))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert!(
        matches!(progress, Progress::Suspended(_)),
        "the import is suspended for its resource"
    );
    assert!(
        engine.run.next_context.is_some(),
        "a suspended load keeps its choice"
    );
    assert!(engine.discard_pending());
    assert!(
        engine.run.next_context.is_none(),
        "a discarded load chooses no instant"
    );

    engine
        .load(parse("p(1) ."))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let chosen = engine.run.next_context;
    assert!(chosen.is_some(), "a successful load chooses the instant");
    assert!(engine.extend(missing()).is_err(), "the resource is missing");
    assert_eq!(
        engine.run.next_context, chosen,
        "a failed extension keeps the load's instant"
    );
}

/// A loading method that fails, or that is discarded while it is suspended,
/// leaves none of its program's statements in the pipeline.
#[test]
fn an_abandoned_loading_method_leaves_no_statements_behind() {
    let mut engine = Engine::new();
    let empty = engine.pipeline.mark();

    engine
        .load(parse("@use lib as l ."))
        .expect_err("the program is not linked");
    assert_eq!(engine.pipeline.mark(), empty);

    engine
        .load(parse("r(1, 2) . q(?x) :- r(?x) ."))
        .expect_err("`r` is used with two numbers of columns");
    assert_eq!(engine.pipeline.mark(), empty);

    let progress = engine
        .start_load(parse(
            "@import p from csv { resource = \"http://example.org/p.csv\" } .",
        ))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert!(matches!(progress, Progress::Suspended(_)));
    assert_ne!(engine.pipeline.mark(), empty);
    assert!(engine.discard_pending());
    assert_eq!(engine.pipeline.mark(), empty);

    engine
        .load(parse("r(1) . q(?x) :- r(?x) ."))
        .expect("the program loads");
    let loaded = engine.pipeline.mark();
    engine
        .extend(parse("r(1, 2) ."))
        .expect_err("`r` has one column");
    assert_eq!(engine.pipeline.mark(), loaded);
    engine
        .reload(parse("r(1, 2) . q(?x) :- r(?x) ."))
        .expect_err("`r` is used with two numbers of columns");
    assert_eq!(engine.pipeline.mark(), loaded);

    // The loaded program is still the one that runs.
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "q"), [["1"]]);
}

/// A reload removes the statements of the program that it replaces from the pipeline,
/// and the reloaded program still explains itself.
#[test]
fn a_reload_leaves_no_statements_of_the_replaced_program_behind() {
    let mut engine = Engine::new();
    engine
        .load(parse("r(1) . q(?x) :- r(?x) ."))
        .expect("the program loads");
    let held = engine.pipeline.statement_count();

    for value in 2..5 {
        engine
            .reload(parse(&format!("r({value}) . q(?x) :- r(?x) .")))
            .expect("the program reloads");
        assert_eq!(engine.pipeline.statement_count(), held);
    }

    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "q"), [["4"]]);
    assert_eq!(engine.revisions().len(), 1);
    let rule = engine.rule(1).expect("the second statement is a rule");
    assert_eq!(rule.text, "q(?x) :- r(?x) .");

    // An extension still finds the statements that it appends to.
    engine
        .extend(parse("s(?x) :- q(?x) ."))
        .expect("the program extends");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "s"), [["4"]]);
}

/// What an engine holds stays level over repeated checks, failed loads and reloads of
/// programs of one size: its statements, its sources and its registered predicates.
#[test]
fn a_long_lived_engine_does_not_grow_with_its_calls() {
    let program = |value: usize| format!("r({value}) . q(?x) :- r(?x) .\n");
    let size = |engine: &Engine| {
        (
            engine.pipeline.statement_count(),
            engine.sources().count(),
            engine.stores.predicates.len(),
        )
    };

    let mut engine = Engine::new();
    engine
        .load_source("main.rls", program(0))
        .unwrap_or_else(|problems| panic!("{problems:?}"));
    engine.reason().expect("reasoning reaches the fixpoint");

    // The second round settles what the first call after a load still holds.
    let mut settled = None;
    for round in 1..6 {
        engine
            .reload_source("main.rls", program(round))
            .unwrap_or_else(|problems| panic!("{problems:?}"));
        engine.reason().expect("reasoning reaches the fixpoint");
        engine
            .reload_source("main.rls", "r(1, 2) . q(?x) :- r(?x) .\n")
            .expect_err("`r` is used with two numbers of columns");
        engine
            .extend_source("more.rls", "r(1) . r(")
            .expect_err("the text does not parse");

        match settled {
            None if round == 2 => settled = Some(size(&engine)),
            None => {}
            Some(settled) => assert_eq!(size(&engine), settled, "round {round}"),
        }
    }

    assert_eq!(rows(&mut engine, "q"), [["5"]]);

    // An engine without a program stays level over its checks.
    let mut engine = Engine::new();
    let mut settled = None;
    for round in 0..4 {
        let report = engine.check_source("main.rls", program(round));
        assert!(report.diagnostics().is_empty(), "{report:?}");
        let report = engine.check_source("main.rls", "q(?x, ?y) :- r(?x) .\n");
        assert!(!report.diagnostics().is_empty());

        match settled {
            None => settled = Some(size(&engine)),
            Some(settled) => assert_eq!(size(&engine), settled, "round {round}"),
        }
    }
}

/// A load that fails registers no predicate, so that a corrected program loads,
/// even one that uses a predicate of the failed program with another number of columns.
#[test]
fn a_failed_load_registers_no_predicate() {
    let mut engine = Engine::new();
    engine
        .load(parse("r(1, 2) . q(?x) :- r(?x) ."))
        .expect_err("`r` is used with two numbers of columns");
    let registered: Vec<&String> = engine.stores.predicates.keys().collect();
    assert!(registered.is_empty(), "{registered:?}");
    engine
        .load(parse("r(1) . q(?x) :- r(?x) ."))
        .expect("the corrected program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "q"), [["1"]]);
}
