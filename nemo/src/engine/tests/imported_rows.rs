//! Rows given through [import_rows](Engine::import_rows) before the first load:
//! kept apart from the program's statements, and checked against the program as its facts are.

use nemo_physical::value::TupleValue;

use super::*;

/// One row of integers.
fn integers(values: &[i64]) -> Vec<DataValue> {
    values
        .iter()
        .map(|&value| DataValue::integer(value))
        .collect()
}

/// Rows given before the load add no statement to the program, and count as loaded facts.
#[test]
fn imported_rows_add_no_statements() {
    let mut engine = Engine::new();
    engine
        .import_rows("p", (0..100).map(|value| integers(&[value])))
        .expect("the rows are kept for the load");
    engine
        .load(parse("q(?x) :- p(?x) ."))
        .expect("the program loads");
    let written = engine.written.as_ref().expect("loaded").revision();
    let compiled = engine.revision.expect("loaded");
    for revision in [written, compiled] {
        assert_eq!(
            engine.pipeline.statements(revision).count(),
            1,
            "the rule is the only statement"
        );
    }
    assert_eq!(engine.loaded_facts(), 100);
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(engine.count("q").expect("q is derived"), 100);
}

/// A row that a declaration refuses fails the load with the diagnostic that a fact with its
/// values gets, and a note that names the row.
/// Of several refused values, the first in row order is reported, as for facts.
#[test]
fn imported_rows_are_refused_as_facts_are() {
    let mut engine = Engine::new();
    engine
        .import_rows(
            "p",
            [
                integers(&[1, 2]),
                vec![DataValue::integer(3), DataValue::string("x")],
                vec![DataValue::string("y"), DataValue::integer(4)],
            ],
        )
        .expect("the rows are checked when a program loads");
    let refusal = engine
        .load(parse("@declare p(int, int) . q(?x) :- p(?x, _) ."))
        .expect_err("the declaration refuses two rows");
    assert_eq!(
        refusal.code(),
        codes::FACT_OUTSIDE_DECLARATION,
        "{refusal:?}"
    );
    assert_eq!(refusal.field("column"), Some("2"), "{refusal:?}");
    assert!(
        refusal
            .notes()
            .iter()
            .any(|note| note == "the row given to `import_rows` is `p(3, \"x\")`"),
        "{refusal:?}"
    );
}

/// A structure in a row is checked against the `@type` of its label, as in a fact.
#[test]
fn imported_structures_are_checked_against_their_types() {
    let mut engine = Engine::new();
    let point = TupleValue::labeled("point", [DataValue::integer(1), DataValue::string("a")]);
    engine
        .import_rows("p", [vec![DataValue::tuple(point)]])
        .expect("the rows are checked when a program loads");
    let refusal = engine
        .load(parse("@type point(int, int) . q(?x) :- p(?x) ."))
        .expect_err("the type of `point` refuses the row");
    assert_eq!(refusal.code(), codes::VALUE_OUTSIDE_TYPE, "{refusal:?}");
    assert!(
        refusal
            .notes()
            .iter()
            .any(|note| note.starts_with("the row given to `import_rows` is `p(")),
        "{refusal:?}"
    );
}

/// A column that holds values of several types is analyzed with all of them.
///
/// Debug builds also check every stored row against the analyzed types.
#[test]
fn imported_rows_seed_their_columns_with_every_type() {
    let mut engine = Engine::new();
    engine
        .import_rows(
            "p",
            [
                integers(&[1]),
                vec![DataValue::string("a")],
                vec![DataValue::iri("b")],
            ],
        )
        .expect("the rows are kept for the load");
    engine
        .load(parse(
            "s(?x) :- p(?x), ISSTRING(?x) . i(?x) :- p(?x), ISIRI(?x) .",
        ))
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(engine.count("s").expect("s is derived"), 1);
    assert_eq!(engine.count("i").expect("i is derived"), 1);
    assert!(
        engine
            .reports()
            .iter()
            .all(|report| report.code() != codes::NEVER_FIRES),
        "{:?}",
        engine.reports()
    );
}

/// A call whose rows disagree on their number of values adds none of them,
/// and a later call for the predicate needs as many values per row as the earlier ones.
#[test]
fn imported_rows_keep_one_arity() {
    let mut engine = Engine::new();
    let refusal = engine
        .import_rows("p", [integers(&[1]), integers(&[1, 2])])
        .expect_err("the rows disagree");
    assert_eq!(refusal.code(), codes::ARITY_MISMATCH, "{refusal:?}");
    assert!(
        refusal
            .notes()
            .iter()
            .any(|note| note == "the row given to `import_rows` is `p(1, 2)`"),
        "{refusal:?}"
    );
    engine
        .import_rows("p", [integers(&[2])])
        .expect("one value per row");
    let refusal = engine
        .import_rows("p", [integers(&[3, 4])])
        .expect_err("the earlier rows have one value");
    assert_eq!(refusal.code(), codes::ARITY_MISMATCH, "{refusal:?}");

    engine
        .load(parse("q(?x) :- p(?x) ."))
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(
        rows(&mut engine, "q"),
        [["2"]],
        "only the accepted call's row"
    );
}

/// Of rows that disagree on their number of values, the one refused is the row without the
/// number that the predicate's rows need, even if it is the first.
#[test]
fn the_row_without_the_predicates_arity_is_refused() {
    let first_refused = |refusal: &Diagnostic, row: &str| {
        assert_eq!(refusal.code(), codes::ARITY_MISMATCH, "{refusal:?}");
        assert!(
            refusal
                .notes()
                .iter()
                .any(|note| note == &format!("the row given to `import_rows` is `{row}`")),
            "{refusal:?}"
        );
    };
    let mut engine = Engine::new();
    engine
        .import_rows("p", [integers(&[1])])
        .expect("the rows are kept for the load");
    let refusal = engine
        .import_rows("p", [integers(&[2, 3]), integers(&[4])])
        .expect_err("the earlier rows have one value");
    first_refused(&refusal, "p(2, 3)");

    engine
        .load(parse("q(?x) :- p(?x) . r(5, 6) ."))
        .expect("the program loads");
    let refusal = engine
        .import_rows("r", [integers(&[7]), integers(&[8, 9])])
        .expect_err("the program gives `r` two values");
    first_refused(&refusal, "r(7)");
}

/// A row without values makes a nullary predicate hold, before the load and after it.
#[test]
fn imported_rows_without_values_hold_a_nullary_predicate() {
    let mut engine = Engine::new();
    engine
        .import_rows("ready", [Vec::new()])
        .expect("the rows are kept for the load");
    engine
        .load(parse(
            "go(1) :- ready() . wait(2) :- later() . @declare later() .",
        ))
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "go"), [["1"]]);
    assert_eq!(engine.count("wait").expect("wait is derived"), 0);

    engine
        .import_rows("later", [Vec::new(), Vec::new()])
        .expect("the predicate is the program's");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "wait"), [["2"]]);
}

/// A null inside a structure is refused at the call, as a null is.
#[test]
fn a_null_inside_a_structure_does_not_import() {
    let mut engine = Engine::new();
    let pair = TupleValue::new([DataValue::integer(1), DataValue::null(3)]);
    let refusal = engine
        .import_rows("p", [vec![DataValue::tuple(pair)]])
        .expect_err("no term denotes a null");
    assert_eq!(refusal.code(), codes::NULL_IN_ROW, "{refusal:?}");
}

/// A load that fails keeps the rows, so that a corrected program loads with them.
#[test]
fn a_failed_load_keeps_the_imported_rows() {
    let mut engine = Engine::new();
    engine
        .import_rows("p", [vec![DataValue::string("x")]])
        .expect("the rows are kept for the load");
    engine
        .load(parse("@declare p(int) ."))
        .expect_err("the declaration refuses the row");
    engine
        .load(parse("@declare p(string) . q(?x) :- p(?x) ."))
        .expect("the corrected program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(engine.count("q").expect("q is derived"), 1);
}

/// Rows given to a predicate that the program never names make it a predicate of the program:
/// it is listed, and takes more rows after the load.
#[test]
fn imported_rows_make_their_predicate_the_programs() {
    let mut engine = Engine::new();
    engine
        .import_rows("given", [integers(&[1])])
        .expect("the rows are kept for the load");
    engine.load(parse("q(1) .")).expect("the program loads");
    assert_eq!(
        engine.predicate("given").map(|info| info.role),
        Some(PredicateRole::Extensional)
    );
    engine
        .import_rows("given", [integers(&[2])])
        .expect("the predicate is the program's");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "given"), [["1"], ["2"]]);
}

/// A reload keeps the rows given before the load while their predicate is unchanged,
/// and reports dropping them when it rebuilds the predicate,
/// as for rows given after the load.
#[test]
fn a_reload_keeps_the_rows_given_before_the_load() {
    let mut engine = Engine::new();
    engine
        .import_rows("p", [integers(&[1])])
        .expect("the rows are kept for the load");
    engine
        .load(parse("q(?x) :- p(?x) ."))
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    engine
        .reload(parse("q(?x) :- p(?x) . r(?x) :- p(?x) ."))
        .expect("the replacement loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "r"), [["1"]]);

    engine
        .reload(parse("@declare p(int) . q(?x) :- p(?x) ."))
        .expect("the replacement loads");
    assert!(
        engine
            .reports()
            .iter()
            .any(|report| report.code() == codes::IMPORTED_ROWS_DROPPED),
        "{:?}",
        engine.reports()
    );
}

/// An imported predicate with rows given through `import_rows` is stored whole, as one with
/// facts is, though the rules read only one of its columns,
/// and stays whole through a reload that keeps it.
#[test]
fn imported_rows_keep_an_import_whole() {
    let source = r#"@import p from csv { resource = "p.csv", format = (int, int) } .
                    q(?x) :- p(?x, _) ."#;
    let mut engine = Engine::new();
    engine.add_resource("p.csv", b"1,10\n".to_vec());
    engine
        .import_rows("p", [integers(&[2, 20])])
        .expect("the rows are kept for the load");
    engine.load(parse(source)).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "p"), [["1", "10"], ["2", "20"]]);

    engine.reload(parse(source)).expect("the replacement loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "p"), [["1", "10"], ["2", "20"]]);
}

/// A row given before the load traces as a loaded fact.
#[test]
fn imported_rows_trace_as_loaded_facts() {
    let mut engine = Engine::new();
    engine
        .import_rows("edge", [vec![DataValue::iri("b"), DataValue::iri("c")]])
        .expect("the rows are kept for the load");
    engine
        .load(parse(
            "edge(a, b) .
             reach(?x, ?y) :- edge(?x, ?y) .
             reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .",
        ))
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    let fact = Fact::parse("reach(a, c) .").expect("the fact parses");
    let response = engine.trace(&[fact]).expect("tracing answers");
    assert!(
        response
            .nodes
            .iter()
            .any(|node| node.fact == "edge(b, c)" && node.derivation.is_none()),
        "{response:?}"
    );
}
