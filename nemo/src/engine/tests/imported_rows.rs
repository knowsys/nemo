//! Rows given through [import_rows](Engine::import_rows) before the first load,
//! which are kept apart from the program's statements.

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
    let loaded = engine.loaded.as_ref().expect("loaded");
    let (written, compiled) = (loaded.written.revision(), loaded.revision);
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
