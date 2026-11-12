//! The debug check of stored rows against the inferred types,
//! and the types of an import that is stored without some columns.

use super::*;
use nemo_physical::value::{Type, ValueDomain};
#[cfg(debug_assertions)]
use nemo_physical::{storage::StorageValue, trie::RowBuffer};
#[cfg(debug_assertions)]
use std::collections::HashMap;

/// Debug builds check every stored block against the inferred column types,
/// so a value outside them, which would be a bug in the analysis, panics.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "the analysis inferred")]
fn a_stored_value_outside_the_inferred_types_panics_in_debug_builds() {
    let engine = run("p(1) .");
    let &Registered { table, .. } = engine.stores.predicates.get("p").expect("registered");
    let types = HashMap::from([(table, vec![Type::of(ValueDomain::String)])]);
    let mut rows = RowBuffer::new(1);
    let one = DataValue::integer(1);
    rows.push_row(&[StorageValue::from_value(one.as_ref()).expect("inline")]);
    engine.assert_stored_types(&types, table, &rows);
}

/// Debug builds check the rows that reasoning stores too,
/// so a derived value outside the inferred column types panics.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "the analysis inferred")]
fn a_derived_value_outside_the_inferred_types_panics_in_debug_builds() {
    let mut engine = Engine::new();
    engine
        .load(parse("p(1) . q(?x) :- p(?x) ."))
        .expect("the program loads");

    // The analysis inferred integers for `q`, and strings stand in for a wrong inference.
    let &Registered { table, .. } = engine.stores.predicates.get("q").expect("registered");
    let loaded = engine.loaded.as_mut().expect("a program is loaded");
    Arc::get_mut(&mut loaded.compiled)
        .expect("no run holds the program")
        .column_types
        .insert(table, vec![Type::of(ValueDomain::String)]);

    let _ = engine.reason();
}

/// A program that reads two columns of a three-column import, so the middle one is not
/// stored.
const PRUNED_IMPORT: &str = "@import c from csv { resource = \"c.csv\", format = (string, int, \
                             string) } .\nq(?s, ?z) :- c(?s, _, ?z) .\n@output q .";

/// A row given to [import_rows](Engine::import_rows) for an import stored without a column
/// is checked at its columns as written, including the column that is not stored.
#[test]
fn rows_of_a_pruned_import_are_checked_at_their_written_columns() {
    let data = [("c.csv", "a,1,b\nc,2,d\n")];
    let declared = format!("@declare c(a: string, b: int, c: string) .\n{PRUNED_IMPORT}");
    let mut engine = load_resources(&declared, &data);
    assert_eq!(stored(&engine, "c").1, 2, "the middle column is pruned");
    engine
        .import_rows(
            "c",
            vec![vec![
                DataValue::string("x"),
                DataValue::integer(5),
                DataValue::string("y"),
            ]],
        )
        .expect("the row fits the declaration");
    let refusal = engine
        .import_rows(
            "c",
            vec![vec![
                DataValue::string("x"),
                DataValue::string("no"),
                DataValue::string("y"),
            ]],
        )
        .expect_err("the pruned column is checked too");
    assert_eq!(
        refusal.code(),
        codes::FACT_OUTSIDE_DECLARATION,
        "{refusal:?}"
    );
    assert_eq!(refusal.field("column"), Some("2"));
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "q"), 3);
}

/// A declaration that an extension gives an import stored without a column is checked
/// against the stored columns, each at its column as written.
#[test]
fn a_late_declaration_checks_a_pruned_import_at_its_written_columns() {
    let data = [("c.csv", "a,1,b\nc,2,d\n")];
    let mut engine = load_resources(PRUNED_IMPORT, &data);
    assert_eq!(stored(&engine, "c").1, 2, "the middle column is pruned");
    engine
        .extend(parse("@declare c(a: string, b: int, c: string) ."))
        .expect("the stored columns fit the declaration");

    let mut engine = load_resources(PRUNED_IMPORT, &data);
    let refusal = engine
        .extend(parse("@declare c(a: string, b: int, c: int) ."))
        .expect_err("the third column stores strings");
    assert_eq!(refusal.code(), codes::STORED_VALUES_EXCLUDED, "{refusal:?}");
    assert_eq!(refusal.field("column"), Some("3"));
    assert_eq!(refusal.field("stored"), Some("string"));
}
