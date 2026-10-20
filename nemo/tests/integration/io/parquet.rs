//! Parquet through Arrow: the round trip through the engine, pruned and filtered imports, the
//! values an import's columns hold, and the compression the format refuses.

use nemo::{
    engine::Engine,
    model::{FileId, Program},
};
use nemo_physical::value::DataValue;

use crate::common::{TempDirectory, parse};

use super::run;

/// Parquet exports write through Arrow and read back through the Parquet import,
/// the full engine cycle.
#[test]
fn parquet_round_trips_through_the_engine() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let mut engine = run(r#"p(1, "alpha") .
p(2, "beta") .
@export p to parquet .
"#);
    engine.export(directory).expect("the export writes");
    let file = directory.join("p.parquet");
    assert!(file.exists(), "the conventional name is predicate.parquet");

    let mut reader = Engine::new();
    let source = r#"@import q from parquet { resource = "p.parquet" } ."#;
    let (program, diagnostics) = Program::parse(source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    reader.set_import_directory(directory);
    reader.load(program).expect("the file reads back");
    reader.reason().expect("nothing to derive");
    let mut rows = reader.rows("q").expect("the predicate loaded");
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![DataValue::integer(1), DataValue::string("alpha")],
            vec![DataValue::integer(2), DataValue::string("beta")],
        ]
    );
}

/// A Parquet import delivers only the columns the rules read,
/// the dropped ones not even read from the file, and only the rows they can use.
#[test]
fn parquet_imports_prune_and_filter() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let mut engine = run(r#"p(1, "alpha", 10) .
p(2, "beta", 20) .
p(3, "gamma", 20) .
@export p to parquet .
"#);
    engine.export(directory).expect("the export writes");

    let mut reader = Engine::new();
    let program = parse(
        r#"@import q from parquet { resource = "p.parquet" } .
big(?n) :- q(?id, ?name, ?n), ?n > 15 .
@output big ."#,
    );
    reader.set_import_directory(directory);
    reader.load(program).expect("the file reads back");
    reader.reason().expect("the rule derives");
    assert!(
        crate::common::arities(&reader).contains(&(String::from("q"), 3)),
        "the predicate is listed at its written width"
    );
    // Only the number column is delivered, and only the rows above the
    // threshold.
    assert_eq!(
        reader
            .rows("q")
            .map_err(|refusal| refusal.message().to_owned()),
        Err(String::from(
            "`q` is not stored whole: columns 1 and 2 are not stored, and only the rows \
             the rules reading it can use are kept"
        ))
    );
    assert_eq!(
        reader.rows("big").expect("the rule derives"),
        vec![vec![DataValue::integer(20)]]
    );
}

/// A Parquet import's columns hold any value, since the file's schema is read only with
/// the file, so its columns can be declared only as `any`, or as what conditions establish.
#[test]
fn parquet_imports_hold_any_value() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let mut engine = run(r#"p(1, "alpha") .
p(2, "beta") .
@export p to parquet .
"#);
    engine.export(directory).expect("the export writes");

    let mut reader = Engine::new();
    let program = parse(
        r#"@declare q(n: int, s: string) .
@import q from parquet { resource = "p.parquet" } ."#,
    );
    reader.set_import_directory(directory);
    let refusal = reader
        .load(program)
        .expect_err("the columns are not shown to be integers and strings");
    assert_eq!(refusal.code(), nemo::codes::IMPORT_OUTSIDE_DECLARATION);

    let mut reader = Engine::new();
    let program = parse(
        r#"@declare q(n: any, s: any) .
@import q from parquet { resource = "p.parquet" } ."#,
    );
    reader.set_import_directory(directory);
    reader.load(program).expect("the file reads back");
    assert_eq!(reader.rows("q").expect("registered").len(), 2);
    assert!(reader.reports().is_empty(), "{:?}", reader.reports());

    // Conditions on the import narrow its columns to what is declared.
    let mut reader = Engine::new();
    let program = parse(
        r#"@declare q(n: int, s: string) .
@import q(?n, ?s) from parquet { resource = "p.parquet" } where ISINTEGER(?n), ISSTRING(?s) ."#,
    );
    reader.set_import_directory(directory);
    reader
        .load(program)
        .expect("the conditions establish the declared types");
    assert_eq!(reader.rows("q").expect("registered").len(), 2);
}

/// A gzip request on a Parquet export is an error, since the format compresses internally.
#[test]
fn parquet_rejects_stream_compression() {
    let source = "p(1) .\n@export p to parquet { compression = \"gzip\" } .\n";
    let (program, diagnostics) = Program::parse(source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let mut engine = Engine::new();
    let problem = engine.load(program).expect_err("parquet compresses itself");
    assert_eq!(problem.code(), nemo::codes::UNKNOWN_OPTION, "{problem:?}");
    assert_eq!(problem.field("option"), Some("compression"));
    assert_eq!(problem.field("format"), Some("parquet"));
}

/// Column variables naming another number of columns than the file has are counted against
/// the file.
#[test]
fn named_columns_are_counted_against_the_file() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let mut engine = run("p(1, \"alpha\") .\n@export p to parquet .\n");
    engine.export(directory).expect("the export writes");

    let mut reader = Engine::new();
    let program = parse(r#"@import q(?n) from parquet { resource = "p.parquet" } ."#);
    reader.set_import_directory(directory);
    let problem = reader.load(program).expect_err("the file has two columns");
    assert_eq!(
        problem.code(),
        nemo::codes::IMPORT_COLUMN_COUNT,
        "{problem:?}"
    );
    assert_eq!(
        (problem.field("declared"), problem.field("arity")),
        (Some("1"), Some("2"))
    );
}
