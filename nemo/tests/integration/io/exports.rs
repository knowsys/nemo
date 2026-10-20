//! RDF exports: the column count the format takes, and the rows without an RDF form.

use nemo::{
    engine::Engine,
    model::{FileId, Program},
};

use super::run;

/// An RDF export of a predicate with the wrong arity is an error at
/// load, naming the number of columns the format takes.
#[test]
fn rdf_exports_check_the_column_count() {
    let source = "p(1, 2) .\n@export p to ntriples .\n";
    let (program, diagnostics) = Program::parse(source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let mut engine = Engine::new();
    let problem = engine.load(program).expect_err("two columns are no triple");
    assert_eq!(problem.code(), nemo::codes::RDF_EXPORT_ARITY, "{problem:?}");
    assert_eq!(problem.field("columns"), Some("3"));
    assert_eq!(problem.field("arity"), Some("2"));
}

/// A row without an RDF form is dropped and counted, never silently.
#[test]
fn rdf_exports_count_dropped_rows() {
    let mut engine = run(r#"t(<http://example.org/a>, <http://example.org/p>, 1) .
t("no subject", <http://example.org/p>, 2) .
@export t to ntriples .
"#);
    let specs = engine.exports().to_vec();
    let bytes = engine.export_bytes(&specs[0]).expect("the export writes");
    let text = String::from_utf8(bytes).expect("text");
    assert_eq!(text.lines().count(), 1, "one row has an RDF form: {text}");
    assert!(
        engine
            .reports()
            .iter()
            .any(|report| report.code() == nemo::codes::EXPORT_DROPPED),
        "the dropped row is reported: {:?}",
        engine.reports()
    );
}
