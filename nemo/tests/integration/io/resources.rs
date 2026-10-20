//! How imports name their resources: a SPARQL query checked before any fetch, the options a
//! local resource refuses, and the directory relative imports resolve against.

use nemo::{
    engine::Engine,
    model::{FileId, Program},
};
use nemo_physical::value::DataValue;

use crate::common::{TempDirectory, parse};

/// A malformed query is a check-time diagnostic with the option's
/// span, not a runtime surprise.
#[cfg(feature = "sparql")]
#[test]
fn a_broken_query_is_diagnosed_before_any_fetch() {
    let source = r#"@import q from sparql { endpoint = "http://127.0.0.1:1/sparql",
        query = "SELECT ?a WHERE { broken" } ."#;
    let (program, diagnostics) = Program::parse(source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let mut engine = Engine::new();
    let problem = engine.load(program).expect_err("the query is broken");
    assert_eq!(
        problem.code(),
        nemo::codes::NOT_A_SPARQL_QUERY,
        "{problem:?}"
    );
}

/// A query selecting no variables is refused before any request is sent.
#[cfg(feature = "sparql")]
#[test]
fn a_query_selecting_nothing_is_diagnosed_before_any_fetch() {
    let program = parse(
        r#"@import q from sparql { endpoint = "http://127.0.0.1:1/sparql",
        query = "SELECT * WHERE { }" } ."#,
    );
    let mut engine = Engine::new();
    let problem = engine.load(program).expect_err("the query has no columns");
    assert_eq!(
        problem.code(),
        nemo::codes::SPARQL_NO_PROJECTION,
        "{problem:?}"
    );
    assert!(engine.pending().is_empty(), "{:?}", engine.pending());
}

/// A base the directive gives that is not an IRI is the option's fault, not the query's.
#[cfg(feature = "sparql")]
#[test]
fn an_invalid_base_of_a_query_is_the_options_fault() {
    let program = parse(
        r#"@import q from sparql { endpoint = "http://127.0.0.1:1/sparql",
        base = "not an iri", query = "SELECT ?a WHERE { ?a ?b <x> }" } ."#,
    );
    let problem = Engine::new()
        .load(program)
        .expect_err("the base is not an IRI");
    assert_eq!(
        problem.code(),
        nemo::codes::OPTION_VALUE_TYPE,
        "{problem:?}"
    );
    assert_eq!(problem.field("option"), Some("base"), "{problem:?}");
}

/// `http_headers` on a local resource is an error,
/// since it describes a request nothing will make.
#[test]
fn headers_on_a_local_resource_are_an_error() {
    let source = r#"@import p from csv { resource = "local.csv",
        http_headers = { Authorization = "x" } } ."#;
    let (program, diagnostics) = Program::parse(source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let mut engine = Engine::new();
    let problem = engine
        .load(program)
        .expect_err("nothing sends these headers");
    assert_eq!(
        problem.code(),
        nemo::codes::HEADERS_NOT_REMOTE,
        "{problem:?}"
    );
}

/// `on_error` on a local resource is an error, since no fetch can fail.
#[test]
fn on_error_on_a_local_resource_is_an_error() {
    let program = parse(r#"@import p from csv { resource = "local.csv", on_error = warn } ."#);
    let problem = Engine::new().load(program).expect_err("nothing is fetched");
    assert_eq!(
        problem.code(),
        nemo::codes::ON_ERROR_NOT_REMOTE,
        "{problem:?}"
    );

    let program = parse(
        r#"@import p from csv { resource = "https://example.org/p.csv", on_error = maybe } ."#,
    );
    let problem = Engine::new()
        .load(program)
        .expect_err("the value is no behavior");
    assert_eq!(
        problem.code(),
        nemo::codes::UNKNOWN_OPTION_VALUE,
        "{problem:?}"
    );
}

/// The engine's import directory resolves the program's relative imports,
/// while a module's still resolve beside the module.
#[test]
fn the_import_directory_moves_the_programs_imports_but_not_a_modules() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    for name in ["data", "lib"] {
        std::fs::create_dir_all(directory.join(name)).expect("the directory creates");
    }
    std::fs::write(directory.join("data").join("rows.csv"), "3\n").expect("the data writes");
    std::fs::write(directory.join("lib").join("rows.csv"), "4\n").expect("the data writes");
    std::fs::write(
        directory.join("lib").join("loader.rls"),
        "@import rows from csv { resource = \"rows.csv\" } .\n@output rows .\n",
    )
    .expect("the module writes");
    let source = "@import p from csv { resource = \"rows.csv\" } .\n\
                  @use lib::loader {} as lib .\n\
                  q(?x) :- p(?x) .\n\
                  q(?x) :- lib::rows(?x) .\n";

    let mut engine = Engine::new();
    engine.set_import_directory(directory.join("data"));
    let file = engine
        .add_source(directory.join("main.rls").display().to_string(), source)
        .expect("the text is small");
    let (program, diagnostics) = engine.parse_source(file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let program = engine.link(program).expect("the program links");
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    let mut rows = engine.rows("q").expect("the predicate is derived");
    rows.sort();
    assert_eq!(
        rows,
        vec![vec![DataValue::integer(3)], vec![DataValue::integer(4)]]
    );
}
