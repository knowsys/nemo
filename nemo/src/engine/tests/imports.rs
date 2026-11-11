//! Imports: pruned columns, pushed filters, column formats, and the checks
//! against declarations.

use super::*;

/// An import column that no rule reads is not stored, unless the engine retains imports.
#[test]
fn unread_import_columns_prune_unless_retained() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_engine_prune.csv");
    std::fs::write(
        &path,
        "1,x,10
2,y,20
2,z,20
",
    )
    .expect("the temp file writes");
    let source = "@import p from csv{resource=\"nemo_engine_prune.csv\", format=(int,string,int)} .
q(?a) :- p(?a, _, ?b), ?b > 5 .
@output q .
";

    // No rule reads the string column, so the stored table leaves it
    // out, and the two rows that differ only there become one.
    let program = parse(source);
    let mut engine = Engine::new();
    engine.set_import_directory(directory);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(stored(&engine, "p"), (2, 2));
    assert_eq!(count(&mut engine, "q"), 2);

    // With imports retained, the same program keeps the written width and every
    // row.
    let (program, _) = Program::parse(source);
    let mut engine = Engine::new();
    engine.retain_imports();
    engine.set_import_directory(directory);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "p"), 3);
}

/// A universal variable that the rule uses nowhere else reads nothing, like `_`,
/// so its column is not stored.
#[test]
fn named_unused_variables_prune_like_anonymous_ones() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_engine_prune_named.csv");
    std::fs::write(&path, "1,x,10\n2,y,20\n2,z,20\n").expect("the temp file writes");
    let source =
        "@import p from csv{resource=\"nemo_engine_prune_named.csv\", format=(int,string,int)} .
q(?a) :- p(?a, ?label, ?b), ?b > 5 .
@output q .
";
    let program = parse(source);
    let mut engine = Engine::new();
    engine.set_import_directory(directory);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(stored(&engine, "p"), (2, 2), "the label column is gone");
    assert_eq!(count(&mut engine, "q"), 2);
    assert!(
        arities(&engine).contains(&(String::from("p"), 3)),
        "the listed arity is the written one"
    );
    assert_eq!(
        engine
            .rows("p")
            .map_err(|refusal| refusal.message().to_owned()),
        Err(String::from(
            "`p` is not stored whole: column 2 is not stored, and only the rows the rules \
             reading it can use are kept"
        ))
    );
}

/// If every rule that reads an imported predicate constrains it,
/// by a constant in the atom or by comparisons on its variables,
/// the import stores only the rows that some rule can use.
///
/// One rule that reads the predicate without a constraint keeps every row.
#[test]
fn constrained_readers_push_their_filter_into_the_import() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_engine_filter.csv");
    std::fs::write(&path, "1,5\n2,7\n3,20\n4,5\n5,1\n").expect("the temp file writes");
    let load = |source: &str| {
        let program = parse(source);
        let mut engine = Engine::new();
        engine.set_import_directory(directory);
        engine.load(program).expect("the program loads");
        engine.reason().expect("reasoning reaches the fixpoint");
        engine
    };
    let import = "@import p from csv{resource=\"nemo_engine_filter.csv\", format=(int,int)} .";

    let mut engine = load(&format!(
        "{import}
q(?a) :- p(?a, 5) .
r(?a) :- p(?a, ?v), ?v > 10, ?v < 100 .
@output q, r ."
    ));
    assert_eq!(
        stored(&engine, "p").0,
        3,
        "rows with 5, or between 10 and 100"
    );
    assert_eq!(count(&mut engine, "q"), 2);
    assert_eq!(count(&mut engine, "r"), 1);

    let mut engine = load(&format!(
        "{import}
q(?a) :- p(?a, 5) .
s(?a, ?v) :- p(?a, ?v) .
@output q, s ."
    ));
    assert_eq!(
        count(&mut engine, "p"),
        5,
        "an unconstrained reader keeps every row"
    );

    // A comparison constrains its column wherever else the variable
    // occurs, since no match escapes a body condition.
    let mut engine = load(&format!(
        "{import}
t(?v) :- p(?a, ?v), ?v > 10, ?a > 2 .
@output t ."
    ));
    assert_eq!(stored(&engine, "p").0, 1, "both columns constrain");
    assert_eq!(rows(&mut engine, "t"), vec![vec![String::from("20")]]);

    // The rule draws once per row, and a copy of the comparison in the import would draw
    // again.
    let mut engine = load(&format!(
        "{import}
u(?a) :- p(?a, ?v), ?v < RAND() .
@output u ."
    ));
    assert_eq!(
        stored(&engine, "p").0,
        5,
        "a nondeterministic comparison stays in the rule"
    );
    assert_eq!(count(&mut engine, "u"), 0);
}

/// Rows added through [import_rows](Engine::import_rows) after the load still pass through
/// the rule's own condition, since a filter pushed into an import reduces only what is
/// loaded, never what the rules derive.
#[test]
fn rows_imported_after_a_pushed_filter_still_pass_the_rule() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_engine_filter_rows.csv");
    std::fs::write(&path, "1,5\n2,7\n").expect("the temp file writes");
    let source = "@import p from csv{resource=\"nemo_engine_filter_rows.csv\", format=(int,int)} .
q(?a) :- p(?a, 5) .
@output q .
";
    let program = parse(source);
    let mut engine = Engine::new();
    engine.set_import_directory(directory);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "q"), 1);
    engine
        .import_rows(
            "p",
            [
                vec![DataValue::integer(8), DataValue::integer(5)],
                vec![DataValue::integer(9), DataValue::integer(6)],
            ],
        )
        .expect("rows join a loaded engine");
    engine.reason().expect("reasoning continues");
    assert_eq!(stored(&engine, "p").0, 3);
    assert_eq!(
        count(&mut engine, "q"),
        2,
        "only the matching host row derives"
    );
}

/// An extension that reads rows the import left out is refused,
/// since the stored table cannot supply them.
///
/// With imports retained, the same extension succeeds.
#[test]
fn an_extension_relaxing_a_pushed_filter_is_an_error() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_engine_filter_extend.csv");
    std::fs::write(&path, "1,5\n2,7\n").expect("the temp file writes");
    let source =
        "@import p from csv{resource=\"nemo_engine_filter_extend.csv\", format=(int,int)} .
q(?a) :- p(?a, 5) .
@output q .
";
    let extension = "r(?a) :- p(?a, 7) .";

    let mut engine = Engine::new();
    engine.set_import_directory(directory);
    engine.load(parse(source)).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    let refusal = engine
        .extend(parse(extension))
        .expect_err("the rows for 7 were never stored");
    assert_eq!(refusal.code(), codes::PRUNING_INSTABILITY, "{refusal:?}");

    let mut engine = Engine::new();
    engine.retain_imports();
    engine.set_import_directory(directory);
    engine.load(parse(source)).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    engine
        .extend(parse(extension))
        .expect("every row was stored");
    engine.reason().expect("reasoning continues");
    assert_eq!(count(&mut engine, "r"), 1);
}

/// RDF and JSON imports, too, store only the columns and rows that the rules read.
#[test]
fn rdf_and_json_imports_prune_and_filter_too() {
    let mut engine = Engine::new();
    engine.add_resource(
        "graph.nt",
        "<http://x/a> <http://x/p> <http://x/1> .
<http://x/a> <http://x/p> <http://x/2> .
<http://x/b> <http://x/q> <http://x/3> .
"
        .as_bytes()
        .to_vec(),
    );
    engine.add_resource(
        "doc.json",
        r#"{"k": 1, "l": [true, false]}"#.as_bytes().to_vec(),
    );
    let source = r#"@import g from ntriples { resource = "graph.nt" } .
@import j from json { resource = "doc.json" } .
subject(?s) :- g(?s, <http://x/p>, ?o) .
kind(?t) :- j(?id, <type>, ?t) .
@output subject, kind .
"#;
    let program = parse(source);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    // No rule reads the object, and the predicate column is a constant,
    // so the graph stores two columns and only the `p` statements,
    // whose two rows become one.
    assert!(arities(&engine).contains(&(String::from("g"), 3)));
    assert_eq!(stored(&engine, "g"), (1, 2));
    assert_eq!(count(&mut engine, "subject"), 1);
    // No rule reads the document's node ids, and the key is a constant,
    // so the table stores two columns of the `type` rows, one per node,
    // and the five nodes are of four kinds.
    assert!(arities(&engine).contains(&(String::from("j"), 3)));
    assert_eq!(stored(&engine, "j"), (4, 2), "object, number, array, bool");
    assert_eq!(count(&mut engine, "kind"), 4);
}

/// `limit` caps what an import reads and what an export writes,
/// and on RDF and SPARQL imports, `format` keeps or skips columns of terms.
#[test]
fn limits_and_term_column_formats_apply() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_engine_limit.csv");
    std::fs::write(&path, "1\n2\n3\n4\n").expect("the temp file writes");
    let mut engine = Engine::new();
    engine.retain_imports();
    engine.add_resource(
        "graph.nt",
        "<http://x/a> <http://x/p> <http://x/1> .
<http://x/b> <http://x/p> <http://x/2> .
<http://x/c> <http://x/p> <http://x/3> .
"
        .as_bytes()
        .to_vec(),
    );
    let source = r#"@import p from csv { resource = "nemo_engine_limit.csv", limit = 2 } .
@import g from ntriples { resource = "graph.nt", format = (term, skip, term), limit = 2 } .
@export p to csv { resource = "limited.csv", limit = 1 } .
"#;
    let program = parse(source);
    engine.set_import_directory(directory);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "p"), 2, "the import stops at its limit");
    assert!(arities(&engine).contains(&(String::from("g"), 2)));
    assert_eq!(count(&mut engine, "g"), 2, "two columns, two statements");

    let out = directory.join("out");
    engine.export(&out).expect("the export writes");
    let written = std::fs::read_to_string(out.join("limited.csv")).expect("written");
    assert_eq!(written.lines().count(), 1, "the export stops at its limit");

    let bad = r#"@import g from ntriples { resource = "graph.nt", format = (int, term, term) } ."#;
    let (program, _) = Program::parse(bad);
    let mut engine = Engine::new();
    engine.add_resource("graph.nt", "".as_bytes().to_vec());
    let refusal = engine.load(program).expect_err("terms take term or skip");
    assert_eq!(refusal.code(), codes::TERM_COLUMN_FORMAT, "{refusal:?}");
}

/// An import stored without the columns no rule reads is listed at its
/// full width, and the methods returning its facts return an error for it,
/// naming what is left out.
#[test]
fn reading_an_import_stored_partially_names_what_is_left_out() {
    let data = [("p.csv", "1,x\n2,y\n")];
    let mut engine = load_resources(
        "@import p from csv { resource = \"p.csv\" } .\nq(?a) :- p(?a, _) .\n@output q .",
        &data,
    );
    assert!(arities(&engine).contains(&(String::from("p"), 2)));
    assert_eq!(engine.predicate("p").map(|info| info.arity), Some(2));
    let refusal = engine.rows("p").expect_err("p is stored partially");
    assert_eq!(refusal.code(), codes::NOT_STORED_WHOLE);
    assert_eq!(
        refusal.message(),
        "`p` is not stored whole: column 2 is not stored, since no rule reads it"
    );
    assert_eq!(
        refusal.hints(),
        ["declare it `@output`, or call `retain_imports()` before loading"]
    );
    assert_eq!(
        engine.count("p").map_err(|refusal| refusal.code()),
        Err(codes::NOT_STORED_WHOLE)
    );
    assert_eq!(engine.column_types("p"), None);
    let fact = Fact::parse("p(1, x) .").expect("the fact parses");
    assert_eq!(
        engine
            .trace(&[fact])
            .map(|_| ())
            .map_err(|refusal| refusal.code()),
        Err(codes::NOT_STORED_WHOLE)
    );

    // With a filter pushed into the import, the rows it rejects are left out too.
    let mut engine = load_resources(
        "@import p from csv { resource = \"p.csv\" } .\nq(?a) :- p(?a, ?b), ?b = \"x\" .\n@output q .",
        &data,
    );
    let refusal = engine.rows("p").expect_err("p is stored partially");
    assert_eq!(
        refusal.message(),
        "`p` is not stored whole: only the rows the rules reading it can use are stored"
    );

    // Declared `@output`, the import is stored whole.
    let mut engine = load_resources(
        "@import p from csv { resource = \"p.csv\" } .\nq(?a) :- p(?a, _) .\n@output p .",
        &data,
    );
    assert_eq!(count(&mut engine, "p"), 2);
}

/// A declaration chooses no format for an import.
/// Without formats the import reads terms, which a narrower declaration refuses.
/// With the formats stated, the declaration is proved,
/// and a cell that its format does not read makes its row malformed.
#[test]
fn a_declaration_chooses_no_format_for_its_import() {
    let source = |format: &str| {
        format!(
            "@declare p(n: int, b: bool) .
             @import p from csv {{ resource = \"data.csv\", quoting = false{format} }} ."
        )
    };

    let (program, _) = Program::parse(&source(""));
    let mut engine = Engine::new();
    engine.add_resource("data.csv", b"1,true\n".to_vec());
    let refusal = engine
        .load(program)
        .expect_err("terms are not all integers");
    assert_eq!(
        refusal.code(),
        codes::IMPORT_OUTSIDE_DECLARATION,
        "{refusal:?}"
    );

    // Line 2 has no int in its first column, line 3 no bool in its second,
    // and line 4 has the wrong width.
    let mut engine = load_resources(
        &source(", format = (int, bool)"),
        &[("data.csv", "1,true\nx,true\n2,7\n3\n")],
    );
    assert_eq!(
        engine.rows("p").expect("registered"),
        vec![vec![DataValue::integer(1), DataValue::boolean(true)]]
    );
    assert_eq!(reported_count(&engine, codes::MALFORMED_ROWS), Some(3));

    // The formats give the stored column types.
    let types: Vec<String> = engine
        .column_types("p")
        .expect("registered")
        .iter()
        .map(|datatype| TypeSyntax::of(datatype).to_string())
        .collect();
    assert_eq!(types, ["int", "bool"]);
}

/// The formats of an import apply to all of its columns, the ones no rule reads included:
/// a row whose cell its format rejects is malformed whether or not a rule reads the column,
/// so pruning a column never changes which rows load.
#[test]
fn formats_check_pruned_import_columns() {
    for rule in ["q(?x) :- p(?x, ?y) .", "q(?y) :- p(?x, ?y) ."] {
        // The second column is read as `bool`, which neither cell is.
        let mut engine = load_resources(
            &format!(
                "@declare p(x: int, y: bool) .
                 @import p from csv {{ resource = \"data.csv\", format = (int, bool) }} .
                 {rule}
                 @output q ."
            ),
            &[("data.csv", "1,x\n2,y\n")],
        );
        assert!(engine.rows("q").expect("registered").is_empty(), "{rule}");
        assert_eq!(
            reported_count(&engine, codes::MALFORMED_ROWS),
            Some(2),
            "{rule}"
        );
    }
}

/// With both a `format` and a declaration, the format reads and the declaration checks.
/// `term` reads values of every type, so a narrower declaration refuses it,
/// as it refuses a typed format that reads values outside it.
#[test]
fn a_format_must_lie_within_the_declaration() {
    for (declared, format) in [("string", "term"), ("int", "decimal")] {
        let (program, _) = Program::parse(&format!(
            "@declare n(x: {declared}) .
             @import n from csv {{ resource = \"n.csv\", format = ({format}) }} ."
        ));
        let mut engine = Engine::new();
        engine.add_resource("n.csv", b"1\n".to_vec());
        let refusal = engine
            .load(program)
            .expect_err("the format reads values outside the declaration");
        assert_eq!(
            refusal.code(),
            codes::IMPORT_OUTSIDE_DECLARATION,
            "{format} into {declared}: {refusal:?}"
        );
    }

    // The `string` format reads `42` as a string, which the declaration admits.
    let mut engine = load_resources(
        "@declare s(x: string) .
         @import s from csv { resource = \"s.csv\", format = (string) } .",
        &[("s.csv", "42\nalice\n")],
    );
    let mut strings = engine.rows("s").expect("registered");
    strings.sort();
    assert_eq!(
        strings,
        vec![
            vec![DataValue::string("42")],
            vec![DataValue::string("alice")]
        ]
    );
}

/// A row with a missing value in a column that does not admit one is dropped,
/// with a warning of its own that says how to keep such rows.
/// With the marker the row is kept, and the column's type gains `null`.
#[test]
fn rows_with_a_missing_value_are_reported_apart() {
    let source = |format: &str| {
        format!("@import p from csv {{ resource = \"data.csv\", format = (int, {format}) }} .")
    };
    let data = [("data.csv", "1,2\n3,\nx,4\n")];

    let mut engine = load_resources(&source("int"), &data);
    assert_eq!(engine.rows("p").expect("registered").len(), 1);
    assert_eq!(reported_count(&engine, codes::MALFORMED_ROWS), Some(1));
    assert_eq!(reported_count(&engine, codes::MISSING_VALUE_ROWS), Some(1));
    let warning = engine
        .reports()
        .iter()
        .find(|report| report.code() == codes::MISSING_VALUE_ROWS)
        .expect("the missing value is reported");
    assert_eq!(warning.notes(), ["the first one is on line 2"]);
    assert!(warning.hints().iter().any(|hint| hint.contains("int?")));

    let mut engine = load_resources(&source("int?"), &data);
    assert_eq!(engine.rows("p").expect("registered").len(), 2);
    assert_eq!(reported_count(&engine, codes::MISSING_VALUE_ROWS), None);
    let types: Vec<String> = engine
        .column_types("p")
        .expect("registered")
        .iter()
        .map(|datatype| TypeSyntax::of(datatype).to_string())
        .collect();
    assert_eq!(types, ["int", "null | int"]);
}

/// The marker belongs to the formats that read cells:
/// a column that is not read has no values to miss,
/// and a source of terms, such as RDF, has no missing values.
#[test]
fn the_marker_is_refused_where_no_value_can_be_missing() {
    let problems = check("@import p from csv { resource = \"p.csv\", format = (int, skip?) } .");
    assert_eq!(
        problems[0].code(),
        codes::UNKNOWN_OPTION_VALUE,
        "{problems:?}"
    );

    let problems =
        check("@import g from ntriples { resource = \"g.nt\", format = (term?, term, term) } .");
    assert_eq!(
        problems[0].code(),
        codes::TERM_COLUMN_FORMAT,
        "{problems:?}"
    );
    assert_eq!(problems[0].field("format"), Some("term?"));
}

/// A format that reads values its declared type does not contain is refused
/// when the program is checked, before any file is opened.
#[test]
fn a_format_wider_than_the_declaration_is_an_error_at_check() {
    for (declared, format) in [
        ("string", "int"),
        ("iri", "double"),
        ("int", "number"),
        ("int | string", "term"),
    ] {
        let problems = check(&format!(
            "@declare p(x: {declared}) .
             @import p from csv {{ resource = \"nowhere.csv\", format = ({format}) }} ."
        ));
        let [problem] = &problems[..] else {
            panic!("expected the one refusal for {format} into {declared}: {problems:?}");
        };
        assert_eq!(
            problem.code(),
            codes::IMPORT_OUTSIDE_DECLARATION,
            "{problem:?}"
        );
    }
    // A column no rule reads is checked too, so what is refused does not depend on the rules.
    let problems = check(
        "@declare t(a: string, b: string) .
         @import t from csv { resource = \"nowhere.csv\", format = (string, int) } .
         q(?x) :- t(?x, _) .",
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.code() == codes::IMPORT_OUTSIDE_DECLARATION),
        "{problems:?}"
    );
    // A name outside the vocabulary says what the vocabulary is.
    let problems = check("@import p from csv { resource = \"nowhere.csv\", format = (integer) } .");
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert_eq!(
        problems[0].code(),
        codes::UNKNOWN_OPTION_VALUE,
        "{:?}",
        problems[0]
    );
    assert_eq!(problems[0].field("value"), Some("integer"));
}

/// RDF statements come as the resource writes them: a subject is an IRI or a blank node,
/// a predicate an IRI, and an object any term, so a declaration must allow that.
#[test]
fn declared_rdf_imports_take_the_shape_of_statements() {
    const GRAPH: &str =
        "<http://x/a> <http://x/p> \"5\"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://x/b> <http://x/p> \"text\" .
_:blank <http://x/p> \"6\"^^<http://www.w3.org/2001/XMLSchema#integer> .
";
    // A declaration of the statements' shape loads every one.
    let mut engine = load_resources(
        "@declare g(s: iri | null, p: iri, o: any) .
         @import g from ntriples { resource = \"graph.nt\" } .",
        &[("graph.nt", GRAPH)],
    );
    assert_eq!(count(&mut engine, "g"), 3);
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());
    // A narrower one is refused, before the graph is read.
    for declaration in [
        "@declare g(s: iri, p: iri, o: int) .",
        "@declare g(s: iri | null, o: int) .",
    ] {
        let format = if declaration.contains("p: iri") {
            ""
        } else {
            ", format = (term, skip, term)"
        };
        let problems = check(&format!(
            "{declaration} @import g from ntriples {{ resource = \"graph.nt\"{format} }} ."
        ));
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert_eq!(
            problems[0].code(),
            codes::IMPORT_OUTSIDE_DECLARATION,
            "{:?}",
            problems[0]
        );
    }
    // Format only: every statement loads.
    let mut engine = load_resources(
        "@import g from ntriples { resource = \"graph.nt\", format = (term, skip, term) } .",
        &[("graph.nt", GRAPH)],
    );
    assert_eq!(count(&mut engine, "g"), 3);
    // `string` is a format for reading cells, and RDF columns contain terms.
    let (program, _) = Program::parse(
        "@import g from ntriples { resource = \"graph.nt\", format = (string, term, term) } .",
    );
    let mut engine = Engine::new();
    engine.add_resource("graph.nt", GRAPH.as_bytes().to_vec());
    let refusal = engine.load(program).expect_err("terms take term or skip");
    assert_eq!(refusal.code(), codes::TERM_COLUMN_FORMAT, "{refusal:?}");
    assert_eq!(refusal.field("format"), Some("string"));
}

/// A JSON import delivers any value, so only `any` columns can be declared on it.
#[test]
fn declared_json_imports_hold_any_value() {
    const DOCUMENT: &str = r#"{"name": "alice", "age": 42, "tags": ["x", 1]}"#;
    let problems = check(
        "@declare j(node: int, key: iri, value: string) .
         @import j from json { resource = \"data.json\" } .",
    );
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert_eq!(
        problems[0].code(),
        codes::IMPORT_OUTSIDE_DECLARATION,
        "{:?}",
        problems[0]
    );
    let all = load_resources(
        "@import j from json { resource = \"data.json\" } .",
        &[("data.json", DOCUMENT)],
    );
    let declared = load_resources(
        "@declare j(node: any, key: any, value: any) .
         @import j from json { resource = \"data.json\" } .",
        &[("data.json", DOCUMENT)],
    );
    assert_eq!(count(&mut { all }, "j"), count(&mut { declared }, "j"));
}

/// The conditions of an import's `where` clause keep the rows for which they hold,
/// and narrow the import's types, so a declaration can be proved against them
/// even for a format that reads any term.
#[test]
fn import_conditions_keep_rows_and_narrow_types() {
    const GRAPH: &str =
        "<http://x/a> <http://x/p> \"5\"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://x/b> <http://x/p> \"text\" .
_:blank <http://x/p> \"6\"^^<http://www.w3.org/2001/XMLSchema#integer> .
";
    let mut engine = load_resources(
        "@declare age(who: iri, years: int) .
         @import age(?who, ?p, ?years) from ntriples {
             resource = \"graph.nt\", format = (term, skip, term)
         } where ISIRI(?who), ISINTEGER(?years) .",
        &[("graph.nt", GRAPH)],
    );
    assert_eq!(
        engine.rows("age").expect("registered"),
        vec![vec![DataValue::iri("http://x/a"), DataValue::integer(5)]]
    );
    // The rows that fail a condition are left out silently.
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());
    let types: Vec<String> = engine
        .column_types("age")
        .expect("registered")
        .iter()
        .map(|datatype| TypeSyntax::of(datatype).to_string())
        .collect();
    assert_eq!(types, ["iri", "int"]);

    // Without the conditions, the declaration is refused.
    let problems = check(
        "@declare age(who: iri, years: int) .
         @import age from ntriples { resource = \"graph.nt\", format = (term, skip, term) } .",
    );
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert_eq!(problems[0].code(), codes::IMPORT_OUTSIDE_DECLARATION);
    // With them, a check proves it without reading anything.
    let problems = check(
        "@declare age(who: iri, years: int) .
         @import age(?who, ?p, ?years) from ntriples {
             resource = \"nowhere.nt\", format = (term, skip, term)
         } where ISIRI(?who), ISINTEGER(?years) .",
    );
    assert!(problems.is_empty(), "{problems:?}");
}

/// The columns that an import's conditions test are kept like the columns that rules read,
/// and the others are left out as usual.
#[test]
fn import_conditions_keep_the_columns_they_test() {
    let mut engine = load_resources(
        "@import p(?a, ?b, ?c) from csv { resource = \"p.csv\" } where ?b > 1 .
         q(?a) :- p(?a, _, _) .
         @output q .",
        &[(
            "p.csv",
            "x,2,u
y,1,v
z,3,w
",
        )],
    );
    assert_eq!(stored(&engine, "p"), (2, 2));
    let mut derived = engine.rows("q").expect("derived");
    derived.sort();
    assert_eq!(
        derived,
        vec![vec![DataValue::iri("x")], vec![DataValue::iri("z")]]
    );
}

/// A `where` clause is checked against the import's columns.
#[test]
fn import_conditions_read_the_columns_the_import_names() {
    for (program, code) in [
        (
            "@import p(?a, ?b) from csv { resource = \"p.csv\" } where ?c > 1 .",
            codes::BINDING_VARIABLE_UNKNOWN,
        ),
        (
            "@import p(?a, ?b) from csv { resource = \"p.csv\", format = (term, skip) } \
             where ?b > 1 .",
            codes::CONDITION_ON_SKIPPED_COLUMN,
        ),
        (
            "@import p(?a) from csv { resource = \"p.csv\", format = (term, term) } \
             where ?a > 1 .",
            codes::IMPORT_COLUMN_COUNT,
        ),
        (
            "@import p(?a) from csv { resource = \"p.csv\" } where q(?a) .",
            codes::BINDINGS_ON_FORMAT,
        ),
        // The column variables are checked without a `where` clause too.
        (
            "@import p(?a) from csv { resource = \"p.csv\", format = (term, term) } .",
            codes::IMPORT_COLUMN_COUNT,
        ),
        (
            "@import p(?n, ?n) from csv { resource = \"p.csv\", format = (term, term) } \
             where ?n = alice .",
            codes::DUPLICATE_COLUMN_VARIABLE,
        ),
    ] {
        let problems = check(program);
        assert!(
            problems.iter().any(|problem| problem.code() == code),
            "{program}: {problems:?}"
        );
    }
}

/// An option is refused where it would be ignored, and a known option with a value of the
/// wrong kind is named as such.
#[test]
fn directive_options_are_refused_rather_than_ignored() {
    for (program, code) in [
        (
            "@import p from csv { resource = \"p.csv\", delimiter = \";\" } .",
            codes::UNKNOWN_OPTION,
        ),
        (
            "@export p to tsv { resource = \"p.tsv\", delimiter = \";\" } . p(1) .",
            codes::UNKNOWN_OPTION,
        ),
        (
            "@import p from csv { resource = \"p.csv\", ignore_headers = \"true\" } .",
            codes::OPTION_VALUE_TYPE,
        ),
        (
            "@import p from csv { resource = \"p.csv\", quoting = 1 } .",
            codes::OPTION_VALUE_TYPE,
        ),
        (
            "@import p from csv { resource = \"p.csv\", compression = gzip } .",
            codes::OPTION_VALUE_TYPE,
        ),
        // For a format that accepts a base, a base of the wrong type is named as such.
        (
            "@import p from turtle { resource = \"p.ttl\", base = 3 } .",
            codes::OPTION_VALUE_TYPE,
        ),
        // The generic RDF format has no resource name to pick a serialization by.
        (
            "@export p to rdf { } . p(<a>, <b>, <c>) .",
            codes::OPTION_MISSING,
        ),
        // The column formats of an export are counted before any row is written,
        // also for a predicate that holds none.
        (
            "@export p to csv { format = (int) } . p(?x, ?y) :- q(?x, ?y) .",
            codes::EXPORT_FORMAT_ARITY,
        ),
    ] {
        let problems = check(program);
        assert!(
            problems.iter().any(|problem| problem.code() == code),
            "{program}: {problems:?}"
        );
    }
}

/// A header name written as a bare name is sent as written, not resolved against the base,
/// and a header that HTTP cannot send is refused before any request is sent.
#[test]
fn request_headers_are_sent_as_written_or_refused() {
    let mut engine = Engine::new();
    let progress = engine
        .start_load(parse(
            "@base <http://example.org/> .
@import p from csv {
    resource = \"http://example.org/p.csv\",
    http_headers = { Accept = \"text/csv\" }
} .
",
        ))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let Progress::Suspended(pending) = progress else {
        panic!("the import is suspended for its resource");
    };
    assert_eq!(engine.pending(), pending);
    assert_eq!(pending.len(), 1, "{pending:?}");
    assert!(
        pending[0]
            .request
            .headers
            .contains(&(String::from("Accept"), String::from("text/csv"))),
        "{pending:?}"
    );

    for (headers, code) in [
        ("{ \"Bad Name\" = \"x\" }", codes::HEADER_NAME),
        ("{ 3 = \"x\" }", codes::HEADER_NAME),
        ("{ Accept = \"caf\u{e9}\" }", codes::HEADER_VALUE),
        ("{ Accept = 1 }", codes::HEADER_VALUE),
    ] {
        let program = format!(
            "@import p from csv {{ resource = \"http://example.org/p.csv\", \
             http_headers = {headers} }} ."
        );
        let problems = check(&program);
        assert!(
            problems.iter().any(|problem| problem.code() == code),
            "{program}: {problems:?}"
        );
    }
}

/// An import reads a resource registered with [add_resource](Engine::add_resource) instead
/// of the file of that name.
#[test]
fn registered_resources_shadow_the_filesystem() {
    // No `data.csv` exists on disk.
    let program = parse("@import p from csv { resource = \"data.csv\" } . q(?x) :- p(?x, ?y) .");
    let mut engine = Engine::new();
    engine.add_resource("data.csv", "a,1\nb,2\nc,3\n".as_bytes().to_vec());
    engine.load(program).expect("the import reads the bytes");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "q"), 3);
}

/// A diagnostic names only the rule's own variables,
/// since the columns that the normalization adds have no name a program could write.
#[test]
fn invented_columns_stay_out_of_diagnostics() {
    let program = parse("@declare p(x: string) . p(\"a\") . r(1) . q(?y) :- r(?y), p(?y + 1) .");
    let mut engine = Engine::new();
    engine.load(program).expect("the program loads");
    let reports: Vec<String> = engine
        .reports()
        .iter()
        .map(|report| format!("{report:?}"))
        .collect();
    assert!(
        reports.iter().any(|report| report.contains("never")),
        "{reports:?}"
    );
    assert!(
        reports.iter().all(|report| !report.contains('\u{0}')),
        "{reports:?}"
    );
}

/// Rows added through [import_rows](Engine::import_rows) to an imported predicate stored
/// without some columns are given with every written column.
///
/// The import's projection applies to them as it did to the imported rows,
/// and a row with only the stored columns is an error, since its values would
/// land in the wrong columns.
#[test]
fn imported_rows_into_a_pruned_predicate_project_like_the_import() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_engine_prune_rows.csv");
    std::fs::write(&path, "1,x,10\n").expect("the temp file writes");
    let source =
        "@import p from csv{resource=\"nemo_engine_prune_rows.csv\", format=(int,string,int)} .
q(?a) :- p(?a, _, ?b), ?b > 5 .
@output q .
";
    let program = parse(source);
    let mut engine = Engine::new();
    engine.set_import_directory(directory);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    engine
        .import_rows(
            "p",
            [vec![
                DataValue::integer(2),
                DataValue::string("y"),
                DataValue::integer(20),
            ]],
        )
        .expect("a row as written projects");
    let refusal = engine
        .import_rows("p", [vec![DataValue::integer(3), DataValue::integer(30)]])
        .expect_err("a row at the stored width is refused");
    assert_eq!(refusal.code(), codes::PRUNED_PREDICATE_ROWS, "{refusal:?}");
    engine.reason().expect("reasoning continues");
    assert_eq!(count(&mut engine, "q"), 2);
}

/// The `columns` option names the header's columns, one name per format,
/// and the header that it reads is not skipped as well.
#[test]
fn the_columns_option_is_checked_against_the_other_options() {
    let import = |options: &str| {
        let problems = check(&format!(
            "@import p from csv {{ resource = \"p.csv\", {options} }} ."
        ));

        problems.first().map(Diagnostic::code)
    };

    assert_eq!(import("columns = (\"a\", \"b\")"), None);
    assert_eq!(
        import("columns = (\"a\", \"b\"), ignore_headers = true"),
        Some(codes::HEADER_OPTIONS)
    );
    assert_eq!(
        import("columns = (\"a\", \"b\"), format = (int)"),
        Some(codes::COLUMN_NAME_COUNT)
    );
    for names in ["(\"a\", \"a\")", "(\"a\", 1)", "true"] {
        assert_eq!(
            import(&format!("columns = {names}")),
            Some(codes::OPTION_VALUE_TYPE),
            "{names}"
        );
    }

    // An export without formats has one name per column of its predicate.
    let problems = check(
        "p(1, 2) .
         @export p to csv { resource = \"p.csv\", columns = (\"a\") } .",
    );
    assert_eq!(problems[0].code(), codes::COLUMN_NAME_COUNT, "{problems:?}");
}

/// An import whose header lacks a named column fails, and names the column and the resource.
#[test]
fn a_header_without_a_named_column_fails_the_import() {
    let (program, _) = Program::parse(
        "@import p from csv { resource = \"p.csv\", columns = (\"name\", \"age\") } .",
    );
    let mut engine = Engine::new();
    engine.add_resource("p.csv", b"name,years\nalice,30\n".to_vec());
    let failure = engine.load(program).expect_err("the header has no `age`");

    assert_eq!(
        failure.code(),
        nemo_io::codes::HEADER_LACKS_COLUMN,
        "{failure:?}"
    );
    assert_eq!(failure.field("name"), Some("age"));
    assert_eq!(failure.field("resource"), Some("p.csv"));
}

/// The `text` format only writes, so an import refuses it and points at `string`.
#[test]
fn the_text_format_is_refused_on_an_import() {
    let problems = check("@import p from csv { resource = \"p.csv\", format = (int, text) } .");
    assert_eq!(problems[0].code(), codes::WRITE_ONLY_FORMAT, "{problems:?}");
    assert!(
        problems[0]
            .hints()
            .iter()
            .any(|hint| hint.contains("`string`"))
    );

    let problems = check(
        "p(1, \"a\") .
         @export p to csv { resource = \"p.csv\", format = (int, text) } .",
    );
    assert!(problems.is_empty(), "{problems:?}");
}
