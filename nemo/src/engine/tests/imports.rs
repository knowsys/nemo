//! Imports: pruned columns, pushed filters, column formats, and the checks
//! against declarations.

use super::*;

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

    // The string column is anonymous everywhere: the stored table
    // is its projection, and the row differing only there
    // collapses.
    let program = parse(source);
    let mut engine = Engine::new();
    engine.set_import_directory(directory);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(stored(&engine, "p"), (2, 2));
    assert_eq!(count(&mut engine, "q"), 2);

    // Retained, the same program keeps the written width and every
    // row.
    let (program, _) = Program::parse(source, FileId::new(0));
    let mut engine = Engine::new();
    engine.retain_imports();
    engine.set_import_directory(directory);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "p"), 3);
}

/// A universal variable the rule mentions nowhere else reads nothing, like `_`,
/// so the column prunes.
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

/// If every reader constrains the predicate,
/// by a constant in the atom or comparisons on a variable used nowhere else,
/// the loader admits only rows some reader can use.
/// One unconstrained reader keeps every row.
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

    // The rule draws once per row; a copy of the comparison at the loader would draw again.
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

/// Rows arriving by another door after the load still go through the rule's own filter,
/// since the pushdown reduces the load, never the semantics.
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

/// An extension reading rows the loader excluded is refused: the
/// stored relation cannot supply them. With imports retained, the
/// same extension is fine.
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

/// Every import format delivers only the columns and rows the rules
/// read: RDF and JSON through their registered bytes here.
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
    // The object is unread and the predicate column a constant: the
    // graph stores two columns and only the `p` statements, two rows
    // that collapse into one.
    assert!(arities(&engine).contains(&(String::from("g"), 3)));
    assert_eq!(stored(&engine, "g"), (1, 2));
    assert_eq!(count(&mut engine, "subject"), 1);
    // The document's node ids are unread and the key is a constant:
    // two columns, the `type` rows only, one per node, five nodes of
    // four kinds.
    assert!(arities(&engine).contains(&(String::from("j"), 3)));
    assert_eq!(stored(&engine, "j"), (4, 2), "object, number, array, bool");
    assert_eq!(count(&mut engine, "kind"), 4);
}

/// `limit` caps what an import reads and what an export writes; on
/// RDF and SPARQL imports, `format` keeps or skips columns of terms.
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
    let (program, _) = Program::parse(bad, FileId::new(0));
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

    // Rows only a filter admits are left out too.
    let mut engine = load_resources(
        "@import p from csv { resource = \"p.csv\" } .\nq(?a) :- p(?a, ?b), ?b = \"x\" .\n@output q .",
        &data,
    );
    let refusal = engine.rows("p").expect_err("p is stored partially");
    assert_eq!(
        refusal.message(),
        "`p` is not stored whole: only the rows the rules reading it can use are stored"
    );

    // Declared an output, the import is stored whole.
    let mut engine = load_resources(
        "@import p from csv { resource = \"p.csv\" } .\nq(?a) :- p(?a, _) .\n@output p .",
        &data,
    );
    assert_eq!(count(&mut engine, "p"), 2);
}

/// The format is the type of what an import reads: a declaration alone derives the formats,
/// each column reading in the format named like its declared type,
/// and a cell that does not read is malformed.
#[test]
fn a_declaration_reads_its_import_in_its_formats() {
    // Line 2 fails the int reading, line 3 the bool reading, and line 4
    // the width.
    let mut engine = load_resources(
        "@declare p(n: int, b: bool) .
         @import p from csv { resource = \"data.csv\", quoting = false } .",
        &[("data.csv", "1,true\nx,true\n2,7\n3\n")],
    );
    assert_eq!(
        engine.rows("p").expect("registered"),
        vec![vec![DataValue::integer(1), DataValue::boolean(true)]]
    );
    assert_eq!(reported_count(&engine, codes::MALFORMED_ROWS), Some(3));
    // The formats give the store's types.
    let types: Vec<String> = engine
        .column_types("p")
        .expect("registered")
        .iter()
        .map(|datatype| TypeSyntax::of(datatype).to_string())
        .collect();
    assert_eq!(types, ["int", "bool"]);
}

/// A declaration refers to all of a predicate's columns, the ones no rule reads included:
/// a row whose cell the declared type rejects is malformed whether or not a rule reads it,
/// so pruning a column never changes which rows load.
#[test]
fn declarations_check_pruned_import_columns() {
    for rule in ["q(?x) :- p(?x, ?y) .", "q(?y) :- p(?x, ?y) ."] {
        // The second column reads as `bool`, which neither cell is.
        let mut engine = load_resources(
            &format!(
                "@declare p(x: int, y: bool) .
                 @import p from csv {{ resource = \"data.csv\" }} .
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

/// With both a `format` and a declaration, `term` defers to the
/// declaration on the columns it types, while a typed format keeps its
/// own reading, which must lie within the declaration.
#[test]
fn term_defers_to_the_declaration_and_typed_formats_must_fit_it() {
    let mut engine = load_resources(
        "@declare s(x: string) .
         @import s from csv { resource = \"s.csv\", format = (term) } .",
        &[("s.csv", "42\nalice\n")],
    );
    // `42` reads verbatim rather than as an integer.
    let mut strings = engine.rows("s").expect("registered");
    strings.sort();
    assert_eq!(
        strings,
        vec![
            vec![DataValue::string("42")],
            vec![DataValue::string("alice")]
        ]
    );
    // A decimal reading is wider than `int`, so the import is refused.
    let (program, _) = Program::parse(
        "@declare n(x: int) .
         @import n from csv { resource = \"n.csv\", format = (decimal) } .",
        FileId::new(0),
    );
    let mut engine = Engine::new();
    engine.add_resource("n.csv", b"1\n".to_vec());
    let refusal = engine
        .load(program)
        .expect_err("decimals are not all integers");
    assert_eq!(
        refusal.code(),
        codes::IMPORT_OUTSIDE_DECLARATION,
        "{refusal:?}"
    );
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
    // `string` is a reading of cells, and RDF columns hold terms.
    let (program, _) = Program::parse(
        "@import g from ntriples { resource = \"graph.nt\", format = (string, term, term) } .",
        FileId::new(0),
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

/// The columns an import's conditions test are kept like the columns rules read,
/// and the others are pruned as usual.
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
        // A format that takes a base is told its value is of the wrong type.
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
/// and a header HTTP cannot carry is refused before any request is sent.
#[test]
fn request_headers_are_sent_as_written_or_refused() {
    let mut engine = Engine::new();
    let problem = engine
        .load(parse(
            "@base <http://example.org/> .
@import p from csv {
    resource = \"http://example.org/p.csv\",
    http_headers = { Accept = \"text/csv\" }
} .
",
        ))
        .expect_err("the import waits for its resource");
    assert_eq!(problem.code(), codes::AWAITING_REMOTE, "{problem:?}");
    let pending = engine.pending();
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

#[test]
fn registered_resources_shadow_the_filesystem() {
    // No `data.csv` exists on disk, and the registered bytes serve the
    // import, as for input data that is not in a file.
    let program = parse("@import p from csv { resource = \"data.csv\" } . q(?x) :- p(?x, ?y) .");
    let mut engine = Engine::new();
    engine.add_resource("data.csv", "a,1\nb,2\nc,3\n".as_bytes().to_vec());
    engine.load(program).expect("the import reads the bytes");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "q"), 3);
}

/// A diagnostic names the rule's own variables only: the columns the
/// normalization invents have no name a program could write.
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

/// Rows handed in for a pruned predicate are given as written.
/// The loader's projection applies to them as it did to the import,
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
