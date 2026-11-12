//! What the import passes leave of an import's table: pruned columns and pushed filters.

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
        r#"[[1, "kind", "a"], [2, "kind", "b"], [3, "other", "c"], [4, "kind", "a"]]"#
            .as_bytes()
            .to_vec(),
    );
    let source = r#"@import g from ntriples { resource = "graph.nt" } .
@import j from json { resource = "doc.json" } .
subject(?s) :- g(?s, <http://x/p>, ?o) .
kind(?t) :- j(?id, "kind", ?t) .
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

    // No rule reads the first element of the records, and the second is a constant,
    // so the table stores two columns of the `kind` records, whose three rows become two.
    assert!(arities(&engine).contains(&(String::from("j"), 3)));
    assert_eq!(stored(&engine, "j"), (2, 2));
    assert_eq!(count(&mut engine, "kind"), 2);
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
