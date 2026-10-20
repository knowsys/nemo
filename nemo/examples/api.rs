//! The Rust API in one tour: build a program three ways, run it, read
//! the results back, and check a program without running it.
//!
//! Usage: `cargo run -p nemo --example api`

use nemo::{
    atom,
    diagnostics::render::{Renderer, SourceTexts},
    engine::Engine,
    fact,
    model::{Fact, FileId, Program, Rule, Term},
    rule,
    values::{DataValue, TypeSyntax},
};

fn main() {
    // ── Construction, three ways ─────────────────────────────────────
    //
    // 1. Parsing, for programs that arrive as text.
    let source = "
        parent(alice, bob) . parent(bob, carol) .
        ancestor(?x, ?y) :- parent(?x, ?y) .
        ancestor(?x, ?z) :- parent(?x, ?y), ancestor(?y, ?z) .
    ";
    let (parsed, diagnostics) = Program::parse(source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{diagnostics:?}");

    // 2. Macros, for rule shapes known at compile time.
    let stated = rule!(ancestor(?x, ?y) :- parent(?x, ?y));
    let transitive = rule!(ancestor(?x, ?z) :- parent(?x, ?y), ancestor(?y, ?z));
    let with_macros = Program::builder()
        .fact(fact!(parent(alice, bob)))
        .fact(fact!(parent(bob, carol)))
        .rule(stated)
        .rule(transitive)
        .build();

    // 3. Builders, for shapes computed at runtime. `&str` converts to
    // a *plain string* term, and names are explicit (`Term::constant`).
    let head = atom!(ancestor(?x, ?y));
    let built = Program::builder()
        .fact(Fact::new(
            "parent",
            vec![Term::constant("alice"), Term::constant("bob")],
        ))
        .fact(Fact::new(
            "parent",
            vec![Term::constant("bob"), Term::constant("carol")],
        ))
        .rule(
            Rule::builder()
                .head(head)
                .body(atom!(parent(?x, ?y)))
                .build(),
        )
        .rule(rule!(ancestor(?x, ?z) :- parent(?x, ?y), ancestor(?y, ?z)))
        .build();

    // The three styles build the same model (spans aside).
    assert_eq!(parsed, with_macros);
    assert_eq!(parsed, built);

    // ── Running ──────────────────────────────────────────────────────
    //
    // Data can also arrive from the host directly: rows imported before
    // the load join the program as facts.
    let mut engine = Engine::new();
    engine
        .import_rows(
            "parent",
            vec![vec![DataValue::iri("carol"), DataValue::iri("dave")]],
        )
        .expect("rows import before the load");
    engine.load(parsed).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    // Values print in canonical term syntax; `rows` materializes a
    // whole predicate at once.
    let rows = engine
        .rows("ancestor")
        .expect("the predicate is registered");
    println!("ancestor holds {} rows:", rows.len());
    for row in &rows {
        let rendered: Vec<String> = row.iter().map(ToString::to_string).collect();
        println!("  ancestor({})", rendered.join(", "));
    }

    // Large results page instead: a cursor holds no borrow, and each
    // read returns one bounded, column-major chunk.
    let mut cursor = engine.cursor("ancestor").expect("the predicate exists");
    let mut read = 0;
    while !cursor.is_done() {
        let chunk = engine
            .read_chunk(&mut cursor, 4)
            .expect("the read succeeds");
        read += chunk.len();
    }
    println!("paged through {read} rows in chunks of at most 4");

    // The type analysis ran at load; inferred column types read back
    // per predicate, displayed in declaration syntax.
    let types = engine
        .column_types("ancestor")
        .expect("the predicate is registered");
    let rendered: Vec<String> = types
        .iter()
        .map(|datatype| TypeSyntax::of(datatype).to_string())
        .collect();
    println!("ancestor columns: ({})", rendered.join(", "));

    // ── Data the host holds ──────────────────────────────────────────
    //
    // An `@import` reads a registered in-memory resource in place of
    // the filesystem, the data path of embedders that have none
    // (browsers) or hold their input already (any host language).
    let (fed, diagnostics) = Program::parse(
        "@import p from csv { resource = \"data.csv\" } . q(?x) :- p(?x, ?y) .",
        FileId::new(0),
    );
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let mut engine = Engine::new();
    engine.add_resource("data.csv", "a,1\nb,2\n".as_bytes().to_vec());
    engine.load(fed).expect("the import reads the bytes");
    engine.reason().expect("reasoning reaches the fixpoint");
    println!(
        "q holds {} rows from the in-memory csv",
        engine.count("q").expect("the predicate is registered")
    );

    // ── Multi-stage reasoning ────────────────────────────────────────
    //
    // A loaded engine keeps going: rows imported after the fixpoint are
    // deltas, an extension adds statements and recompiles, and a reload
    // replaces the program while keeping the loaded data of unchanged
    // predicates. The next `reason()` continues to the new fixpoint,
    // and if a change feeds a negation or aggregation, the affected
    // conclusions drop and are computed again on their own.
    let (staged, _) = Program::parse(
        "parent(alice, bob) . ancestor(?x, ?y) :- parent(?x, ?y) .
         ancestor(?x, ?z) :- parent(?x, ?y), ancestor(?y, ?z) .",
        FileId::new(0),
    );
    let mut engine = Engine::new();
    engine.load(staged).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    engine
        .import_rows(
            "parent",
            vec![vec![DataValue::iri("bob"), DataValue::iri("carol")]],
        )
        .expect("rows join the loaded engine");
    let (extension, _) = Program::parse("forebear(?x) :- ancestor(?x, ?y) .", FileId::new(0));
    engine.extend(extension).expect("the extension compiles");
    engine.reason().expect("reasoning continues");
    println!(
        "after import and extension: {} ancestors, {} forebears",
        engine.count("ancestor").expect("registered"),
        engine.count("forebear").expect("registered"),
    );

    // ── Checking without running, and rendering what it finds ────────
    //
    // `check` validates a program (parse structure, `@declare` and
    // `@type` declarations, the type analysis) but reads no data and derives
    // nothing. It is what an editor runs per keystroke. The shared
    // renderer quotes each diagnostic against its source.
    let source = "@type angle(decimal) . spin(angle(\"north\")) .";
    let (broken, _) = Program::parse(source, FileId::new(0));
    let sources = SourceTexts::detached("broken.rls", source);
    let mut checker = Engine::new();
    for report in checker.check(broken).diagnostics() {
        print!("{}", Renderer::plain(&sources).show(report));
    }
}
