//! Building one program three ways: from text, with macros, and with builders.
//!
//! Usage: `cargo run -p nemo --example construct`

use nemo::{
    atom,
    engine::Engine,
    fact,
    model::{Atom, Fact, Program, Rule, Term},
    rule,
};

fn main() {
    // Parsing serves a program that arrives as text.
    // The `quickstart` example loads a text so that the engine can quote it in a diagnostic.
    let (parsed, diagnostics) = Program::parse(
        "parent(alice, bob) . parent(bob, carol) .
         ancestor(?x, ?y) :- parent(?x, ?y) .
         ancestor(?x, ?z) :- parent(?x, ?y), ancestor(?y, ?z) .",
    );
    assert!(diagnostics.is_empty(), "{diagnostics:?}");

    // The macros serve statements whose shape is known when the Rust code is written.
    // They take a subset of the syntax of Nemo.
    let with_macros = Program::builder()
        .fact(fact!(parent(alice, bob)))
        .fact(fact!(parent(bob, carol)))
        .rule(rule!(ancestor(?x, ?y) :- parent(?x, ?y)))
        .rule(rule!(ancestor(?x, ?z) :- parent(?x, ?y), ancestor(?y, ?z)))
        .build();

    // The constructors and builders serve statements that are computed at run time,
    // such as one fact per entry of a list.
    // A `&str` converts to a string term, so a name is created with `Term::constant`.
    let mut builder = Program::builder();
    for (parent, child) in [("alice", "bob"), ("bob", "carol")] {
        let names = [Term::constant(parent), Term::constant(child)];
        builder = builder.fact(Fact::new("parent", names));
    }
    let direct = Rule::builder()
        .head(atom!(ancestor(?x, ?y)))
        .body(atom!(parent(?x, ?y)))
        .build();
    let transitive = Rule::builder()
        .head(Atom::new("ancestor", [Term::var("x"), Term::var("z")]))
        .body(Atom::new("parent", [Term::var("x"), Term::var("y")]))
        .body(Atom::new("ancestor", [Term::var("y"), Term::var("z")]))
        .build();
    let built = builder.rule(direct).rule(transitive).build();

    // The three are the same program, since equality ignores source positions.
    assert_eq!(parsed, with_macros);
    assert_eq!(parsed, built);

    // A program prints as text that parses back to the same program.
    println!("{built}");

    // A built program is loaded as a parsed one is.
    let mut engine = Engine::new();
    engine.load(built).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    println!(
        "ancestor holds {} rows",
        engine
            .count("ancestor")
            .expect("the program derives `ancestor`")
    );
}
