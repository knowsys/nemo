//! The shortest path through the engine: a program text in, typed rows out.
//!
//! Usage: `cargo run -p nemo --example quickstart`

use nemo::{
    engine::Engine,
    values::{NumberRef, ValueRef},
};

/// Three facts, and a rule with a condition.
const PROGRAM: &str = "
    age(alice, 34) . age(bob, 17) . age(carol, 52) .
    adult(?person, ?age) :- age(?person, ?age), ?age >= 18 .
";

fn main() {
    let mut engine = Engine::new();

    // `load_source` registers the text under a name, parses and links it, and loads the
    // program. The engine then knows the text, and quotes it when it renders a diagnostic.
    let loaded = engine.load_source("people.rls", PROGRAM);
    if let Err(diagnostics) = loaded {
        for diagnostic in &diagnostics {
            eprint!("{}", engine.render(diagnostic));
        }
        std::process::exit(1);
    }
    engine.reason().expect("reasoning reaches the fixpoint");

    // A row is a list of values, and `as_ref` gives the view to match on:
    // `alice` is a name, which is an IRI, and `34` is an exact number.
    let rows = engine.rows("adult").expect("the program derives `adult`");
    for row in &rows {
        let (ValueRef::Iri(person), ValueRef::Number(NumberRef::Decimal(age))) =
            (row[0].as_ref(), row[1].as_ref())
        else {
            panic!("a row of `adult` is a name and an integer");
        };
        let age = age.to_i64().expect("an age fits in 64 bits");
        println!("{person} is an adult, aged {age}");
    }

    // Every value also prints in term syntax, as a program writes it.
    for row in &rows {
        println!("adult({}, {}) .", row[0], row[1]);
    }
}
