//! Using a module: one text of rules, instantiated twice over different data.
//!
//! Usage: `cargo run -p nemo --example modules`

use nemo::engine::Engine;

/// A module is an ordinary program text.
/// Its `@input` names what the using program binds, and its `@output` what it offers.
const CLOSURE: &str = "
    @input edge .
    @output reach .
    reach(?x, ?y) :- edge(?x, ?y) .
    reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
";

/// Each `@use` copies the module under an alias and binds its input to a predicate.
const PROGRAM: &str = "
    road(dresden, leipzig) . road(leipzig, halle) .
    rail(dresden, prague) . rail(prague, vienna) .

    @use graph::closure { edge = road } as by_road .
    @use graph::closure { edge = rail } as by_rail .

    reachable(?from, ?to) :- by_road::reach(?from, ?to) .
    reachable(?from, ?to) :- by_rail::reach(?from, ?to) .
";

fn main() {
    let mut engine = Engine::new();

    // A `@use graph::closure` reads `graph/closure.rls` beside the program.
    // A text registered under the module's name is read instead of that file.
    engine
        .add_module("graph::closure", CLOSURE)
        .expect("`graph::closure` is a module name");

    // `load_source` links the program, which instantiates the modules, before it loads it.
    engine
        .load_source("routes.rls", PROGRAM)
        .expect("the program links and loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    // The two instances are independent copies, each with predicates of its own.
    for info in engine.predicates() {
        println!("{}/{} is {}", info.name, info.arity, info.role.name());
    }
    for instance in ["by_road::reach", "by_rail::reach"] {
        let count = engine.count(instance).expect("the instance offers `reach`");
        println!("{instance} holds {count} rows");
    }

    let mut routes: Vec<String> = engine
        .rows("reachable")
        .expect("the program derives `reachable`")
        .iter()
        .map(|row| format!("{} to {}", row[0], row[1]))
        .collect();
    routes.sort();
    println!("{}", routes.join(", "));
}
