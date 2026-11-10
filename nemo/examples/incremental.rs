//! Changing a reasoned engine: new rows, new statements, and a replaced program.
//!
//! Usage: `cargo run -p nemo --example incremental`

use nemo::{engine::Engine, values::DataValue};

/// A task is ready unless it is blocked, and it is blocked while a task it needs is not done.
const PROGRAM: &str = "
    task(build) . task(test) . task(deploy) .
    needs(test, build) . needs(deploy, test) .
    blocked(?task) :- needs(?task, ?other), ~done(?other) .
    ready(?task) :- task(?task), ~blocked(?task) .
";

/// One more task, and a rule over what the loaded rules derive.
const EXTENSION: &str = "
    task(release) . needs(release, deploy) .
    awaited(?task) :- ready(?task), needs(?next, ?task) .
";

/// The program again, with a fact added and without the extension.
const REPLACEMENT: &str = "
    task(build) . task(test) . task(deploy) .
    needs(test, build) . needs(deploy, test) .
    blocked(deploy) .
    blocked(?task) :- needs(?task, ?other), ~done(?other) .
    ready(?task) :- task(?task), ~blocked(?task) .
";

/// Returns the first column of a predicate's rows in term syntax, sorted.
fn names(engine: &mut Engine, predicate: &str) -> String {
    let rows = engine
        .rows(predicate)
        .expect("the program has the predicate");
    let mut names: Vec<String> = rows.iter().map(|row| row[0].to_string()).collect();
    names.sort();
    names.join(", ")
}

fn main() {
    let mut engine = Engine::new();
    engine
        .load_source("tasks.rls", PROGRAM)
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    println!("ready at first: {}", names(&mut engine, "ready"));

    // Rows given through `import_rows` are new facts. `done` is read under a negation, so the next run
    // drops the conclusions that depended on it and derives them again:
    // `blocked(test)` goes, and `ready(test)` comes.
    engine
        .import_rows("done", [vec![DataValue::iri("build")]])
        .expect("the program reads `done`");
    println!(
        "at the fixpoint after the import: {}",
        engine.is_at_fixpoint()
    );
    engine.reason().expect("reasoning continues");
    println!("ready once build is done: {}", names(&mut engine, "ready"));

    // An extension adds statements. Everything stored is kept,
    // and the next run continues from it.
    engine
        .extend_source("more.rls", EXTENSION)
        .expect("the extension compiles");
    engine.reason().expect("reasoning continues");
    println!("awaited: {}", names(&mut engine, "awaited"));

    // A reload replaces the program, which is how statements are retracted.
    // The derived rows are dropped, and the predicates whose statements changed are rebuilt.
    // `done` has no statements, so the rows given to it stay.
    engine
        .reload_source("tasks.rls", REPLACEMENT)
        .expect("the replacement loads");
    engine.reason().expect("reasoning starts over");
    println!("ready after the reload: {}", names(&mut engine, "ready"));
    println!(
        "awaited is gone: {}, done is kept: {} row",
        engine.predicate("awaited").is_none(),
        engine.count("done").expect("the program reads `done`")
    );
    println!(
        "{} loaded and {} derived facts, and {} warnings along the way",
        engine.loaded_facts(),
        engine.derived_facts(),
        engine.reports().len()
    );
}
