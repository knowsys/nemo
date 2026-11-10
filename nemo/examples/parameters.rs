//! Giving the parameters of a program their values.
//!
//! Usage: `cargo run -p nemo --example parameters`

use nemo::engine::Engine;

/// A program with a parameter without a value, and one with a default.
const PROGRAM: &str = r#"
    @parameter $minimum: int .
    @parameter $unit: string = "points" .
    score(alice, 82) . score(bob, 47) . score(carol, 65) .
    passed(?who, ?points, $unit) :- score(?who, ?points), ?points >= $minimum .
"#;

/// Reasons to the fixpoint, and prints who passed.
fn show_passed(engine: &mut Engine, title: &str) {
    engine.reason().expect("reasoning reaches the fixpoint");
    let mut passed: Vec<String> = engine
        .rows("passed")
        .expect("the program derives `passed`")
        .iter()
        .map(|row| format!("{} with {} {}", row[0], row[1], row[2]))
        .collect();
    passed.sort();
    println!("{title}: {}", passed.join(", "));
}

fn main() {
    // A check reads a parameter without a value as any value of its type.
    let mut engine = Engine::new();
    let report = engine.check_source("scores.rls", PROGRAM);
    println!("the check reports {} problems", report.diagnostics().len());

    // A load needs the value.
    let refused = engine
        .load_source("scores.rls", PROGRAM)
        .expect_err("`$minimum` has no value");
    println!("the load reports: {}", refused[0].message());

    // The engine gives a parameter its value in the programs that it loads afterwards.
    engine.set_parameter("minimum", 50);
    engine
        .load_source("scores.rls", PROGRAM)
        .expect("every parameter has a value");
    show_passed(&mut engine, "at least 50");

    // The loaded program keeps the values it was loaded with, and a reload takes the values
    // set since. A set value also replaces the value that a declaration states.
    engine.set_parameter("minimum", 80);
    engine.set_parameter("unit", "marks");
    engine
        .reload_source("scores.rls", PROGRAM)
        .expect("every parameter has a value");
    show_passed(&mut engine, "at least 80");

    // A value for a parameter that the program does not declare refuses the program,
    // so that a misspelled name is noticed.
    engine.set_parameter("minimun", 10);
    let refused = engine
        .reload_source("scores.rls", PROGRAM)
        .expect_err("the program has no `$minimun`");
    println!("the reload reports: {}", refused[0].message());
    println!("  note: {}", refused[0].notes().join("; "));
}
