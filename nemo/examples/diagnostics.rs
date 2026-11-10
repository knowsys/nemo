//! Checking a program without running it, and reading what the check reports.
//!
//! Usage: `cargo run -p nemo --example diagnostics`

use nemo::{codes, engine::Engine, model::Identifier, values::TypeSyntax};

/// A program with one error and one warning:
/// the head of `unbound` has a variable that its body does not bind,
/// and `confused` compares a name with an age.
const PROGRAM: &str = r#"
    @declare person(name: string, age: int) .
    person("Ann", 34) . person("Bo", 17) .
    older(?x, ?y) :- person(?x, ?a), person(?y, ?b), ?a > ?b .
    confused(?x) :- person(?x, ?a), ?x > ?a .
    unbound(?x, ?z) :- person(?x, ?a) .
"#;

fn main() {
    // A check reads no data and stores nothing, and it does not stop at the first problem.
    let mut engine = Engine::new();
    let report = engine.check_source("people.rls", PROGRAM);

    // A diagnostic is data: a stable code, a severity, a message, and labels that point into
    // the source. Its location is the name of the source, the line and the column.
    for diagnostic in report.diagnostics() {
        let location = diagnostic
            .location()
            .map_or_else(|| String::from("no location"), ToString::to_string);
        println!(
            "{} {} at {location}: {}",
            diagnostic.severity(),
            diagnostic.code(),
            diagnostic.message()
        );
    }

    // The rendering quotes the source lines, as a terminal shows them.
    println!();
    for diagnostic in report.diagnostics() {
        print!("{}", engine.render(diagnostic));
    }

    // Every code has an explanation, which `nmo --explain` prints.
    if let Some(entry) = report
        .diagnostics()
        .first()
        .and_then(|diagnostic| codes::REGISTRY.explain(diagnostic.code()))
    {
        println!("\n{} ({}): {}", entry.code, entry.name, entry.explanation);
    }

    // The check also returns what its type analysis inferred,
    // for the statements that translate.
    let types = report.types().expect("the check reached the type analysis");
    if let Some(columns) = types.predicate(&Identifier::bare("older")) {
        let columns: Vec<String> = columns
            .iter()
            .map(|column| TypeSyntax::of(column).to_string())
            .collect();
        println!("\nolder({})", columns.join(", "));
    }
    // A rule that can never fire leaves its variables no value to hold.
    for rule in types.rules() {
        let variables: Vec<String> = rule
            .variables()
            .map(|(name, datatype)| format!("?{name}: {}", TypeSyntax::of(datatype)))
            .collect();
        let location = engine
            .location(rule.span())
            .expect("the rule is in the registered text");
        println!(
            "the rule on line {} binds {}",
            location.line,
            variables.join(", ")
        );
    }
}
