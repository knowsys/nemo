//! The values that [set_parameter](Engine::set_parameter) gives the parameters of a program.

use super::*;

/// A program whose rule keeps the numbers below `$limit`, which is declared without a value.
const BELOW_LIMIT: &str = "@parameter $limit: int .
    n(1) . n(2) . n(3) .
    small(?x) :- n(?x), ?x < $limit .";

/// A set value fills a parameter that is declared without one,
/// and replaces the value that a declaration states.
#[test]
fn a_set_value_fills_a_declaration_and_replaces_its_own_value() {
    let mut engine = Engine::new();
    engine.set_parameter("limit", 3);
    // The name may be written with its `$`.
    engine.set_parameter("$unit", "points");
    engine
        .load(parse(
            "@parameter $limit: int .
             @parameter $unit: string = \"none\" .
             n(1) . n(2) . n(3) .
             small(?x, $unit) :- n(?x), ?x < $limit .",
        ))
        .expect("every parameter has a value");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(
        engine.rows("small").expect("the program derives `small`"),
        vec![
            vec![DataValue::integer(1), DataValue::string("points")],
            vec![DataValue::integer(2), DataValue::string("points")],
        ]
    );
}

/// A value set for a parameter that the program does not declare refuses the program,
/// naming the parameters that it declares, until the value is removed.
#[test]
fn a_value_for_an_undeclared_parameter_refuses_the_program() {
    let program = || parse("@parameter $limit: int = 1 .\np($limit) .");
    let mut engine = Engine::new();
    engine.set_parameter("limt", 3);
    let problem = engine
        .load(program())
        .expect_err("the program has no `$limt`");
    assert_eq!(problem.code(), codes::UNKNOWN_PARAMETER, "{problem:?}");
    assert_eq!(problem.field("name"), Some("limt"));
    assert!(
        problem.notes().iter().any(|note| note.contains("`$limit`")),
        "{problem:?}"
    );

    assert!(engine.unset_parameter("limt"));
    assert!(!engine.unset_parameter("limt"), "the value is removed once");
    engine.load(program()).expect("no value is set any more");
}

/// A parameter of a module instance takes no value from the engine:
/// its value comes from the `@use`.
#[test]
fn a_set_value_is_not_for_a_parameter_of_a_module_instance() {
    let mut engine = Engine::new();
    engine
        .add_module(
            "lib::m",
            "@parameter $limit: int = 1 .\n@output q .\nq($limit) .\n",
        )
        .expect("the name is a module name");
    // The linked program names the instance's parameter `m::limit`.
    engine.set_parameter("m::limit", 5);
    let refused = engine
        .load_source("main.rls", "@use lib::m as m .\nr(?x) :- m::q(?x) .\n")
        .expect_err("the program itself declares no parameter");
    assert_eq!(refused[0].code(), codes::UNKNOWN_PARAMETER, "{refused:?}");
}

/// An extension takes the values set for the parameters it declares,
/// and a value set for a parameter of the loaded program does not refuse it.
#[test]
fn an_extension_takes_the_values_of_its_own_parameters() {
    let mut engine = Engine::new();
    engine.set_parameter("limit", 3);
    engine
        .load(parse(BELOW_LIMIT))
        .expect("the parameter has a value");

    engine.set_parameter("floor", 1);
    engine
        .extend(parse(
            "@parameter $floor: int .
             middle(?x) :- n(?x), ?x > $floor, ?x < $limit .",
        ))
        .expect("the extension declares `$floor`, and the loaded program `$limit`");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(
        engine
            .rows("middle")
            .expect("the extension derives `middle`"),
        vec![vec![DataValue::integer(2)]]
    );

    engine.set_parameter("other", 1);
    let problem = engine
        .extend(parse("more(1) ."))
        .expect_err("neither program declares `$other`");
    assert_eq!(problem.code(), codes::UNKNOWN_PARAMETER, "{problem:?}");
    assert_eq!(problem.field("name"), Some("other"));
}

/// The loaded program keeps the value it was loaded with,
/// and a reload gives it the value set since.
#[test]
fn a_reload_takes_the_value_set_since_the_load() {
    let mut engine = Engine::new();
    engine.set_parameter("limit", 2);
    engine
        .load(parse(BELOW_LIMIT))
        .expect("the parameter has a value");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "small"), 1);

    engine.set_parameter("limit", 4);
    engine.reason().expect("the fixpoint stands");
    assert_eq!(count(&mut engine, "small"), 1, "the load's value stands");

    engine
        .reload(parse(BELOW_LIMIT))
        .expect("the parameter has a value");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "small"), 3);
}

/// A check reads a set value as a load does:
/// a value outside the declared type of its parameter is a problem of the check,
/// where a parameter without a value stands for any value of its type.
#[test]
fn a_check_reads_the_set_values() {
    let mut engine = Engine::new();
    assert!(
        engine.check(parse(BELOW_LIMIT)).diagnostics().is_empty(),
        "without a value, the parameter stands for any integer"
    );

    engine.set_parameter("limit", "three");
    let report = engine.check(parse(BELOW_LIMIT));
    // The check goes on after this problem, and also reports the rule that reads the parameter.
    let problem = report
        .diagnostics()
        .iter()
        .find(|problem| problem.code() == codes::PARAMETER_OUTSIDE_TYPE)
        .unwrap_or_else(|| panic!("the value is a string: {report:?}"));
    assert_eq!(problem.field("name"), Some("limit"));
}

/// A value may read other parameters, as a declared value may.
#[test]
fn a_set_value_may_read_another_parameter() {
    let mut engine = Engine::new();
    let twice = Term::parse("$base * 2").expect("the text is a term");
    engine.set_parameter("limit", twice);
    engine
        .load(parse(
            "@parameter $base: int = 2 .
             @parameter $limit: int .
             n(1) . n(2) . n(3) . n(4) .
             small(?x) :- n(?x), ?x < $limit .",
        ))
        .expect("`$limit` is twice `$base`");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "small"), 3);
}
