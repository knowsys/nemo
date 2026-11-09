//! Structured values: the errors that building and reading them report,
//! and structures around nondeterministic calls.

use super::*;

/// An accessor outside a pattern fails for a wrong operand with a counted evaluation error,
/// while a pattern just does not match.
#[test]
fn an_accessor_outside_a_pattern_errors_and_is_counted() {
    // `5` is not a tuple, and `(1, 2)` has no element 9.
    let mut engine = run("
        t(5) . u((1, 2)) .
        q(?x) :- t(?t), ?x = ?t[0] .
        r(?x) :- u(?t), ?x = ?t[9] .
    ");
    assert_eq!(count(&mut engine, "q"), 0);
    assert_eq!(count(&mut engine, "r"), 0);
    let errors = engine.evaluation_errors();
    assert_eq!(errors.count(EvaluationError::TypeMismatch), 1);
    assert_eq!(errors.count(EvaluationError::Missing), 1);
}

/// A variable in the key of a map pattern is reported as unsupported.
#[test]
fn a_variable_in_a_map_patterns_key_is_an_error() {
    let program = parse("q(?k) :- p({?k = 1}) .");
    let mut engine = Engine::new();
    let problem = engine
        .load(program)
        .expect_err("variable keys are unsupported");
    assert_eq!(problem.code(), codes::UNSUPPORTED, "{problem:?}");
}

/// A map that a rule builds with the same key twice is dropped and counted as malformed,
/// once per row.
#[test]
fn a_map_built_with_one_key_twice_is_dropped_and_counted() {
    let mut engine = run("
        n(1) . n(2) .
        twice({?x = 1, ?x = 2}) :- n(?x) .
    ");
    assert_eq!(count(&mut engine, "twice"), 0);
    let errors = engine.evaluation_errors();
    assert_eq!(
        errors.count(EvaluationError::Malformed),
        2,
        "both rows were counted, not silently lost"
    );
}

/// A map constant that names a key twice fails the load.
#[test]
fn a_map_naming_a_key_twice_is_rejected() {
    let program = parse("bad({a = 1, a = 2}) .");
    let mut engine = Engine::new();
    let problem = engine.load(program).expect_err("the map is malformed");
    assert_eq!(problem.code(), codes::DUPLICATE_MAP_KEY, "{problem:?}");
}

/// A structure around a nondeterministic function call is built per row,
/// as the bare call is evaluated per row.
#[test]
fn structures_around_nondeterministic_calls_are_built_per_row() {
    let mut engine = run("
        p(1) . p(2) . p(3) .
        d((STRUUID(),)) :- p(?x) .
        m({id = STRUUID()}) :- p(?x) .
    ");
    assert_eq!(count(&mut engine, "d"), 3);
    assert_eq!(count(&mut engine, "m"), 3);
}
