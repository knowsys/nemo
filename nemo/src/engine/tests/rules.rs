//! Rules: refused programs, how tables and plans are laid out,
//! nondeterministic functions, and counted evaluation errors.

use super::*;

/// A builtin's name in predicate position is refused with a label that says it is a builtin.
#[test]
fn a_builtin_call_in_predicate_position_says_so() {
    let (_, diagnostics) = Program::parse("log(?x) :- n(?x) .");
    let problem = diagnostics.first().expect("the statement is refused");
    assert!(
        problem.labels()[0].message.contains("`log` is a builtin"),
        "{problem:?}"
    );
}

/// A table that a stratum leaves final is merged into one block before a later stratum
/// reads it.
#[test]
fn a_finished_table_merges_whole_before_later_strata_read_it() {
    // `path` grows over several iterations, and `reach` reads it in a later
    // stratum, which does not write `path`.
    let source = "e(1, 2) . e(2, 3) . e(3, 4) . e(4, 5) . e(5, 6) .\n\
                  path(?x, ?y) :- e(?x, ?y) .\n\
                  path(?x, ?z) :- path(?x, ?y), e(?y, ?z) .\n\
                  reach(?y) :- path(1, ?y) .";
    let program = parse(source);
    let mut engine = Engine::new();
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    let &Registered { table: path, .. } = engine.predicates.get("path").expect("registered");
    assert_eq!(engine.manager.blocks(path).len(), 1);
    assert_eq!(count(&mut engine, "reach"), 5);
}

/// A predicate with more columns than a 16-bit number can count loads.
#[test]
fn a_predicate_wider_than_16_bits_of_columns_loads() {
    let wide = format!("p({}) .", vec!["1"; 70_000].join(", "));
    let program = parse(&wide);
    let mut engine = Engine::new();
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "p"), 1);
}

/// A second `@base` with another IRI is an error for a load and for a check,
/// labeled at both directives.
#[test]
fn a_second_base_iri_is_an_error() {
    let source = "
        @base <https://example.org/> .
        @base <https://example.com/> .
        p(1) .
    ";
    let problem = Engine::new()
        .load(parse(source))
        .expect_err("a file declares one base IRI");
    assert_eq!(problem.code(), codes::CONFLICTING_BASE, "{problem:?}");
    // The second `@base`, and the first it conflicts with.
    assert_eq!(problem.labels().len(), 2, "{problem:?}");

    let problems = check(source);
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert_eq!(problems[0].code(), codes::CONFLICTING_BASE, "{problems:?}");
}

/// Declaring the same `@base` again, in the program or in an extension, is no conflict.
#[test]
fn the_same_base_again_is_no_conflict() {
    let mut engine = run("
        @base <https://example.org/> .
        @base <https://example.org/> .
        p(1) .
    ");
    engine
        .extend(parse("@base <https://example.org/> . q(1) ."))
        .expect("the loaded base again extends");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "https://example.org/q"), 1);
}

/// A rule with an unbound head variable fails the load,
/// and so does a module-qualified name in a program that is not linked.
#[test]
fn unsafe_and_unsupported_rules_are_diagnosed() {
    let (program, _) = Program::parse("p(?x) :- q(?y) .");
    let mut engine = Engine::new();
    assert!(engine.load(program).is_err());

    let (program, _) = Program::parse("p(?x) :- m::q(?x) .");
    let mut engine = Engine::new();
    assert!(engine.load(program).is_err());
}

/// A fact that a rule derives from a nullary fact traces through it,
/// and the nullary fact traces to the rule that derived it.
#[test]
fn a_derived_nullary_fact_traces_like_any_other() {
    let mut engine = run("
        p(1) .
        found() :- p(_) .
        answer(yes) :- found() .
    ");
    let fact = Fact::parse("answer(yes) .").expect("the fact parses");
    let response = engine.trace(&[fact]).expect("tracing answers");
    let answer = response.proof(0).expect("the fact holds");
    let premises: Vec<_> = answer.premises().collect();
    let facts: Vec<&str> = premises.iter().map(|premise| premise.fact()).collect();
    assert_eq!(facts, ["found()"], "{response:?}");
    let found = premises[0];
    assert!(found.rule().is_some(), "a derived fact: {response:?}");
    let facts: Vec<&str> = found.premises().map(|premise| premise.fact()).collect();
    assert_eq!(facts, ["p(1)"], "{response:?}");
}

/// A rule without positive body atoms fires once, adds no predicate to the listing,
/// derives facts that trace to the rule alone, and fires once added by an extension.
#[test]
fn a_rule_without_positive_atoms_fires_once_and_hides_what_it_reads() {
    let mut engine = run("
        q(1) .
        p(?y) :- ?x = 2, ?y = ?x + 1 .
    ");
    assert_eq!(rows(&mut engine, "p"), vec![vec!["3".to_string()]]);
    let listed: Vec<String> = engine
        .predicates()
        .into_iter()
        .map(|info| info.name)
        .collect();
    assert_eq!(
        listed,
        ["p", "q"],
        "the row the body reads is not a predicate"
    );

    // Its facts trace to the rule alone.
    let fact = Fact::parse("p(3) .").expect("the fact parses");
    let response = engine.trace(&[fact]).expect("tracing answers");
    let proof = response.proof(0).expect("the fact holds");
    assert!(proof.rule().is_some(), "a derived fact: {response:?}");
    assert_eq!(proof.premises().count(), 0, "{response:?}");

    // A rule added later fires at the next run.
    engine
        .extend(parse("r(?x) :- ?x = 7 ."))
        .expect("the rule joins the program");
    engine.reason().expect("reasoning continues");
    assert_eq!(rows(&mut engine, "r"), vec![vec!["7".to_string()]]);
}

/// A rule without positive body atoms must still bind every variable.
#[test]
fn a_rule_without_positive_atoms_still_binds_every_variable() {
    let problem = Engine::new()
        .load(parse("p(?x) :- ?x > 1 ."))
        .expect_err("the variable is unbound");
    assert_eq!(problem.code(), codes::UNSAFE_VARIABLE, "{problem:?}");
}

/// A nondeterministic function is called once per row,
/// both in an assignment and in a ground condition.
#[test]
fn nondeterministic_assignments_draw_per_row() {
    let mut engine = run("
        data(1) . data(2) . data(3) .
        drawn(?x, ?v) :- data(?x), ?v = STRUUID() .
        passed(?x) :- data(?x), RAND() >= 0.0d .
    ");
    // Every row gets its own value, and the ground condition is evaluated per
    // row instead of being folded when the rule is compiled.
    let rows = rows(&mut engine, "drawn");
    assert_eq!(rows.len(), 3);
    let mut draws: Vec<&String> = rows.iter().map(|row| &row[1]).collect();
    draws.sort();
    draws.dedup();
    assert_eq!(draws.len(), 3);
    assert_eq!(count(&mut engine, "passed"), 3);
}

/// A recursive rule that calls a nondeterministic function stores one row per input row.
#[test]
fn recursive_draws_happen_once_per_row() {
    // The rule calling `STRUUID()` sits in a recursive component, so its
    // predicate grows across iterations. A delta window that starts at a
    // block boundary before the rule's last application would read some
    // rows again, and every second read would call the function again,
    // storing a second row the head deduplication cannot remove. So such a
    // rule reads exact windows, which gives one value per row.
    let mut engine = run("
        seed(1) .
        data(?x) :- seed(?x) .
        data(?y) :- drawn(?x, ?v), ?y = ?x + 1, ?y < 6 .
        drawn(?x, ?v) :- data(?x), ?v = STRUUID() .
        data(?y) :- drawn(?x, ?v), ?y = ?x + 2, ?y < 6 .
    ");
    assert_eq!(count(&mut engine, "data"), 5);
    assert_eq!(count(&mut engine, "drawn"), 5);
}

/// A predicate negated within its own recursion fails the load.
#[test]
fn a_predicate_negated_within_its_own_recursion_is_refused() {
    let (program, _) = Program::parse("q(1) . p(?x) :- q(?x), ~p(?x) .");
    let mut engine = Engine::new();
    assert!(engine.load(program).is_err());
}

/// A condition that fails to evaluate on a row drops the row and counts an evaluation error.
#[test]
fn evaluation_errors_are_counted_not_silent() {
    let mut engine = run("
        value(1, \"oops\") . value(2, 5) .
        big(?x) :- value(?x, ?v), ?v > 1 .
    ");
    assert_eq!(count(&mut engine, "big"), 1);
    let errors = engine.evaluation_errors();
    assert!(errors.total() > 0);
}

/// One expression with more constants than an instruction addresses is
/// refused, not compiled with a wrapped operand.
#[test]
fn oversized_expressions_are_errors() {
    // Two structures of 35,000 non-ground pairs: each pair costs one
    // constant, and neither structure is too large on its own.
    let half = |from: usize| {
        (from..from + 35_000)
            .map(|index| format!("(?x, {index})"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let source = format!(
        "q(1) . p(?y) :- q(?x), ?y = (({}), ({})) .",
        half(0),
        half(35_000)
    );
    let program = parse(&source);
    let reports = Engine::new().check(program).into_diagnostics();
    assert!(
        reports
            .iter()
            .any(|report| report.code() == codes::EXPRESSION_TOO_LARGE),
        "{reports:?}"
    );
}

/// The plan layout runs a rule's operators inside its joins,
/// so each shape runs in as few units as the executor allows:
///
/// - One unit for a plain rule, and one for a negation over variables.
/// - A computed head column, a filter calling a nondeterministic function, or a
///   negation over a computed column ends a unit below it.
/// - Several heads read one shared result.
/// - An aggregate and an existential check read units of their own.
///
/// A shape that runs in more units than these lost a fusion.
#[test]
fn rule_shapes_run_in_few_units() {
    let facts = "e(1, 2) . e(2, 3) . e(3, 4) . f(3) . g(3, 3) .";
    let cases: &[(&str, &str)] = &[
        ("plain", "p(?x, ?y) :- e(?x, ?z), e(?z, ?y) ."),
        ("computed head", "p(?x, ?y + 1) :- e(?x, ?z), e(?z, ?y) ."),
        ("drawing filter", "p(?x) :- e(?x, ?y), RAND() < 2 ."),
        ("several heads", "p(?x), q(?y) :- e(?x, ?z), e(?z, ?y) ."),
        ("negation", "p(?x, ?y) :- e(?x, ?y), ~f(?y) ."),
        (
            "negation over a computed column",
            "p(?x, ?y) :- e(?x, ?y), ?z = ?y + 1, ~f(?z) .",
        ),
        (
            "negation with a constant",
            "p(?x, ?y) :- e(?x, ?y), ~g(?y, 3) .",
        ),
        ("aggregate", "c(?x, #count(?y)) :- e(?x, ?z), e(?z, ?y) ."),
        ("existential", "p(?x, !v) :- e(?x, ?y) ."),
    ];
    let expected = [1, 2, 2, 3, 1, 2, 2, 3, 3];
    for ((name, rule), steps) in cases.iter().zip(expected) {
        let engine = run(&format!(
            "{facts}
{rule}"
        ));
        assert_eq!(rule_steps(&engine), vec![steps], "{name}");
    }
}
