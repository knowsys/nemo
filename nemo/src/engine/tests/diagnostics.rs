//! Refused programs: the kind each problem is reported as, and what its report names.

use super::*;

/// Returns the problem loading a source fails with.
fn refusal(source: &str) -> Diagnostic {
    Engine::new()
        .load(parse(source))
        .expect_err("the program is refused")
}

/// Returns the warning of the given code that loading a source reports.
fn warning(source: &str, code: nemo_physical::diagnostics::Code) -> Diagnostic {
    let mut engine = Engine::new();
    engine.load(parse(source)).expect("the program loads");
    engine
        .reports()
        .iter()
        .find(|report| report.code() == code)
        .cloned()
        .expect("the warning is reported")
}

#[test]
fn an_ignored_directive_names_its_keyword_at_its_place() {
    let report = warning(
        "@define f(?x) = ?x + 1 . q(1) . p(?x) :- q(?x) .",
        codes::IGNORED_DIRECTIVE,
    );
    assert_eq!(report.field("directive"), Some("@define"), "{report:?}");
    assert_eq!(report.labels().len(), 1, "{report:?}");
}

#[test]
fn a_program_declaring_an_input_twice_is_refused_as_a_module_is() {
    for source in [
        "@input edge . @input edge .",
        "@input edge(from: any, to: any) . @input edge(from: any, to: any) .",
    ] {
        let problem = refusal(source);
        assert_eq!(
            problem.code(),
            codes::DUPLICATE_INPUT,
            "{source}: {problem:?}"
        );
        assert_eq!(problem.field("predicate"), Some("edge"), "{problem:?}");
    }
}

#[test]
fn the_anonymous_variable_cannot_provide_a_value() {
    for source in [
        "q(1) . r(_) :- q(?x) .",
        "q(1) . p(_ + 1) :- q(?x) .",
        "q(1) . p((_, ?x)) :- q(?x) .",
        "q(1) . p(?x, !y), r(?x, _) :- q(?x) .",
        "q(1, 2) . p(_, #count(?y)) :- q(?x, ?y) .",
        "q(1, 2) . p(?x, #sum(_)) :- q(?x, ?y) .",
        "q(1) . p(?x) :- q(?x), ?x > _ .",
    ] {
        let problem = refusal(source);
        assert_eq!(
            problem.code(),
            codes::ANONYMOUS_AS_VALUE,
            "{source}: {problem:?}"
        );
    }
}

#[test]
fn an_aggregate_outside_a_head_is_reported_as_such() {
    for source in [
        "p(#count()) .",
        "r(1, 1) . q(1) . p(?x) :- q(?x), r(?x, #count()) .",
        "r(1) . q(1) . p(?x) :- q(?x), ~r(#count()) .",
    ] {
        let problem = refusal(source);
        assert_eq!(
            problem.code(),
            codes::AGGREGATE_OUTSIDE_HEAD,
            "{source}: {problem:?}"
        );
    }
}

#[test]
fn an_existential_scope_variable_is_an_error_of_the_scope() {
    let problem = refusal("q(1, 2) . p(?x, #sum(?y per !z)) :- q(?x, ?y) .");
    assert_eq!(problem.code(), codes::SCOPE_VARIABLE_KIND, "{problem:?}");
}

#[test]
fn an_aggregated_group_column_names_its_variable() {
    let problem = refusal("q(1, 2) . p(#sum(?y) + ?y) :- q(?x, ?y) .");
    assert_eq!(
        problem.code(),
        codes::AGGREGATED_GROUP_COLUMN,
        "{problem:?}"
    );
    assert_eq!(problem.field("variable"), Some("y"), "{problem:?}");
}

#[test]
fn a_parameter_reading_an_undeclared_one_names_it() {
    let problem = refusal("@parameter $a = $c + 1 . q(1) . p($a) :- q(?x) .");
    assert_eq!(problem.code(), codes::UNDEFINED_PARAMETER, "{problem:?}");
    assert_eq!(problem.field("name"), Some("c"), "{problem:?}");
}

#[test]
fn a_parameter_cycle_names_a_parameter_on_it() {
    // `$a` depends on the cycle of `$b` and `$c` without being on it.
    let problem = refusal(
        "@parameter $a = $b . @parameter $b = $c . @parameter $c = $b .
         q(1) . p($a) :- q(?x) .",
    );
    assert_eq!(problem.code(), codes::PARAMETER_CYCLE, "{problem:?}");
    assert_eq!(problem.field("name"), Some("b"), "{problem:?}");
}

#[test]
fn a_map_naming_a_known_key_twice_is_refused_even_if_built_per_row() {
    for source in [
        "bad({a = 1, a = 2}) .",
        "n(1) . bad({a = ?x, a = 2}) :- n(?x) .",
        "@declare bad(x: {a: int, a: string}) .",
    ] {
        let problem = refusal(source);
        assert_eq!(
            problem.code(),
            codes::DUPLICATE_MAP_KEY,
            "{source}: {problem:?}"
        );
        assert_eq!(problem.field("key"), Some("a"), "{source}: {problem:?}");
    }
}

#[test]
fn an_arity_conflict_names_the_predicate_as_written() {
    for (source, written) in [
        ("@base <http://example.org/> . q(1) . q(1, 2) .", "q"),
        (
            "@prefix ex: <http://example.org/> . ex:q(1) . ex:q(1, 2) .",
            "ex:q",
        ),
    ] {
        let problem = refusal(source);
        assert_eq!(
            problem.code(),
            codes::ARITY_MISMATCH,
            "{source}: {problem:?}"
        );
        assert_eq!(
            problem.field("name"),
            Some(written),
            "{source}: {problem:?}"
        );
    }
}

#[test]
fn a_failing_constant_expression_says_why() {
    let problem = refusal("p(1/0) .");
    assert_eq!(
        problem.code(),
        codes::CONSTANT_EXPRESSION_ERRORS,
        "{problem:?}"
    );
    assert_eq!(
        problem.labels()[0].message,
        "division by zero",
        "{problem:?}"
    );

    let report = warning("q(1) . p(?x) :- q(?x), 1/0 > 0 .", codes::NEVER_FIRES);
    assert_eq!(
        report.labels()[0].message,
        "this condition always fails to evaluate: division by zero",
        "{report:?}"
    );
}

#[test]
fn import_directives_written_wrongly_are_reported_by_what_is_wrong() {
    let problem = refusal("@import p from csv {} .");
    assert_eq!(problem.code(), codes::OPTION_MISSING, "{problem:?}");
    assert_eq!(problem.field("option"), Some("resource"), "{problem:?}");

    let problem = refusal("@import p from <http://example.org/csv> { resource = \"p.csv\" } .");
    assert_eq!(problem.code(), codes::UNKNOWN_FORMAT, "{problem:?}");

    let problem = refusal("@import p from csv { \"resource\" = \"p.csv\" } .");
    assert_eq!(problem.code(), codes::UNKNOWN_OPTION, "{problem:?}");
}
