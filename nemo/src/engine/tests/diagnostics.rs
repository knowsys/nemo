//! Refused programs: the kind each problem is reported as, and what its report names.

use super::*;

/// Returns the problem that loading a source fails with.
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

/// The warning for an ignored directive names its keyword and labels the directive.
#[test]
fn an_ignored_directive_names_its_keyword_at_its_place() {
    let report = warning(
        "@define f(?x) = ?x + 1 . q(1) . p(?x) :- q(?x) .",
        codes::IGNORED_DIRECTIVE,
    );
    assert_eq!(report.field("directive"), Some("@define"), "{report:?}");
    assert_eq!(report.labels().len(), 1, "{report:?}");
}

/// A program that declares an input twice is refused with the error a module gets for it.
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

/// The anonymous variable `_` is refused wherever a value is needed:
/// in a head, an expression, an aggregate, or a comparison.
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

/// An aggregate in a fact or a rule body is refused as an aggregate outside a head.
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

/// An existential variable in an aggregate's scope is refused as a scope variable of the
/// wrong kind.
#[test]
fn an_existential_scope_variable_is_an_error_of_the_scope() {
    let problem = refusal("q(1, 2) . p(?x, #sum(?y per !z)) :- q(?x, ?y) .");
    assert_eq!(problem.code(), codes::SCOPE_VARIABLE_KIND, "{problem:?}");
}

/// The error for a head expression that combines an aggregate with an aggregated variable
/// names the variable.
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

/// The error for a parameter whose value reads an undeclared parameter names the undeclared
/// one.
#[test]
fn a_parameter_reading_an_undeclared_one_names_it() {
    let problem = refusal("@parameter $a = $c + 1 . q(1) . p($a) :- q(?x) .");
    assert_eq!(problem.code(), codes::UNDEFINED_PARAMETER, "{problem:?}");
    assert_eq!(problem.field("name"), Some("c"), "{problem:?}");
}

/// The error for a cycle of parameters names a parameter on the cycle,
/// not one that only depends on it.
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

/// A map that names a constant key twice is refused, in a fact, in a rule head that builds it
/// per row, and in a declared type.
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

/// The error for a predicate used at two arities names the predicate as written,
/// not as it resolves.
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

/// A constant expression that fails to evaluate, in a fact or in a condition,
/// is reported with the reason.
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

/// An import directive with a missing option, an unknown format, or an option key that is
/// not a name is refused with an error for that problem.
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

/// The errors for a named argument and a prefixed name that are easily confused
/// each hint at the other spelling.
#[test]
fn a_named_argument_and_a_prefixed_name_point_to_each_other() {
    // A space after a declared prefix's `:` makes a named argument,
    // and the problem says how to write the prefixed name.
    let hint = "write `wd:42` without spaces for the prefixed name";
    for (source, code) in [
        (
            "@prefix wd: <http://www.wikidata.org/entity/> . in(wd: 42) .",
            codes::NAMED_ARGUMENTS_UNDECLARED,
        ),
        (
            "@prefix wd: <http://www.wikidata.org/entity/> .
             @declare in(item: any) . in(wd: 42) .",
            codes::UNKNOWN_COLUMN,
        ),
    ] {
        let problem = refusal(source);
        assert_eq!(problem.code(), code, "{problem:?}");
        assert!(
            problem.hints().iter().any(|given| given == hint),
            "{problem:?}"
        );
    }
    let problem = refusal("in(item: 42) .");
    assert_eq!(
        problem.hints().len(),
        1,
        "a label that is not a prefix: {problem:?}"
    );
    // The prefixed name keeps the local name as written in the source, not the number's value.
    let mut engine = Engine::new();
    let file = engine
        .add_source(
            "program",
            "@prefix wd: <http://www.wikidata.org/entity/> . in(wd: 007) .",
        )
        .expect("the source registers");
    let (program, diagnostics) = engine.parse_source(file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let problem = engine.load(program).expect_err("the program is refused");
    assert!(
        problem
            .hints()
            .iter()
            .any(|given| given.contains("`wd:007`")),
        "{problem:?}"
    );

    // Without the space, a named argument is parsed as a prefixed name,
    // and the hint of the undeclared prefix mentions the space.
    let problem = refusal("@declare person(age: int) . person(age:30) .");
    assert_eq!(problem.code(), codes::UNDECLARED_PREFIX, "{problem:?}");
    assert!(
        problem.hints()[0].contains("space after the `:`"),
        "{problem:?}"
    );
}

/// The engine resolves and renders a diagnostic against the sources registered with it:
/// a label of a problem in a module is placed in the module's text, under the module's name.
#[test]
fn the_engine_renders_a_diagnostic_against_its_registered_sources() {
    let mut engine = Engine::new();
    // The head of the module's rule has a variable that its body does not bind.
    engine
        .add_module(
            "lib::broken",
            "@input p .\n@output q .\nq(?x, ?y) :- p(?x) .\n",
        )
        .expect("the name is a module name");
    let refused = engine
        .ingest(
            "main.rls",
            "base(1) .\n@use lib::broken { p = base } as b .\n",
            |engine, program| engine.load(program),
        )
        .expect_err("the module's rule is unsafe");
    let [problem] = refused.as_slice() else {
        panic!("the load returns its first problem: {refused:?}");
    };

    let resolved = engine.resolve(problem);
    let label = &resolved.labels[0];
    assert_eq!(label.source.as_deref(), Some("lib/broken.rls"), "{label:?}");
    assert_eq!((label.line, label.column), (Some(3), Some(7)), "{label:?}");
    // The rendering quotes the line of the module's text.
    let rendered = engine.render(problem);
    assert!(rendered.contains("q(?x, ?y) :- p(?x) ."), "{rendered}");
}
