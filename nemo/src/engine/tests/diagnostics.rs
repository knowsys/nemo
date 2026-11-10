//! Refused programs: the kind each problem is reported as, and what its report names.

use super::*;

use crate::{
    diagnostics::Location,
    engine::remote::{Failure, Request},
};

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

/// Returns the location at a line and column of the source with the given name.
fn at(source: &str, line: u32, column: u32) -> Location {
    Location {
        source: Some(source.into()),
        line,
        column,
    }
}

/// A program whose second line is a rule with the head variable `?y`, in column 7,
/// which its body does not bind.
const UNSAFE: &str = "p(1) .\nq(?x, ?y) :- p(?x) .\n";

/// A program whose second line imports a resource from the network.
const REMOTE: &str = "p(1) .\n@import q from csv { resource = \"http://example.org/q.csv\" } .\n";

/// A loading method, in the form that [Engine::with_source] calls.
type Method = fn(&mut Engine, Program) -> Result<(), Diagnostic>;

/// A client that fails every request.
struct Unreachable;

impl Client for Unreachable {
    fn fetch_all(&self, requests: &[Request], _: &dyn Fn() -> bool) -> Option<Vec<Response>> {
        let failed = |_| Err(Failure::transport("the host is unreachable"));
        Some(requests.iter().map(failed).collect())
    }
}

/// Returns the location of the problem that each method fails with for the program `text`,
/// with the method's name.
///
/// A method marked `true` is called on an engine that has loaded a program,
/// and every engine has a client that fails every request.
fn refused_at(
    text: &str,
    methods: &[(&'static str, bool, Method)],
) -> Vec<(&'static str, Option<Location>)> {
    methods
        .iter()
        .map(|&(name, after_load, method)| {
            let mut engine = Engine::new();
            engine.set_client(Some(Box::new(Unreachable)));
            if after_load {
                engine
                    .load_source("base.rls", "p(1) .\n")
                    .expect("the program loads");
            }
            let refused = engine
                .with_source("test.rls", text, method)
                .expect_err("the program is refused");
            (name, refused[0].location().cloned())
        })
        .collect()
}

/// A problem in a module has its location in the module's text, under the module's name,
/// and the engine renders it with the line of that text.
#[test]
fn a_problem_in_a_module_is_located_and_rendered_in_the_modules_text() {
    let mut engine = Engine::new();
    // The head of the module's rule has a variable that its body does not bind.
    engine
        .add_module(
            "lib::broken",
            "@input p .\n@output q .\nq(?x, ?y) :- p(?x) .\n",
        )
        .expect("the name is a module name");
    let refused = engine
        .load_source(
            "main.rls",
            "base(1) .\n@use lib::broken { p = base } as b .\n",
        )
        .expect_err("the module's rule is unsafe");
    let [problem] = refused.as_slice() else {
        panic!("the load returns its first problem: {refused:?}");
    };

    assert_eq!(problem.location(), Some(&at("lib/broken.rls", 3, 7)));
    let rendered = engine.render(problem);
    assert!(rendered.contains("q(?x, ?y) :- p(?x) ."), "{rendered}");
}

/// The diagnostics of parsing a registered source have their locations under its name.
#[test]
fn a_parse_of_a_registered_source_locates_its_diagnostics() {
    let mut engine = Engine::new();
    // The atom `p(?x` on line 2 is not closed.
    let file = engine
        .add_source("test.rls", "p(1) .\nq(?x) :- p(?x .\n")
        .expect("the text is small");
    let (_, diagnostics) = engine.parse_source(file);
    let [problem] = diagnostics.as_slice() else {
        panic!("one statement does not parse: {diagnostics:?}");
    };
    assert_eq!(problem.location(), Some(&at("test.rls", 2, 15)));
}

/// A link that fails returns its problem with the location of the problem's label.
#[test]
fn a_link_locates_the_problem_it_returns() {
    let mut engine = Engine::new();
    // The module registered as `lib::m` has no output `q`, which line 2 reads.
    engine
        .add_module("lib::m", "@output p .\np(1) .\n")
        .expect("the name is a module name");
    let file = engine
        .add_source("main.rls", "@use lib::m as m .\nr(?x) :- m::q(?x) .\n")
        .expect("the text is small");
    let (program, diagnostics) = engine.parse_source(file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let problem = engine.link(program).expect_err("`m` offers no `q`");
    assert_eq!(problem.location(), Some(&at("main.rls", 2, 10)));
}

/// A loading method returns the problem of a program with the location of the problem's
/// label, in its plain form and in its `start_*` form.
#[test]
fn a_loading_method_locates_the_problem_it_returns() {
    let methods: [(&str, bool, Method); 6] = [
        ("load", false, Engine::load),
        ("start_load", false, |engine, program| {
            engine.start_load(program).map(drop)
        }),
        ("extend", true, Engine::extend),
        ("start_extend", true, |engine, program| {
            engine.start_extend(program).map(drop)
        }),
        ("reload", true, Engine::reload),
        ("start_reload", true, |engine, program| {
            engine.start_reload(program).map(drop)
        }),
    ];
    let expected = methods.map(|(name, _, _)| (name, Some(at("test.rls", 2, 7))));
    assert_eq!(refused_at(UNSAFE, &methods), expected);
}

/// A loading method that fails when the responses to its requests arrive returns its problem
/// with the location of the problem's label,
/// whether the engine's client sent the requests or `resume` was given the responses.
#[test]
fn a_loading_method_locates_a_problem_of_its_responses() {
    let resumed: Method = |engine, program| {
        let Progress::Suspended(pending) = engine.start_load(program)? else {
            panic!("the import needs its resource");
        };
        let failure = Failure::transport("the host is unreachable");
        engine.resume([(pending[0].id, Err(failure))]).map(drop)
    };
    let methods: [(&str, bool, Method); 4] = [
        ("load", false, Engine::load),
        ("extend", true, Engine::extend),
        ("reload", true, Engine::reload),
        ("resume", false, resumed),
    ];
    let expected = methods.map(|(name, _, _)| (name, Some(at("test.rls", 2, 1))));
    assert_eq!(refused_at(REMOTE, &methods), expected);
}

/// A check returns its diagnostics with their locations:
/// those of parsing, those of the check, and the problem that ends a check early.
#[test]
fn a_check_locates_its_diagnostics() {
    let locations = |report: &CheckReport| -> Vec<Option<Location>> {
        report
            .diagnostics()
            .iter()
            .map(|diagnostic| diagnostic.location().cloned())
            .collect()
    };
    let mut engine = Engine::new();
    // The first line does not parse, and the head variable `?y` of the second is not bound.
    let whole = engine.check_source("test.rls", "p(1 .\nq(?x, ?y) :- p(?x) .\n");

    // A program that is not linked ends the check early, with a label at its `@use`.
    let file = engine
        .add_source("main.rls", "base(1) .\n@use lib::m { p = base } as m .\n")
        .expect("the text is small");
    let (program, diagnostics) = engine.parse_source(file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let ended = engine.check(program);

    assert_eq!(
        (locations(&whole), locations(&ended)),
        (
            vec![Some(at("test.rls", 1, 5)), Some(at("test.rls", 2, 7))],
            vec![Some(at("main.rls", 2, 1))]
        ),
        "{whole:?}\n{ended:?}"
    );
}

/// A report has the location of its label,
/// and a report about a statement of a module instance also has the location of the `@use`
/// that made the instance.
#[test]
fn a_report_has_the_locations_of_its_labels() {
    let mut engine = Engine::new();
    // Line 3 of the module and line 3 of the program are `@define` directives,
    // and each is ignored with a warning.
    engine
        .add_module(
            "lib::noisy",
            "@output q .\nq(1) .\n@define f(?x) = ?x + 1 .\n",
        )
        .expect("the name is a module name");
    engine
        .load_source(
            "main.rls",
            "p(1) .\n@use lib::noisy as n .\n@define g(?x) = ?x + 2 .\n",
        )
        .expect("the program loads");
    let located: Vec<Vec<Option<Location>>> = engine
        .reports()
        .iter()
        .filter(|report| report.code() == codes::IGNORED_DIRECTIVE)
        .map(|report| {
            let labels = report.labels().iter();
            labels.map(|label| label.location.clone()).collect()
        })
        .collect();
    assert!(
        located.contains(&vec![Some(at("main.rls", 3, 1))]),
        "the program's directive: {located:?}"
    );
    assert!(
        located.contains(&vec![
            Some(at("lib/noisy.rls", 3, 1)),
            Some(at("main.rls", 2, 1))
        ]),
        "the module's directive: {located:?}"
    );
}

/// Rows that a rule of a module instance cannot take are refused
/// with the location of the `@use` that made the instance.
#[test]
fn imported_rows_that_a_module_instance_refuses_are_refused_at_its_use() {
    let mut engine = Engine::new();
    // The module copies its input into `q`, which it declares to hold integers.
    engine
        .add_module(
            "lib::typed",
            "@input p .
@declare q(x: int) .
@output q .
q(?x) :- p(?x) .
",
        )
        .expect("the name is a module name");
    engine
        .load_source(
            "main.rls",
            "base(1) .
@use lib::typed { p = base } as t .
",
        )
        .expect("the program loads");
    let refused = engine
        .import_rows("base", [vec![DataValue::string("a")]])
        .expect_err("the module's `q` holds no string");
    assert_eq!(
        refused.location(),
        Some(&at("main.rls", 2, 1)),
        "{refused:?}"
    );
}

/// A label of a program whose text is not registered has no location,
/// since the engine has no text to count the lines of.
#[test]
fn a_label_of_an_unregistered_text_has_no_location() {
    let problem = refusal(UNSAFE);
    assert_eq!(problem.labels().len(), 1, "{problem:?}");
    assert_eq!(problem.location(), None);
}
