//! Checking a program without loading it, and the inputs of a module run
//! on its own.

use super::*;

/// A check reports every problem it finds, where a load stops at the first.
#[test]
fn checking_collects_every_problem() {
    // `broken` does not bind `?y`, and `cycle` negates itself.
    let problems = check(
        "
        tail(1) .
        head(?x) :- tail(?x) .
        broken(?y) :- head(?x) .
        cycle(?x) :- head(?x), ~cycle(?x) .
    ",
    );
    assert_eq!(problems.len(), 2, "{problems:?}");
}

/// A check validates an import's directive without reading its resource.
#[test]
fn checking_reads_no_import_data() {
    // The resource does not exist.
    let problems = check(
        "
        @import p from csv { resource = \"nowhere.csv\", format = (int, int) } .
        q(?x) :- p(?x, ?x) .
    ",
    );
    assert!(problems.is_empty(), "{problems:?}");
}

/// A check reports an import directive with an unknown option.
#[test]
fn checking_still_reports_a_malformed_import() {
    let problems = check(
        "
        @import p from csv { nonsense = 1, resource = \"data.csv\" } .
    ",
    );
    assert_eq!(problems.len(), 1, "{problems:?}");
}

/// A check of a module on its own does not report the rules over its inputs as never firing.
#[test]
fn a_modules_inputs_are_open_predicates() {
    // The input `edge` is given by whoever uses the module.
    let problems = check(
        "
        @input edge .
        closure(?x, ?y) :- edge(?x, ?y) .
        closure(?x, ?z) :- edge(?x, ?y), closure(?y, ?z) .
        @output closure .
    ",
    );
    assert!(problems.is_empty(), "{problems:?}");
}

/// The column types that an `@input` declares type the rules over it,
/// so that a check finds a rule that never fires, although the data comes from outside.
#[test]
fn a_typed_input_types_the_module_from_within() {
    // A name, which is a string, never compares with an age, which is an integer.
    let problems = check(
        "
        @input person(name: string, age: int) .
        odd(?x) :- person(?x, ?a), ?x > ?a .
        @output odd .
    ",
    );
    let [warning] = &problems[..] else {
        panic!("expected the one warning: {problems:?}");
    };
    assert_eq!(warning.code(), codes::NEVER_FIRES, "{warning:?}");
}

/// Loading a program with an `@input` that nothing supplies warns about the input.
#[test]
fn an_unsupplied_input_is_warned_about_at_load_only() {
    let source = "@input edge .\nreach(?x, ?y) :- edge(?x, ?y) .\n@output reach .";
    let (program, _) = Program::parse(source);
    let mut engine = Engine::new();
    engine.load(program).expect("the module runs on its own");
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::INPUT_UNSUPPLIED, "{warning:?}");
    assert_eq!(warning.field("predicate"), Some("edge"));
}

/// A check reads a parameter without a value as any value of its declared type,
/// and accepts the program.
#[test]
fn a_parameter_without_a_value_reads_as_any_value_of_its_type() {
    // The values are given when the program is loaded.
    let problems = check(
        "
        @parameter $limit: int .
        @parameter $start .
        @parameter $twice = $limit * 2 .
        size(a, 3) .
        big(?x) :- size(?x, ?n), ?n > $twice .
        reach($start) .
        reach(?y) :- reach(?x), size(?x, ?y) .
        @output big .
        @output reach .
    ",
    );
    assert!(problems.is_empty(), "{problems:?}");
}

/// For a check, an import that reads a parameter without a value makes its predicate an
/// input, and the other imports into that predicate are left out.
#[test]
fn an_import_reading_a_parameter_without_a_value_makes_its_predicate_an_input() {
    // A load given `$dir` reads both resources into `data`. The check instead reads `data`
    // as an input that may contain any value, so the second import is not an import into
    // an input.
    let problems = check(
        r#"
        @parameter $dir: string .
        @import data from csv { resource = f"{$dir}/a.csv" } .
        @import data from csv { resource = "b.csv" } .
        seen(?x) :- data(?x) .
        @output seen .
    "#,
    );
    assert!(problems.is_empty(), "{problems:?}");
}

/// A check accepts a parameter without a value as a group term and as an aggregated value.
#[test]
fn a_parameter_without_a_value_may_be_grouped_and_aggregated() {
    // A load given `$k` groups by that constant and sums it, which it accepts.
    let problems = check(
        "
        @parameter $k: int .
        p(1) .
        r($k, #sum($k)) :- p(?x) .
        @output r .
    ",
    );
    assert!(problems.is_empty(), "{problems:?}");
}

/// A parameter's declared type types the rules that read it,
/// and a warning about such a rule shows no hidden name.
#[test]
fn a_parameters_declared_type_types_the_rules_reading_it() {
    // No string is a number, so the rule never fires, whatever value
    // the parameter is given.
    let problems = check(
        "
        @parameter $name: string .
        size(a, 3) .
        named(?x) :- size(?x, ?n), ?n = $name .
        @output named .
    ",
    );
    let [warning] = &problems[..] else {
        panic!("expected the one warning: {problems:?}");
    };
    assert_eq!(warning.code(), codes::NEVER_FIRES, "{warning:?}");
    assert!(
        !format!("{warning:?}").contains("\\0"),
        "the hidden names stay hidden: {warning:?}"
    );
}

/// A check accepts an import and an export whose resources read a parameter without a value.
#[test]
fn an_import_reading_a_parameter_without_a_value_holds_anything() {
    // What the import reads depends on the value, so the check reads
    // nothing, and leaves out the export that the value names.
    let problems = check(
        "
        @parameter $file: string .
        @import data from csv { resource = $file } .
        copy(?x) :- data(?x) .
        @export copy to csv { resource = f\"{$file}.copy\" } .
    ",
    );
    assert!(problems.is_empty(), "{problems:?}");
}

/// A check reports the problem of a parameter once,
/// and reads the parameter as any value in the statements that read it.
#[test]
fn a_parameter_with_a_problem_is_reported_once_and_read_as_any_value() {
    let programs = [
        // The value is outside the declared type, and a rule reads the predicate of the
        // rule that reads the parameter.
        (
            "@parameter $limit: int = \"three\" .
             n(1) .
             small(?x) :- n(?x), ?x < $limit .
             big(?x) :- small(?x) .
             @output big .",
            codes::PARAMETER_OUTSIDE_TYPE,
        ),
        // The value of `$c` reads a parameter with a problem.
        (
            "@parameter $a: int = \"x\" .
             @parameter $c: int = $a + 1 .
             p($c) .",
            codes::PARAMETER_OUTSIDE_TYPE,
        ),
        // The value does not evaluate.
        (
            "@parameter $a = 1 / 0 .
             p($a) .
             q(?x) :- p(?x), ?x > $a .",
            codes::CONSTANT_EXPRESSION_ERRORS,
        ),
        // The value reads a parameter that is not declared.
        (
            "@parameter $a = $nope .
             p($a) .",
            codes::UNDEFINED_PARAMETER,
        ),
        // The parameters read each other.
        (
            "@parameter $a = $b .
             @parameter $b = $a .
             p($a) .",
            codes::PARAMETER_CYCLE,
        ),
        // The parameter is declared twice.
        (
            "@parameter $a = 1 .
             @parameter $a = 2 .
             p($a) .",
            codes::DUPLICATE_PARAMETER,
        ),
    ];
    let reported = programs.map(|(source, _)| {
        let problems = check(source);
        problems.iter().map(Diagnostic::code).collect::<Vec<_>>()
    });
    assert_eq!(reported, programs.map(|(_, code)| vec![code]));
}

/// A check reports the problem of every parameter that has one,
/// and the other parameters have their values.
#[test]
fn a_parameter_with_a_problem_leaves_the_others_as_they_are() {
    // `$b` has its value, although the value of `$a`, which is declared before it, is refused.
    let problems = check(
        "@parameter $a: int = \"x\" .
         @parameter $b: int = 1 .
         p($b) .",
    );
    let named: Vec<_> = problems
        .iter()
        .map(|problem| problem.field("name"))
        .collect();
    assert_eq!(named, [Some("a")], "{problems:?}");

    // Both values are outside their declared types.
    let problems = check(
        "@parameter $a: int = \"x\" .
         @parameter $b: string = 1 .
         p($a, $b) .",
    );
    let reported: Vec<_> = problems
        .iter()
        .map(|problem| (problem.code(), problem.field("name")))
        .collect();
    assert_eq!(
        reported,
        [
            (codes::PARAMETER_OUTSIDE_TYPE, Some("a")),
            (codes::PARAMETER_OUTSIDE_TYPE, Some("b"))
        ],
        "{problems:?}"
    );
}

/// A check reports a parameter's declared type that it refuses once,
/// and reads the parameter as any value.
#[test]
fn a_refused_type_of_a_parameter_is_reported_once() {
    // No `@type` defines `nosuch`.
    let problems = check(
        "@parameter $a: nosuch = 1 .
         p($a) .",
    );
    assert_eq!(problems.len(), 1, "{problems:?}");
}

/// Loading a program with a parameter without a value is an error.
#[test]
fn a_run_needs_a_value_for_every_parameter() {
    let (program, _) = Program::parse("@parameter $limit: int .\np(1) .");
    let problem = Engine::new()
        .load(program)
        .expect_err("the parameter has no value");
    assert_eq!(
        problem.code(),
        codes::PARAMETER_WITHOUT_VALUE,
        "{problem:?}"
    );
}

/// A predicate declared `@input` cannot also be imported.
#[test]
fn an_input_cannot_also_be_imported() {
    let (program, _) =
        Program::parse("@input p .\n@import p from csv { resource = \"data.csv\" } .");
    let problem = Engine::new()
        .load(program)
        .expect_err("an input is supplied from outside");
    assert_eq!(problem.code(), codes::IMPORT_INTO_INPUT, "{problem:?}");
    assert_eq!(problem.field("predicate"), Some("p"));
}

/// A check loads no program, so the engine still has nothing to reason over.
#[test]
fn a_checked_engine_holds_no_program() {
    let (program, _) = Program::parse("p(1) .");
    let mut engine = Engine::new();
    assert!(engine.check(program).diagnostics().is_empty());
    assert!(
        engine.reason().is_err(),
        "a check loads nothing to reason over"
    );
}

/// A check leaves no registered predicate behind,
/// and the engine loads a program afterwards,
/// even one that uses a checked predicate at another arity.
#[test]
fn a_check_leaves_no_registrations_behind() {
    let mut engine = Engine::new();
    let reports = engine.check(parse("p(1) . q(?x) :- p(?x) ."));
    assert!(reports.diagnostics().is_empty(), "{reports:?}");
    assert!(
        engine.predicates().is_empty(),
        "the check registered nothing"
    );
    engine
        .load(parse("p(1, 2) . q(?x, ?y) :- p(?x, ?y) ."))
        .expect("the program loads after a check");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "q"), 1);
}

/// A check returns its diagnostics in its report, and adds none to the engine's reports.
#[test]
fn a_check_leaves_no_reports_behind() {
    let mut engine = Engine::new();
    // The head of the rule has a variable that its body does not bind.
    let report = engine.check_source("test.rls", "q(?x, ?y) :- p(?x) .\n");
    assert!(!report.diagnostics().is_empty());
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());

    let report = engine.check(parse("q(?x, ?y) :- p(?x) ."));
    assert!(!report.diagnostics().is_empty());
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());
}
