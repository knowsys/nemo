//! Checking a program without loading it, and the inputs of a module run
//! on its own.

use super::*;

#[test]
fn checking_collects_every_problem() {
    // Two independent mistakes; a load would stop at the first.
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

#[test]
fn checking_reads_no_import_data() {
    // The resource does not exist; a check validates the directive's
    // shape and never notices.
    let problems = check(
        "
        @import p from csv { resource = \"nowhere.csv\", format = (int, int) } .
        q(?x) :- p(?x, ?x) .
    ",
    );
    assert!(problems.is_empty(), "{problems:?}");
}

#[test]
fn checking_still_reports_a_malformed_import() {
    let problems = check(
        "
        @import p from csv { nonsense = 1, resource = \"data.csv\" } .
    ",
    );
    assert_eq!(problems.len(), 1, "{problems:?}");
}

#[test]
fn a_modules_inputs_are_open_predicates() {
    // The input `edge` is supplied by whoever uses the module; a
    // check of the file on its own must not call its rules dead.
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

#[test]
fn a_typed_input_types_the_module_from_within() {
    // The input's columns declare it, so the analysis can prove a
    // rule dead even though the data arrives from outside.
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

#[test]
fn an_unsupplied_input_is_warned_about_at_load_only() {
    let source = "@input edge .\nreach(?x, ?y) :- edge(?x, ?y) .\n@output reach .";
    let (program, _) = Program::parse(source, FileId::new(0));
    let mut engine = Engine::new();
    engine.load(program).expect("the module runs on its own");
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::INPUT_UNSUPPLIED, "{warning:?}");
    assert_eq!(warning.field("predicate"), Some("edge"));
}

#[test]
fn a_parameter_without_a_value_reads_as_any_value_of_its_type() {
    // The values are supplied when the program runs; a check of the
    // file on its own must not refuse it.
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

#[test]
fn an_import_reading_a_parameter_without_a_value_holds_anything() {
    // What the import reads depends on the value, so the check reads
    // nothing, and leaves out the export the value names.
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

#[test]
fn a_run_needs_a_value_for_every_parameter() {
    let (program, _) = Program::parse("@parameter $limit: int .\np(1) .", FileId::new(0));
    let problem = Engine::new()
        .load(program)
        .expect_err("the parameter has no value");
    assert_eq!(
        problem.code(),
        codes::PARAMETER_WITHOUT_VALUE,
        "{problem:?}"
    );
}

#[test]
fn an_input_cannot_also_be_imported() {
    let (program, _) = Program::parse(
        "@input p .\n@import p from csv { resource = \"data.csv\" } .",
        FileId::new(0),
    );
    let problem = Engine::new()
        .load(program)
        .expect_err("an input is supplied from outside");
    assert_eq!(problem.code(), codes::IMPORT_INTO_INPUT, "{problem:?}");
    assert_eq!(problem.field("predicate"), Some("p"));
}

#[test]
fn a_checked_engine_holds_no_program() {
    let (program, _) = Program::parse("p(1) .", FileId::new(0));
    let mut engine = Engine::new();
    assert!(engine.check(program).diagnostics().is_empty());
    assert!(
        engine.reason().is_err(),
        "a check loads nothing to reason over"
    );
}

/// A check leaves the engine as it found it:
/// nothing it registered shows, and the engine loads afterwards,
/// even a program using a checked predicate at another arity.
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
