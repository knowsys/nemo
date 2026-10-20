//! Reports: the summary of the declared outputs, the JSON and full reports, logging, and the
//! event log.

use std::fs;

use crate::common::{ROLES, run, workspace, write};

#[test]
fn the_summary_reports_the_declared_outputs() {
    let directory = workspace("summary");
    write(
        &directory,
        "program.rls",
        "edge(1, 2) . edge(2, 3) .\n\
         reach(?x, ?y) :- edge(?x, ?y) .\n\
         reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .\n\
         @output reach .\n",
    );

    let outcome = run(&directory, &["program.rls", "-e", "none"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    // The headline counts what reasoning added: the three `reach`
    // facts, not the loaded `edge` rows.
    assert!(
        outcome.stderr.contains("derived 3 facts"),
        "{}",
        outcome.stderr
    );
    assert!(outcome.stderr.contains("reach "), "{}", outcome.stderr);
    assert!(
        !outcome.stderr.contains("edge "),
        "only the declared outputs are listed: {}",
        outcome.stderr
    );
}

#[test]
fn the_json_report_owns_the_standard_output() {
    let directory = workspace("print-json");
    write(
        &directory,
        "program.rls",
        "p(1) .\nq(?x) :- p(?x) .\n@output q .\n",
    );
    write(
        &directory,
        "piped.rls",
        "p(1) .\n@export p to csv { resource = \"\" } .\n",
    );

    // Nothing else may write there: not the printed results, not a
    // trace printed rather than written, not an export to the standard
    // output. The run says so before doing any work.
    let claims: [&[&str]; 5] = [
        &["program.rls", "--print", "--report", "json"],
        &["program.rls", "--trace", "q(1)", "--report", "json"],
        &["program.rls", "--trace-all", "--report", "json"],
        &[
            "program.rls",
            "--trace-tree",
            r#"{"predicate": "q"}"#,
            "--report",
            "json",
        ],
        &["piped.rls", "--report", "json"],
    ];
    for arguments in claims {
        let outcome = run(&directory, arguments);
        assert_eq!(outcome.status, Some(1), "{arguments:?}: {}", outcome.stdout);
        assert!(
            outcome.stderr.contains("[N0901]"),
            "{arguments:?}: {}",
            outcome.stderr
        );
        assert!(
            outcome.stdout.is_empty(),
            "{arguments:?}: {}",
            outcome.stdout
        );
    }

    // A trace written to its file leaves the document alone.
    let outcome = run(
        &directory,
        &[
            "program.rls",
            "-e",
            "none",
            "--trace",
            "q(1)",
            "--trace-output",
            "trace.json",
            "--report",
            "json",
        ],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let document: serde_json::Value =
        serde_json::from_str(&outcome.stdout).expect("one JSON document and nothing else");
    assert_eq!(document["run"]["derived"], 1, "{document}");
}

#[test]
fn verbosity_logs_the_run_to_the_error_stream() {
    let directory = workspace("verbose");
    write(
        &directory,
        "program.rls",
        "p(1) .\nq(?x) :- p(?x) .\n@output q .\n",
    );

    let quiet = run(&directory, &["program.rls", "-e", "none"]);
    assert_eq!(quiet.status, Some(0), "{}", quiet.stderr);
    assert!(
        !quiet.stderr.contains("loading a program"),
        "the default level logs nothing: {}",
        quiet.stderr
    );

    let verbose = run(&directory, &["program.rls", "-e", "none", "-vv"]);
    assert_eq!(verbose.status, Some(0), "{}", verbose.stderr);
    assert!(
        verbose.stderr.contains("loading a program"),
        "{}",
        verbose.stderr
    );
    assert!(
        verbose.stderr.contains("stratum started"),
        "{}",
        verbose.stderr
    );

    // A filter selects by module: the engine's staging, nothing else.
    let filtered = run(
        &directory,
        &[
            "program.rls",
            "-e",
            "none",
            "--log",
            "nemo::engine::staging=debug",
        ],
    );
    assert_eq!(filtered.status, Some(0), "{}", filtered.stderr);
    assert!(
        filtered.stderr.contains("program loaded"),
        "{}",
        filtered.stderr
    );
    assert!(
        !filtered.stderr.contains("stratum started"),
        "{}",
        filtered.stderr
    );

    let broken = run(
        &directory,
        &["program.rls", "-e", "none", "--log", "nemo::=[["],
    );
    assert_eq!(broken.status, Some(1), "{}", broken.stderr);
    assert!(broken.stderr.contains("[N0912] Error"), "{}", broken.stderr);
}

#[test]
fn the_event_log_holds_the_telemetry_stream_as_json_lines() {
    let directory = workspace("events");
    write(
        &directory,
        "program.rls",
        "p(1) .\nq(?x) :- p(?x) .\n@output q .\n",
    );

    let outcome = run(
        &directory,
        &["program.rls", "-e", "none", "--events", "events.jsonl"],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let log = fs::read_to_string(directory.join("events.jsonl")).expect("the log was written");
    let kinds: Vec<String> = log
        .lines()
        .map(|line| {
            let event: serde_json::Value =
                serde_json::from_str(line).expect("one JSON object per line");
            event["kind"]
                .as_str()
                .expect("every event names its kind")
                .to_owned()
        })
        .collect();
    assert!(kinds.contains(&String::from("rule_applied")), "{kinds:?}");
    assert_eq!(
        kinds.last().map(String::as_str),
        Some("finished"),
        "{kinds:?}"
    );
}

#[test]
fn the_full_report_lists_rules_by_time() {
    let directory = workspace("timing");
    write(
        &directory,
        "program.rls",
        "p(1) .\nq(?x) :- p(?x) .\n@output q .\n",
    );

    let outcome = run(
        &directory,
        &["program.rls", "-e", "none", "--report", "all"],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(
        outcome.stderr.contains("derived   dropped  rule"),
        "{}",
        outcome.stderr
    );
    assert!(
        outcome.stderr.contains("q(?x) :- p(?x) ."),
        "{}",
        outcome.stderr
    );
}

#[test]
fn old_report_spellings_are_accepted() {
    let directory = workspace("report-spellings");
    write(&directory, "program.rls", ROLES);

    for spelling in ["short", "auto"] {
        let outcome = run(
            &directory,
            &["program.rls", "-e", "none", "--report", spelling],
        );
        assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
        assert!(outcome.stderr.contains("Finished in"), "{}", outcome.stderr);
        assert!(
            !outcome.stderr.contains("parse "),
            "{spelling} is the summary: {}",
            outcome.stderr
        );
    }
    for spelling in ["time", "mem"] {
        let outcome = run(
            &directory,
            &["program.rls", "-e", "none", "--report", spelling],
        );
        assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
        assert!(
            outcome.stderr.contains("parse "),
            "{spelling} is the timing report: {}",
            outcome.stderr
        );
    }
}

#[test]
fn a_program_declaring_no_outputs_has_every_derived_predicate_as_one() {
    let directory = workspace("no-outputs");
    write(
        &directory,
        "program.rls",
        "edge(1, 2) . edge(2, 3) .\n\
         reach(?x, ?y) :- edge(?x, ?y) .\n\
         reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .\n",
    );

    // The summary counts every derived predicate.
    let outcome = run(&directory, &["program.rls", "-e", "none"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(!outcome.stderr.contains("edge "), "{}", outcome.stderr);
    assert!(outcome.stderr.contains("reach "), "{}", outcome.stderr);

    // A bare `--print` prints every derived predicate.
    let outcome = run(&directory, &["program.rls", "--print"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let mut facts: Vec<&str> = outcome.stdout.lines().collect();
    facts.sort_unstable();
    assert_eq!(facts, ["reach(1, 2) .", "reach(1, 3) .", "reach(2, 3) ."]);
}
