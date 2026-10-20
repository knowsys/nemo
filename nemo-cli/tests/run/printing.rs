//! Printing: facts in term syntax on the standard output instead of files, selected by what
//! feeds the predicates, and ended quietly when the reader closes the pipe.

use std::{path::Path, process::Stdio};

use crate::common::{Outcome, ROLES, command, run, workspace, write};

/// Runs the binary with a reader on its standard output that wants
/// nothing: the pipe is closed before the run writes a byte.
fn run_unread(directory: &Path, arguments: &[&str]) -> Outcome {
    let mut child = command(directory, arguments)
        .stdin(Stdio::null())
        .spawn()
        .expect("the binary runs");
    drop(child.stdout.take());
    let output = child.wait_with_output().expect("the binary finishes");
    Outcome {
        status: output.status.code(),
        stdout: String::new(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// A chain of the given number of edges under the transitive `reach`
/// rules: a program whose traces run long.
fn chain(edges: usize) -> String {
    let mut program = String::new();
    for from in 1..=edges {
        program.push_str(&format!("edge({from}, {}) .\n", from + 1));
    }
    program.push_str(
        "reach(?x, ?y) :- edge(?x, ?y) .\n\
         reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .\n\
         @output reach .\n",
    );
    program
}

#[test]
fn facts_printed_under_a_base_trace_as_printed() {
    // Under a base, a bare name is relative to the base,
    // so the relative IRIs `<b>` and `<edge>` print in angle brackets.
    let directory = workspace("print-base");
    write(
        &directory,
        "program.rls",
        "@base <http://example.org/> .\n\
         edge(a, <b>) .\n\
         <edge>(<b>, c) .\n\
         path(?x, ?y) :- edge(?x, ?y) .\n",
    );
    let printed = run(&directory, &["program.rls", "-P=all", "--report", "none"]);
    assert_eq!(printed.status, Some(0), "{}", printed.stderr);
    let facts: Vec<&str> = printed
        .stdout
        .lines()
        .map(|line| line.trim_end_matches(" ."))
        .collect();
    assert!(
        facts.contains(&"<edge>(<b>, <http://example.org/c>)"),
        "{facts:?}"
    );

    let traced = run(
        &directory,
        &[
            "program.rls",
            "-e",
            "none",
            "--report",
            "none",
            "--trace",
            &facts.join(";"),
        ],
    );
    assert_eq!(traced.status, Some(0), "{}", traced.stderr);
    assert!(!traced.stdout.contains("not derived"), "{}", traced.stdout);
}

#[test]
fn printing_writes_the_facts_to_standard_output_and_no_files() {
    let directory = workspace("print");
    write(
        &directory,
        "program.rls",
        "edge(1, 2) . edge(2, 3) .\n\
         reach(?x, ?y) :- edge(?x, ?y) .\n\
         reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .\n\
         @output reach .\n",
    );

    let outcome = run(&directory, &["program.rls", "--print", "-D", "out"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let mut facts: Vec<&str> = outcome.stdout.lines().collect();
    facts.sort_unstable();
    assert_eq!(facts, ["reach(1, 2) .", "reach(1, 3) .", "reach(2, 3) ."]);
    // Printing replaces the files, and the summary keeps to the error
    // stream, since stdout stays the data's.
    assert!(!directory.join("out").exists());
    assert!(
        outcome.stderr.contains("derived 3 facts"),
        "{}",
        outcome.stderr
    );
}

#[test]
fn printing_all_includes_what_the_program_loaded() {
    let directory = workspace("print-all");
    write(
        &directory,
        "program.rls",
        "edge(1, 2) .\nreach(?x, ?y) :- edge(?x, ?y) .\n",
    );

    let outcome = run(&directory, &["program.rls", "-P", "-e", "all"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let mut facts: Vec<&str> = outcome.stdout.lines().collect();
    facts.sort_unstable();
    assert_eq!(facts, ["edge(1, 2) .", "reach(1, 2) ."]);
}

#[test]
fn printing_selects_by_what_feeds_the_predicates() {
    let directory = workspace("print-roles");
    write(&directory, "program.rls", ROLES);

    let idb = run(&directory, &["program.rls", "--print=idb"]);
    assert_eq!(idb.status, Some(0), "{}", idb.stderr);
    let mut facts: Vec<&str> = idb.stdout.lines().collect();
    facts.sort_unstable();
    assert_eq!(
        facts,
        [
            "both(0) .",
            "both(1) .",
            "both(2) .",
            "reach(1, 2) .",
            "reach(1, 3) .",
            "reach(2, 3) .",
        ],
        "derived predicates, the mixed one included"
    );

    let edb = run(&directory, &["program.rls", "-P=edb"]);
    assert_eq!(edb.status, Some(0), "{}", edb.stderr);
    let mut facts: Vec<&str> = edb.stdout.lines().collect();
    facts.sort_unstable();
    assert_eq!(
        facts,
        [
            "both(0) .",
            "both(1) .",
            "both(2) .",
            "edge(1, 2) .",
            "edge(2, 3) ."
        ],
        "given predicates, the mixed one included"
    );
}

#[test]
fn a_closed_pipe_ends_every_print_quietly() {
    let directory = workspace("closed-pipe");
    write(&directory, "program.rls", &chain(40));

    // The reader has all it wants: no error, no message, whatever the
    // run was writing, since the traces stream far more than a pipe holds.
    let prints: [&[&str]; 5] = [
        &["program.rls", "-q", "-e", "none", "--trace-all"],
        &["program.rls", "-q", "-e", "none", "--trace", "reach(1, 41)"],
        &[
            "program.rls",
            "-q",
            "-e",
            "none",
            "--trace-tree",
            r#"{"predicate": "reach"}"#,
        ],
        &["program.rls", "-e", "none", "--report", "json"],
        &["program.rls", "-q", "-P"],
    ];
    for arguments in prints {
        let outcome = run_unread(&directory, arguments);
        assert_eq!(outcome.status, Some(0), "{arguments:?}: {}", outcome.stderr);
        assert!(
            outcome.stderr.is_empty(),
            "{arguments:?}: {}",
            outcome.stderr
        );
    }
}

#[test]
fn the_print_flag_takes_its_mode_only_with_an_equals_sign() {
    let directory = workspace("print-first");
    write(
        &directory,
        "program.rls",
        "p(1) .\nq(?x) :- p(?x) .\n@output q .\n",
    );

    // Bare, the flag leaves a following program path alone.
    let outcome = run(&directory, &["-P", "program.rls"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert_eq!(outcome.stdout, "q(1) .\n");
    let outcome = run(&directory, &["--print=edb", "program.rls"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert_eq!(outcome.stdout, "p(1) .\n");
    // A mode without the equals sign is a program path.
    let outcome = run(&directory, &["-P", "idb", "program.rls"]);
    assert_eq!(outcome.status, Some(1), "{}", outcome.stdout);
    assert!(outcome.stderr.contains("`idb`"), "{}", outcome.stderr);
}

#[test]
fn printing_skips_a_declared_predicate_the_program_never_used() {
    let directory = workspace("print-unused");
    write(&directory, "output.rls", "p(1) .\n@output p, q .\n");
    write(&directory, "export.rls", "p(1) .\n@export q to csv {} .\n");

    // As the export of such a predicate is an empty file, its printing
    // is nothing, not a failure after the work.
    let outcome = run(&directory, &["output.rls", "-P"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert_eq!(outcome.stdout, "p(1) .\n");
    let outcome = run(&directory, &["export.rls", "-P"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert_eq!(outcome.stdout, "");
}
