//! Errors, warnings, and exit codes: diagnostics name their source, codes explain themselves,
//! and warnings set the exit code unless silenced.

use std::fs;

use crate::common::{run, workspace, write};

#[test]
fn a_parse_error_points_at_its_source_and_fails() {
    let directory = workspace("parse");
    write(&directory, "broken.rls", "p(1, 2) .\nq(?x) :- p(?x, 3\n");

    let outcome = run(&directory, &["broken.rls", "-D", "out"]);
    assert_eq!(outcome.status, Some(1));
    assert!(
        outcome.stderr.contains("[N0202] Error"),
        "{}",
        outcome.stderr
    );
    assert!(
        outcome.stderr.contains("broken.rls:"),
        "the source is named: {}",
        outcome.stderr
    );
    assert!(
        !directory.join("out").exists(),
        "a program that does not parse writes nothing"
    );
}

#[test]
fn declared_columns_drop_rows_that_do_not_fit() {
    let directory = workspace("declare");
    write(
        &directory,
        "program.rls",
        "@declare p(int) .\n\
         @import p from csv { resource = \"data.csv\" } .\n\
         q(?x) :- p(?x) .\n\
         @export q to csv {} .\n",
    );
    write(&directory, "data.csv", "1\nabc\n2\n");

    let outcome = run(&directory, &["program.rls", "-D", "out"]);
    // The row outside the declared kind is dropped and warned about,
    // never an error, and a warned run says so in its exit code.
    assert_eq!(outcome.status, Some(3), "{}", outcome.stderr);
    let written =
        fs::read_to_string(directory.join("out").join("q.csv")).expect("the export was written");
    let mut rows: Vec<&str> = written.lines().collect();
    rows.sort_unstable();
    assert_eq!(rows, ["1", "2"]);
    assert!(
        outcome.stderr.contains("malformed"),
        "the dropped row is reported: {}",
        outcome.stderr
    );
}

#[test]
fn codes_explain_themselves() {
    let directory = workspace("explain");

    let outcome = run(&directory, &["--explain", "N0204"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(outcome.stdout.contains("N0204"), "{}", outcome.stdout);
    assert!(
        outcome.stdout.contains("UnsafeVariable"),
        "{}",
        outcome.stdout
    );
    assert!(outcome.stdout.contains("not bound"), "{}", outcome.stdout);

    // The command line's own codes explain themselves as well.
    let outcome = run(&directory, &["--explain", "N0909"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(
        outcome.stdout.contains("ExportExists"),
        "{}",
        outcome.stdout
    );
    assert!(
        outcome.stdout.contains("already exists"),
        "{}",
        outcome.stdout
    );

    // An unknown code is refused, and only a malformed one is told how
    // codes are written.
    let outcome = run(&directory, &["--explain", "N9999"]);
    assert_eq!(outcome.status, Some(1), "{}", outcome.stdout);
    assert!(outcome.stderr.contains("[N0914]"), "{}", outcome.stderr);
    assert!(!outcome.stderr.contains("Help:"), "{}", outcome.stderr);
    let outcome = run(&directory, &["--explain", "foo"]);
    assert_eq!(outcome.status, Some(1), "{}", outcome.stdout);
    assert!(
        outcome.stderr.contains("Help: write a code as `N`"),
        "{}",
        outcome.stderr
    );
}

#[test]
fn warnings_set_the_exit_code_unless_silenced() {
    let directory = workspace("warnings");
    write(
        &directory,
        "program.rls",
        "@declare p(int) .\n\
         @import p from csv { resource = \"data.csv\" } .\n\
         q(?x) :- p(?x) .\n\
         @output q .\n",
    );
    write(&directory, "data.csv", "1\nabc\n2\n");

    let outcome = run(&directory, &["program.rls", "-e", "none"]);
    assert_eq!(outcome.status, Some(3), "{}", outcome.stderr);
    assert!(outcome.stderr.contains("Warning"), "{}", outcome.stderr);

    let outcome = run(&directory, &["program.rls", "-e", "none", "--no-warnings"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(!outcome.stderr.contains("Warning"), "{}", outcome.stderr);
    assert!(outcome.stderr.contains("Finished in"), "{}", outcome.stderr);
}
