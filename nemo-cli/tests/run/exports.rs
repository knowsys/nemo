//! Exports: the files a run writes, the predicates it writes them for, and the guards against
//! replacing an earlier run's files and colliding ones.

use std::{fs, io::Read, process::Stdio};

use crate::common::{ROLES, command, run, workspace, write};

#[test]
fn a_run_writes_what_the_program_exports() {
    let directory = workspace("exports");
    write(
        &directory,
        "program.rls",
        "edge(1, 2) . edge(2, 3) .\n\
         reach(?x, ?y) :- edge(?x, ?y) .\n\
         reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .\n\
         @export reach to csv {} .\n",
    );

    let outcome = run(&directory, &["program.rls", "-D", "out"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);

    let written = fs::read_to_string(directory.join("out").join("reach.csv"))
        .expect("the export was written");
    let mut rows: Vec<&str> = written.lines().collect();
    rows.sort_unstable();
    assert_eq!(rows, ["1,2", "1,3", "2,3"]);
    // The default report names the exported predicate, on the error
    // stream, since stdout stays the data's.
    assert!(outcome.stderr.contains("reach "), "{}", outcome.stderr);
    assert!(
        !outcome.stderr.contains("edge "),
        "a predicate the program is not about stays out of the report: {}",
        outcome.stderr
    );
}

#[test]
fn an_export_is_kept_until_the_run_is_told_to_replace_it() {
    let directory = workspace("overwrite");
    write(&directory, "program.rls", "p(1) .\n@export p to csv {} .\n");

    assert_eq!(
        run(&directory, &["program.rls", "-D", "out"]).status,
        Some(0)
    );

    let again = run(&directory, &["program.rls", "-D", "out"]);
    assert_eq!(again.status, Some(1));
    assert!(again.stderr.contains("[N0909] Error"), "{}", again.stderr);

    let forced = run(&directory, &["program.rls", "-D", "out", "--overwrite"]);
    assert_eq!(forced.status, Some(0), "{}", forced.stderr);
}

#[test]
fn two_exports_cannot_be_written_to_one_file() {
    let directory = workspace("collision");
    write(
        &directory,
        "program.rls",
        "p(1) . q(2) .\n\
         @export p to csv { resource = \"same.csv\" } .\n\
         @export q to csv { resource = \"same.csv\" } .\n",
    );

    let outcome = run(&directory, &["program.rls", "-D", "out"]);
    assert_eq!(outcome.status, Some(1), "{}", outcome.stdout);
    assert!(
        outcome
            .stderr
            .contains("[N0908] Error: `p` and `q` would both be written to `same.csv`"),
        "{}",
        outcome.stderr
    );
    // --overwrite permits replacing an earlier run's file, not
    // discarding one of this run's own exports.
    let forced = run(&directory, &["program.rls", "-D", "out", "--overwrite"]);
    assert_eq!(forced.status, Some(1), "{}", forced.stdout);

    // Another spelling of the same file collides too.
    write(
        &directory,
        "spelled.rls",
        "p(1) . q(2) .\n\
         @export p to csv { resource = \"same.csv\" } .\n\
         @export q to csv { resource = \"./sub/../same.csv\" } .\n",
    );
    let outcome = run(&directory, &["spelled.rls", "-D", "out"]);
    assert!(
        outcome.stderr.contains("[N0908] Error"),
        "{}",
        outcome.stderr
    );

    // One predicate exported twice into one file is named once.
    write(
        &directory,
        "twice.rls",
        "p(1) .\n\
         @export p to csv { resource = \"same.csv\" } .\n\
         @export p to csv { resource = \"same.csv\" } .\n",
    );
    let outcome = run(&directory, &["twice.rls", "-D", "out"]);
    assert!(
        outcome
            .stderr
            .contains("[N0908] Error: two exports of `p` would both be written to `same.csv`"),
        "{}",
        outcome.stderr
    );

    // Where the file system ignores case, so does the check.
    if cfg!(any(windows, target_os = "macos")) {
        write(&directory, "cased.rls", "a(1) . A(2) .\n");
        let outcome = run(&directory, &["cased.rls", "-D", "out", "-e", "all"]);
        assert!(
            outcome.stderr.contains("[N0908] Error"),
            "{}",
            outcome.stderr
        );
    }
}

#[test]
fn gzipping_leaves_a_resource_that_is_already_gzipped_alone() {
    let directory = workspace("gzip");
    write(
        &directory,
        "program.rls",
        "p(1) .\n@export p to csv { resource = \"mine.csv.gz\" } .\n",
    );

    let outcome = run(&directory, &["program.rls", "-D", "out", "--gzip"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(directory.join("out").join("mine.csv.gz").exists());
    assert!(
        !directory.join("out").join("mine.csv.gz.gz").exists(),
        "a `.gz` resource is already written through gzip"
    );
}

#[test]
fn every_predicate_is_written_when_the_run_is_asked_for_all() {
    let directory = workspace("all");
    write(
        &directory,
        "program.rls",
        "edge(1, 2) .\nreach(?x, ?y) :- edge(?x, ?y) .\n",
    );

    let outcome = run(&directory, &["program.rls", "-D", "out", "-e", "all"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(directory.join("out").join("edge.csv").exists());
    assert!(directory.join("out").join("reach.csv").exists());
}

#[test]
fn exports_select_by_what_feeds_the_predicates() {
    let directory = workspace("export-roles");
    write(&directory, "program.rls", ROLES);

    let outcome = run(&directory, &["program.rls", "-D", "idb", "-e", "idb"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(directory.join("idb").join("reach.csv").exists());
    assert!(directory.join("idb").join("both.csv").exists());
    assert!(!directory.join("idb").join("edge.csv").exists());

    let outcome = run(&directory, &["program.rls", "-D", "edb", "-e", "edb"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(directory.join("edb").join("edge.csv").exists());
    assert!(directory.join("edb").join("both.csv").exists());
    assert!(!directory.join("edb").join("reach.csv").exists());
}

#[test]
fn the_overwrite_guard_runs_before_reasoning_in_every_mode() {
    let directory = workspace("guard");
    write(
        &directory,
        "program.rls",
        &format!("{ROLES}@export reach to csv {{}} .\n"),
    );

    for mode in ["keep", "idb", "edb", "all"] {
        let arguments = ["program.rls", "-D", mode, "-e", mode, "--log", "info"];
        let first = run(&directory, &arguments);
        assert_eq!(first.status, Some(0), "{mode}: {}", first.stderr);
        assert!(
            first.stderr.contains("reasoning"),
            "{mode}: {}",
            first.stderr
        );
        // The second run fails before it reasons.
        let again = run(&directory, &arguments);
        assert_eq!(again.status, Some(1), "{mode}: {}", again.stderr);
        assert!(again.stderr.contains("[N0909]"), "{mode}: {}", again.stderr);
        assert!(
            !again.stderr.contains("reasoning"),
            "{mode}: {}",
            again.stderr
        );
    }
}

#[test]
fn the_html_report_is_guarded_like_an_export() {
    let directory = workspace("html-report");
    write(
        &directory,
        "claimed.rls",
        "p(1) .\n@export p to csv { resource = \"report.html\" } .\n",
    );
    write(&directory, "program.rls", "p(1) .\n@output p .\n");

    // An export into the report's file would be replaced by the report.
    let outcome = run(
        &directory,
        &["claimed.rls", "-D", "out", "--report", "html"],
    );
    assert_eq!(outcome.status, Some(1), "{}", outcome.stderr);
    assert!(
        outcome.stderr.contains(
            "[N0908] Error: `p` and the HTML report would both be written to `report.html`"
        ),
        "{}",
        outcome.stderr
    );
    assert!(!directory.join("out").join("report.html").exists());

    // An earlier report is kept until the run is told to replace it.
    let arguments = ["program.rls", "-D", "out", "--report", "html"];
    let first = run(&directory, &arguments);
    assert_eq!(first.status, Some(0), "{}", first.stderr);
    assert!(directory.join("out").join("report.html").exists());
    let again = run(&directory, &arguments);
    assert_eq!(again.status, Some(1), "{}", again.stderr);
    assert!(again.stderr.contains("[N0909] Error"), "{}", again.stderr);
    let mut overwriting = arguments.to_vec();
    overwriting.push("--overwrite");
    let forced = run(&directory, &overwriting);
    assert_eq!(forced.status, Some(0), "{}", forced.stderr);
}

#[test]
fn an_export_to_a_closed_standard_output_ends_quietly() {
    let directory = workspace("closed-stdout");
    // More rows than a pipe holds, so the export is still writing when
    // the reader leaves.
    let rows: String = (0..300_000).map(|row| format!("{row}\n")).collect();
    write(&directory, "data.csv", &rows);
    write(
        &directory,
        "program.rls",
        "@import p from csv { resource = \"data.csv\" } .\n\
         @export p to csv { resource = \"\" } .\n",
    );

    let mut child = command(&directory, &["program.rls", "-q"])
        .stdin(Stdio::null())
        .spawn()
        .expect("the binary runs");
    let mut stdout = child.stdout.take().expect("stdout is piped");
    let mut first = [0; 1];
    stdout.read_exact(&mut first).expect("the export starts");
    drop(stdout);
    let output = child.wait_with_output().expect("the binary finishes");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "{stderr}");
    assert!(!stderr.contains("N0104"), "{stderr}");
}
