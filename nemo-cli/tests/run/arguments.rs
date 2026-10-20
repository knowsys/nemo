//! What the command line hands the program: parameter values and module roots, and the older
//! spellings of its flags.

use std::fs;

use crate::common::{ROLES, run, workspace, write};

#[test]
fn a_parameter_takes_the_value_the_command_line_gives_it() {
    let directory = workspace("parameter");
    write(
        &directory,
        "program.rls",
        "@parameter $limit .\n\
         n(1) . n(2) . n(3) . n(4) .\n\
         small(?x) :- n(?x), ?x < $limit .\n\
         @export small to csv {} .\n",
    );

    // Without a value the declaration is unfulfilled, and the run says so.
    let missing = run(&directory, &["program.rls", "-D", "out"]);
    assert_eq!(missing.status, Some(1));
    assert!(missing.stderr.contains("$limit"), "{}", missing.stderr);

    let supplied = run(&directory, &["program.rls", "-D", "out", "-p", "limit=3"]);
    assert_eq!(supplied.status, Some(0), "{}", supplied.stderr);
    let written = fs::read_to_string(directory.join("out").join("small.csv"))
        .expect("the export was written");
    let mut rows: Vec<&str> = written.lines().collect();
    rows.sort_unstable();
    assert_eq!(rows, ["1", "2"]);
}

#[test]
fn a_malformed_parameter_setting_is_reported_rather_than_fatal() {
    let directory = workspace("malformed-param");
    write(&directory, "program.rls", "@parameter $limit .\np(1) .\n");

    // `%` starts a comment and `.` ends a statement, so this setting
    // lexes as a valueless declaration: a diagnostic, never a panic.
    // A setting without a name, or with a value of more than one term,
    // is malformed too.
    for setting in [
        "limit.%=",
        "limit . %=1",
        "nonsense",
        "=3",
        "$=3",
        "limit=1 . p(2)",
    ] {
        let outcome = run(&directory, &["program.rls", "-p", setting]);
        assert_eq!(
            outcome.status,
            Some(1),
            "`{setting}` exited unexpectedly: {}",
            outcome.stderr
        );
        assert!(
            outcome.stderr.contains("[N0900] Error"),
            "`{setting}`: {}",
            outcome.stderr
        );
    }

    // A name with room around it is still that name, and so is a name
    // written with its `$`.
    for setting in ["limit =3", "$limit=3"] {
        let outcome = run(&directory, &["program.rls", "-p", setting, "-e", "none"]);
        assert_eq!(outcome.status, Some(0), "`{setting}`: {}", outcome.stderr);
    }
}

#[test]
fn an_option_value_read_as_a_program_says_how_to_write_it() {
    let directory = workspace("option-value");
    write(&directory, "program.rls", ROLES);

    // `-P` takes its mode only after `=`, so `idb` is a second program.
    let outcome = run(&directory, &["program.rls", "-P", "idb"]);
    assert_eq!(outcome.status, Some(1), "{}", outcome.stderr);
    assert!(
        outcome.stderr.contains("write an option's value with `=`"),
        "{}",
        outcome.stderr
    );

    // A program that is simply missing gets no such advice.
    let outcome = run(&directory, &["missing.rls"]);
    assert_eq!(outcome.status, Some(1), "{}", outcome.stderr);
    assert!(
        !outcome.stderr.contains("write an option's value"),
        "{}",
        outcome.stderr
    );
}

#[test]
fn old_spellings_of_the_flags_still_work() {
    let directory = workspace("spellings");
    write(&directory, "program.rls", ROLES);

    let outcome = run(&directory, &["program.rls", "--print-facts=idb"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(
        outcome.stdout.contains("reach(1, 3) ."),
        "{}",
        outcome.stdout
    );
    // `auto` keeps the terminal to the printed data.
    let outcome = run(
        &directory,
        &["program.rls", "--print-facts=idb", "--report", "auto"],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(
        !outcome.stderr.contains("Finished in"),
        "{}",
        outcome.stderr
    );
    let outcome = run(
        &directory,
        &["program.rls", "-e", "none", "--report", "auto"],
    );
    assert!(outcome.stderr.contains("Finished in"), "{}", outcome.stderr);

    let outcome = run(&directory, &["program.rls", "-D", "out", "-e", "idb"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let outcome = run(
        &directory,
        &[
            "program.rls",
            "-D",
            "out",
            "-e",
            "idb",
            "--overwrite-results",
        ],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);

    // The query given inline, in the format old `--trace-tree` read
    // from a file.
    let outcome = run(
        &directory,
        &[
            "program.rls",
            "-e",
            "none",
            "--trace-tree",
            r#"{"predicate": "reach", "tableEntries": {"queries": ["1, 3"]}}"#,
        ],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let response: serde_json::Value =
        serde_json::from_str(outcome.stdout.trim()).expect("the response is JSON");
    assert_eq!(response["predicate"], "reach", "{response}");
    assert_eq!(
        response["tableEntries"]["entries"].as_array().map(Vec::len),
        Some(1),
        "{response}"
    );
}

#[test]
fn a_module_root_names_modules_in_its_directory() {
    let directory = workspace("module-root");
    fs::create_dir_all(directory.join("lib")).expect("the workspace is writable");
    write(
        &directory.join("lib"),
        "closure.rls",
        "@input edge .\n@output reach .\n\
         reach(?x, ?y) :- edge(?x, ?y) .\n\
         reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .\n",
    );
    write(
        &directory,
        "program.rls",
        "link(1, 2) . link(2, 3) .\n\
         @use shared::closure { edge = link } as c .\n\
         path(?x, ?y) :- c::reach(?x, ?y) .\n\
         @output path .\n",
    );

    let outcome = run(
        &directory,
        &["program.rls", "--module-root", "shared=lib", "-P"],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(outcome.stdout.contains("path(1, 3)"), "{}", outcome.stdout);

    // Without the root, `shared` is a directory next to the program.
    let unrooted = run(&directory, &["program.rls", "-P"]);
    assert_eq!(unrooted.status, Some(1));
    assert!(
        unrooted.stderr.contains("[N0411] Error"),
        "{}",
        unrooted.stderr
    );

    for (setting, code) in [
        ("shared", "[N0917] Error"),
        ("=lib", "[N0917] Error"),
        (" =lib", "[N0917] Error"),
        ("a::b=lib", "[N0422] Error"),
    ] {
        let outcome = run(&directory, &["program.rls", "--module-root", setting]);
        assert_eq!(outcome.status, Some(1), "`{setting}`: {}", outcome.stderr);
        assert!(
            outcome.stderr.contains(code),
            "`{setting}`: {}",
            outcome.stderr
        );
    }
}
