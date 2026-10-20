//! Tracing: trees for named and for all derived facts, trace input files, and the explanation
//! view's queries.

use std::fs;

use crate::common::{ROLES, run, workspace, write};

#[test]
fn a_fact_named_and_derived_prints_once_under_trace_all() {
    let directory = workspace("trace-all-named");
    write(&directory, "program.rls", ROLES);

    let outcome = run(
        &directory,
        &[
            "program.rls",
            "-e",
            "none",
            "--trace-all",
            "--trace",
            "reach(1, 3)",
        ],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let roots = outcome
        .stdout
        .lines()
        .filter(|line| line.starts_with(" reach(1, 3) :- "))
        .count();
    assert_eq!(roots, 1, "{}", outcome.stdout);
}

#[test]
fn tracing_every_derived_fact_streams_a_tree_per_fact() {
    let directory = workspace("trace-all");
    write(&directory, "program.rls", ROLES);

    let outcome = run(&directory, &["program.rls", "-e", "none", "--trace-all"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    // A tree per derived fact (six roots, each on a line of its own
    // behind one space, a premise sitting behind a branch), and none
    // for the given rows of `edge`, which only ever appear as premises.
    let mut roots: Vec<&str> = outcome
        .stdout
        .lines()
        .filter(|line| {
            let mut characters = line.chars();
            characters.next() == Some(' ')
                && characters
                    .next()
                    .is_some_and(|next| !matches!(next, ' ' | '│' | '├' | '└'))
        })
        .map(|line| line[1..].split(" :- ").next().expect("a label"))
        .collect();
    roots.sort_unstable();
    assert_eq!(
        roots,
        [
            "both(0)",
            "both(1)",
            "both(2)",
            "reach(1, 2)",
            "reach(1, 3)",
            "reach(2, 3)",
        ],
        "{}",
        outcome.stdout
    );

    // The old spelling still works, and JSON lines go into the file.
    let outcome = run(
        &directory,
        &[
            "program.rls",
            "-e",
            "none",
            "--trace-all-idb-facts",
            "--trace-output",
            "trace.jsonl",
        ],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let written = fs::read_to_string(directory.join("trace.jsonl")).expect("the trace was written");
    let lines: Vec<serde_json::Value> = written
        .lines()
        .map(|line| serde_json::from_str(line).expect("one JSON object per line"))
        .collect();
    let mut conclusions: Vec<&str> = lines
        .iter()
        .map(|line| line["conclusion"].as_str().expect("a conclusion"))
        .collect();
    conclusions.sort_unstable();
    // Every derived fact once, and the given premises as asserted
    // inferences.
    assert_eq!(
        conclusions,
        [
            "both(0)",
            "both(1)",
            "both(2)",
            "edge(1, 2)",
            "edge(2, 3)",
            "reach(1, 2)",
            "reach(1, 3)",
            "reach(2, 3)",
        ]
    );
}

#[test]
fn trace_input_files_add_up() {
    let directory = workspace("trace-inputs");
    write(&directory, "program.rls", ROLES);
    write(&directory, "first.txt", "reach(1, 2)");
    write(&directory, "second.txt", "reach(1, 3); both(1)");

    let outcome = run(
        &directory,
        &[
            "program.rls",
            "-e",
            "none",
            "--trace-input",
            "first.txt",
            "--trace-input-file",
            "second.txt",
        ],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    for fact in ["reach(1, 2)", "reach(1, 3)", "both(1)"] {
        assert!(outcome.stdout.contains(fact), "{fact}: {}", outcome.stdout);
    }
}

#[test]
fn a_text_that_is_not_a_fact_says_how_to_separate_facts() {
    let directory = workspace("not-a-fact");
    write(&directory, "program.rls", ROLES);
    write(&directory, "lines.txt", "reach(1, 2)\nreach(1, 3)\n");

    // Facts on lines of their own, without `;`, read as one text.
    let outcome = run(
        &directory,
        &["program.rls", "-e", "none", "--trace-input", "lines.txt"],
    );
    assert_eq!(outcome.status, Some(1), "{}", outcome.stderr);
    assert!(
        outcome.stderr.contains("[N0905] Error"),
        "{}",
        outcome.stderr
    );
    assert!(
        outcome.stderr.contains("separate several facts with `;`"),
        "{}",
        outcome.stderr
    );

    // The terminator a fact may leave out is not what the parser found.
    let outcome = run(
        &directory,
        &["program.rls", "-e", "none", "--trace", "reach(1, "],
    );
    assert_eq!(outcome.status, Some(1), "{}", outcome.stderr);
    assert!(
        outcome.stderr.contains("found the end of the input"),
        "{}",
        outcome.stderr
    );
}

#[test]
fn trace_queries_follow_the_explanation_views_protocol() {
    let directory = workspace("trace-query");
    write(&directory, "program.rls", ROLES);
    write(
        &directory,
        "query.json",
        r#"{"predicate": "reach", "tableEntries": {"queries": ["1, 3"]}}"#,
    );

    // A tree query: the root's facts, expanded by the server.
    let outcome = run(
        &directory,
        &["program.rls", "-e", "none", "--trace-tree", "query.json"],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let response: serde_json::Value =
        serde_json::from_str(outcome.stdout.trim()).expect("the response is JSON");
    assert_eq!(response["predicate"], "reach", "{response}");
    assert_eq!(
        response["tableEntries"]["entries"][0]["termTuple"],
        serde_json::json!(["1", "3"]),
        "{response}"
    );
    assert!(
        response["childInformation"]["rule"]["id"].is_number(),
        "{response}"
    );

    // A node query: the shape given, the facts of every node, parents first.
    let outcome = run(
        &directory,
        &[
            "program.rls",
            "-e",
            "none",
            "--trace-node",
            r#"{"predicate": "reach", "childInformation": {"rule": 2, "headIndex": 0,
                "children": [{"tableEntries": {"queries": ["1, 2"]}}]}}"#,
        ],
    );
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let response: serde_json::Value =
        serde_json::from_str(outcome.stdout.trim()).expect("the response is JSON");
    let nodes = response.as_array().expect("a list of nodes");
    assert_eq!(nodes.len(), 2, "{response}");
    assert_eq!(nodes[1]["addressInTree"], serde_json::json!([0]));
    assert_eq!(
        nodes[0]["tableEntries"]["entries"][0]["termTuple"],
        serde_json::json!(["1", "2"]),
        "{response}"
    );

    // A file that is not a query is an error.
    let outcome = run(
        &directory,
        &["program.rls", "-e", "none", "--trace-tree", "program.rls"],
    );
    assert_eq!(outcome.status, Some(1), "{}", outcome.stdout);
    assert!(outcome.stderr.contains("[N0915]"), "{}", outcome.stderr);

    // Inline JSON is read as JSON, an array too, not as a file name.
    let outcome = run(
        &directory,
        &["program.rls", "-e", "none", "--trace-tree", "[1]"],
    );
    assert_eq!(outcome.status, Some(1), "{}", outcome.stdout);
    assert!(outcome.stderr.contains("[N0915]"), "{}", outcome.stderr);
    assert!(!outcome.stderr.contains("os error"), "{}", outcome.stderr);

    // The query cannot be combined with another trace, since both
    // would write the one `--trace-output`.
    let outcome = run(
        &directory,
        &[
            "program.rls",
            "-e",
            "none",
            "--trace",
            "reach(1, 3)",
            "--trace-tree",
            "query.json",
            "--trace-output",
            "trace.json",
        ],
    );
    assert_eq!(outcome.status, Some(2), "{}", outcome.stderr);
    assert!(
        outcome.stderr.contains("cannot be used with"),
        "{}",
        outcome.stderr
    );
    assert!(!directory.join("trace.json").exists());
    let outcome = run(
        &directory,
        &[
            "program.rls",
            "-e",
            "none",
            "--trace-all",
            "--trace-node",
            "query.json",
        ],
    );
    assert_eq!(outcome.status, Some(2), "{}", outcome.stderr);
    let outcome = run(
        &directory,
        &[
            "program.rls",
            "-e",
            "none",
            "--trace-tree",
            "query.json",
            "--trace-node",
            "query.json",
        ],
    );
    assert_eq!(outcome.status, Some(2), "{}", outcome.stderr);
}
