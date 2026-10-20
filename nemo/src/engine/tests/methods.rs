//! The engine's methods around reasoning: predicate listings, cursors,
//! cancellation, telemetry, exports, and the errors of an engine that has not
//! reached its fixpoint.

use super::*;

/// A predicate's role follows from where its rows come from: rules, input facts, or both.
/// A predicate only ever read is given, holding nothing.
#[test]
fn a_predicates_role_follows_where_its_rows_come_from() {
    let mut engine = Engine::new();
    engine
        .import_rows("imported", [vec![DataValue::integer(1)]])
        .expect("rows register before the load");
    let program = parse(
        "edge(1, 2) .
         reach(?x, ?y) :- edge(?x, ?y) .
         both(0) .
         both(?x) :- edge(?x, _) .
         @input given .
         uses(?x) :- given(?x) .
         lonely(?x) :- reach(?x, ?y), ~unseen(?y) .
         @declare unseen(int) .",
    );
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    let roles = roles(&engine);
    let role = |name: &str| {
        roles
            .iter()
            .find(|(predicate, _)| predicate == name)
            .map(|(_, role)| *role)
            .unwrap_or_else(|| panic!("{name} has a role: {roles:?}"))
    };
    assert_eq!(role("edge"), PredicateRole::Extensional);
    assert_eq!(role("reach"), PredicateRole::Derived);
    assert_eq!(role("both"), PredicateRole::Both);
    assert_eq!(
        role("given"),
        PredicateRole::Extensional,
        "an input is given"
    );
    assert_eq!(
        role("imported"),
        PredicateRole::Extensional,
        "rows added through `import_rows` are given"
    );
    assert_eq!(role("lonely"), PredicateRole::Derived);
    assert_eq!(role("unseen"), PredicateRole::Extensional, "only ever read");
    assert!(
        roles.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "sorted by name: {roles:?}"
    );
}

#[test]
fn cancellation_stops_at_a_boundary_and_resumes() {
    let mut source = String::new();
    for index in 0..60 {
        source += &format!("edge({index}, {}) . ", index + 1);
    }
    source += "path(?x, ?y) :- edge(?x, ?y) . path(?x, ?z) :- edge(?x, ?y), path(?y, ?z) .";
    let load = || {
        let program = parse(&source);
        let mut engine = Engine::new();
        engine.load(program).expect("the program loads");
        engine
    };

    // A stale cancellation is cleared when the run starts.
    let mut engine = load();
    engine.cancel_token().cancel();
    engine
        .reason()
        .expect("a stale cancellation does not stop a fresh run");
    assert_eq!(count(&mut engine, "path"), 61 * 60 / 2);

    // A concurrent cancellation lands at a rule boundary or after the
    // fixpoint. Either way the engine ends at the same fixpoint,
    // resuming if it was cancelled. (Moving the engine into the thread
    // is also the compile-time proof that it is `Send`.)
    let mut engine = load();
    let token = engine.cancel_token();
    let handle = std::thread::spawn(move || {
        let outcome = engine.reason();
        (engine, outcome)
    });
    token.cancel();
    let (mut engine, outcome) = handle.join().expect("the run does not panic");
    if let Err(report) = outcome {
        assert_eq!(report.code(), codes::CANCELLED, "{report}");
        engine.reason().expect("resuming reaches the fixpoint");
    }
    assert_eq!(count(&mut engine, "path"), 61 * 60 / 2);
}

#[test]
fn cursors_page_through_the_rows() {
    let mut engine = run("
        edge(1, 2) . edge(2, 3) . edge(3, 4) . edge(4, 5) .
        path(?x, ?y) :- edge(?x, ?y) .
        path(?x, ?z) :- edge(?x, ?y), path(?y, ?z) .
    ");
    let all = engine.rows("path").expect("the predicate exists");
    assert_eq!(all.len(), 10);

    let mut cursor = engine.cursor("path").expect("the predicate exists");
    assert_eq!(cursor.remaining(), 10);
    let mut paged = Vec::new();
    let mut reads = 0;
    while !cursor.is_done() {
        let chunk = engine
            .read_chunk(&mut cursor, 3)
            .expect("the read succeeds");
        assert!(!chunk.is_empty() && chunk.len() <= 3);
        assert_eq!(chunk.arity(), 2);
        assert_eq!(chunk.column(0).len(), chunk.len());
        paged.extend(chunk.rows());
        reads += 1;
    }
    assert_eq!(reads, all.len().div_ceil(3));
    assert_eq!(paged, all);

    // An exhausted cursor keeps reading empty chunks, never errors.
    assert!(
        engine
            .read_chunk(&mut cursor, 3)
            .expect("an exhausted cursor is not an error")
            .is_empty()
    );
}

#[test]
fn telemetry_events_account_for_the_run() {
    use crate::telemetry::Recorder;

    let program = parse(
        "edge(1, 2) . edge(2, 3) . edge(3, 4) .
         reach(?x, ?y) :- edge(?x, ?y) .
         reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .",
    );
    let mut engine = Engine::new();
    let recorder = Arc::new(Recorder::new());
    engine.set_telemetry(recorder.clone());
    engine.load(program).expect("the program loads");
    recorder.set_rules(engine.ran_rules().to_vec());
    engine.reason().expect("reasoning reaches the fixpoint");

    assert_eq!(engine.ran_rules().len(), 2);
    assert!(engine.ran_rules()[0].contains("edge"));
    let snapshot = recorder.snapshot_json();
    assert!(snapshot.contains("\"finished\":true"), "{snapshot}");
    assert!(snapshot.contains("\"fixpoint\":true"), "{snapshot}");
    assert!(
        snapshot.contains(&format!("\"derived\":{}", engine.derived_facts())),
        "the stream's total matches the engine's: {snapshot}"
    );
    assert!(snapshot.contains("reach(?x, ?y)"), "{snapshot}");
}

#[test]
fn a_gzipped_export_says_so_in_its_name() {
    use nemo_io::compression::Compression;

    let specification = |resource: Option<&str>, compression| {
        let mut specification = ExportSpec::csv("p");
        specification.resource = resource.map(str::to_owned);
        specification.compression = compression;
        specification
    };
    // Otherwise nothing that detects compression by name, this
    // engine's own reader included, can read the file back.
    assert_eq!(
        specification(None, Compression::Gzip).file_name(),
        "p.csv.gz"
    );
    assert_eq!(
        specification(Some("mine.csv"), Compression::Gzip).file_name(),
        "mine.csv.gz"
    );
    // A name that already says it is not said twice.
    assert_eq!(
        specification(Some("mine.csv.gz"), Compression::Gzip).file_name(),
        "mine.csv.gz"
    );
    assert_eq!(specification(None, Compression::Auto).file_name(), "p.csv");
}

#[test]
fn an_export_rejects_the_options_that_describe_reading() {
    for (format, option) in [
        ("csv", "ignore_headers = true"),
        ("csv", "quoting = false"),
        ("csv", "on_error = warn"),
        ("json", "http_headers = { Accept = \"application/json\" }"),
        ("json", "on_error = warn"),
    ] {
        let source = format!(
            "p(1) .
@export p to {format} {{ {option} }} .
"
        );
        let program = parse(&source);
        let mut engine = Engine::new();
        let problem = engine
            .load(program)
            .expect_err("an option about reading has no business here");
        assert_eq!(problem.code(), codes::READ_OPTION_ON_EXPORT, "{problem:?}");
    }
}

/// A cursor notices any later work on its table, new blocks as much as a changed count,
/// and returns an error rather than rows that no longer match the table.
#[test]
fn cursors_notice_later_work_on_their_table() {
    let mut engine = run("
        edge(1, 2) . edge(2, 3) .
        t(?x, ?y) :- edge(?x, ?y) .
        t(?x, ?z) :- t(?x, ?y), edge(?y, ?z) .
    ");
    let mut cursor = engine.cursor("t").expect("the predicate exists");
    let first = engine
        .read_chunk(&mut cursor, 1)
        .expect("the first row reads");
    assert_eq!(first.len(), 1);
    engine
        .import_rows("edge", [vec![DataValue::integer(3), DataValue::integer(4)]])
        .expect("the rows land");
    engine.reason().expect("reasoning continues");
    let refusal = engine
        .read_chunk(&mut cursor, 1)
        .expect_err("the table gained blocks since the cursor opened");
    assert_eq!(refusal.code(), codes::CURSOR_INVALIDATED, "{refusal:?}");
}

/// A run stopped short of the fixpoint leaves nothing to explain: a
/// trace fails until reasoning has run to the end.
#[test]
fn traces_fail_on_a_store_short_of_the_fixpoint() {
    let mut source = String::new();
    for index in 0..60 {
        source += &format!("edge({index}, {}) . ", index + 1);
    }
    source += "path(?x, ?y) :- edge(?x, ?y) . path(?x, ?z) :- edge(?x, ?y), path(?y, ?z) .";
    let program = parse(&source);
    let mut engine = Engine::new();
    engine.load(program).expect("the program loads");
    let fact = Fact::parse("path(0, 60) .").expect("a fact");
    // Before any run, and after a cancelled one, the store is no
    // fixpoint; after a completed one it is.
    let refusal = engine
        .trace(std::slice::from_ref(&fact))
        .expect_err("nothing ran yet");
    assert!(
        [codes::PENDING_UPDATES, codes::UNFINISHED].contains(&refusal.code()),
        "{refusal:?}"
    );
    let token = engine.cancel_token();
    let handle = std::thread::spawn(move || {
        let outcome = engine.reason();
        (engine, outcome)
    });
    token.cancel();
    let (mut engine, outcome) = handle.join().expect("the run does not panic");
    if outcome.is_err() {
        let refusal = engine
            .trace(std::slice::from_ref(&fact))
            .expect_err("the run was cut short");
        assert_eq!(refusal.code(), codes::UNFINISHED, "{refusal:?}");
        engine.reason().expect("resuming reaches the fixpoint");
    }
    let response = engine.trace(&[fact]).expect("the fixpoint is traceable");
    assert!(response.facts[0].holds);
}

/// A diagnostic naming a predicate a stage introduced names the written
/// literals it replaces instead.
#[cfg(feature = "sparql")]
#[test]
fn diagnostics_name_the_literals_a_hidden_predicate_replaces() {
    let program = parse(
        "@import triple from sparql { endpoint = <http://example.org/sparql>, \
         query = \"SELECT ?s ?p ?o WHERE { ?s ?p ?o }\" } .\n\
         anc(<urn:a>) .\nanc(?p) :- anc(?c), triple(?c, <urn:parent>, ?p) .\n@output anc .",
    );
    let mut engine = Engine::new();
    engine.load(program).expect("the program loads");
    let written = engine
        .written
        .as_ref()
        .expect("a loaded program is written");
    let presented = written.present(
        Diagnostic::warning(codes::NEVER_FIRES, "a rule reads `triple_query1`")
            .with_field("predicate", "triple_query1"),
    );
    let literals = "the literals `triple(?c, <urn:parent>, ?p)`";
    assert_eq!(presented.message(), format!("a rule reads {literals}"));
    assert_eq!(presented.field("predicate"), Some(literals));
    // The binding rule is hidden too.
    let presented = written.present(Diagnostic::warning(
        codes::NEVER_FIRES,
        "a rule reads `triple_bindings1`",
    ));
    assert_eq!(presented.message(), "a rule reads the literals `anc(?c)`");
}

/// A program's outputs are what it declares `@output` or exports; one
/// declaring neither has every predicate rules derive as an output, as old
/// nemo had, so its imports are trimmed as in any other program.
#[test]
fn outputs_are_declared_or_derived() {
    let data = [("p.csv", "1,x\n2,y\n")];
    let declared = load_resources(
        "@import p from csv { resource = \"p.csv\" } .\nq(?a) :- p(?a, _) .\n\
         r(?a) :- q(?a) .\n@output r .\n@export q to csv {} .",
        &data,
    );
    assert_eq!(declared.outputs(), ["r", "q"]);

    let mut undeclared = load_resources(
        "@import p from csv { resource = \"p.csv\" } .\nq(?a) :- p(?a, _) .",
        &data,
    );
    assert_eq!(undeclared.outputs(), ["q"]);
    assert_eq!(count(&mut undeclared, "q"), 2);
    // The import is no output: only the column the rule reads is stored.
    assert_eq!(stored(&undeclared, "p"), (2, 1));
}

#[test]
fn a_program_text_is_carried_through_a_loading_method() {
    let mut engine = Engine::new();
    engine
        .ingest(
            "test.rls",
            "p(1) .\nq(?x) :- p(?x) .\n",
            |engine, program| engine.load(program),
        )
        .expect("the program loads");
    assert_eq!(
        arities(&engine),
        vec![(String::from("p"), 1), (String::from("q"), 1)]
    );
}

#[test]
fn a_text_with_errors_fails_with_what_parsing_reported() {
    let mut engine = Engine::new();
    let refused = engine
        .ingest(
            "test.rls",
            "p(1 .\nq(?x) :- p(?x) .\n",
            |engine, program| engine.load(program),
        )
        .expect_err("the first statement does not parse");
    assert!(!refused.is_empty());
    assert!(
        refused
            .iter()
            .all(|diagnostic| diagnostic.severity() == nemo_physical::diagnostics::Severity::Error)
    );
    // The spans point into the registered text.
    let file = refused[0].labels()[0].span.file;
    assert_eq!(engine.file_name(file), Some("test.rls"));
    assert!(engine.predicates().is_empty(), "nothing loaded");
}

#[test]
fn a_check_collects_what_parsing_and_the_engine_report() {
    let mut engine = Engine::new();
    // One statement does not parse, the other is unsafe: both are
    // reported, and nothing loads.
    let reported = engine.check_source("test.rls", "p(1 .\nq(?x, ?y) :- p(?x) .\n");
    let codes: Vec<String> = reported
        .diagnostics()
        .iter()
        .map(|diagnostic| diagnostic.code().to_string())
        .collect();
    assert!(codes.contains(&String::from("N0202")), "{codes:?}");
    assert!(codes.contains(&String::from("N0204")), "{codes:?}");
    assert!(engine.predicates().is_empty(), "nothing loaded");
}
