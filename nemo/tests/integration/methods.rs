//! The engine's methods around reasoning: predicate listings, cursors,
//! cancellation, telemetry, exports, and the errors of an engine that has not
//! reached its fixpoint.

use std::{sync::Arc, time::Duration};

use nemo::{
    codes,
    engine::{Engine, ExportSpec, PredicateRole, remote::Progress},
    model::{Fact, FileId},
    telemetry::Recorder,
};
use nemo_io::compression::Compression;
use nemo_physical::{
    diagnostics::{Code, Diagnostic, Severity},
    value::DataValue,
};

use crate::common::{TempDirectory, arities, cells, count, parse, roles, run};

/// A predicate's role follows from where its rows come from: rules, input facts, or both.
///
/// A predicate that is only read counts as extensional, and has no facts.
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

/// A cancellation from before a run is cleared when the run starts,
/// and one during a run stops it at a rule boundary, after which a later run continues to the
/// same fixpoint.
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
    // continuing if it was cancelled. (Moving the engine into the thread
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

/// A cursor delivers a predicate's rows in bounded chunks, the same rows as reading them at
/// once, and delivers empty chunks once it is exhausted.
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

/// The telemetry events of a run record that it reached the fixpoint,
/// how many facts it derived, and the rules it applied.
#[test]
fn telemetry_events_account_for_the_run() {
    let program = parse(
        "edge(1, 2) . edge(2, 3) . edge(3, 4) .
         reach(?x, ?y) :- edge(?x, ?y) .
         reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .",
    );
    let mut engine = Engine::new();
    let recorder = Arc::new(Recorder::new());
    engine.set_telemetry(recorder.clone());
    engine.load(program).expect("the program loads");
    recorder.set_rules(engine.compiled_rules());
    engine.reason().expect("reasoning reaches the fixpoint");

    assert_eq!(engine.compiled_rules().len(), 2);
    assert!(engine.compiled_rules()[0].contains("edge"));
    let snapshot = recorder.snapshot_json();
    assert!(snapshot.contains("\"finished\":true"), "{snapshot}");
    assert!(snapshot.contains("\"fixpoint\":true"), "{snapshot}");
    assert!(
        snapshot.contains(&format!("\"derived\":{}", engine.derived_facts())),
        "the stream's total matches the engine's: {snapshot}"
    );
    assert!(snapshot.contains("reach(?x, ?y)"), "{snapshot}");
}

/// Telemetry compares the delta atom of a variant with the atoms that bind the first variable
/// of the rule's order, so a rule without variables, which has no order, is not observed.
#[test]
fn telemetry_observes_no_variant_of_a_rule_without_variables() {
    let program = parse(
        "p(1) .
         found() :- p(_) .
         copied(?x) :- found(), p(?x) .
         answer(yes) :- found() .",
    );
    let mut engine = Engine::new();
    let recorder = Arc::new(Recorder::new());
    engine.set_telemetry(recorder.clone());
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    // Only the variant of `copied` led by `found()` sums the rows of the atom that leads, `p`.
    let snapshot = recorder.snapshot_json();
    let led: Vec<u64> = snapshot
        .split("\"ledRows\":")
        .skip(1)
        .map(|rest| {
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            digits.parse().expect("a count")
        })
        .collect();
    assert!(!led.is_empty(), "{snapshot}");
    assert!(led.iter().all(|&rows| rows <= 1), "{snapshot}");
}

/// The file name of an export under gzip compression ends with `.gz`, which is added once.
#[test]
fn a_gzipped_export_says_so_in_its_name() {
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

    // A name that already ends with `.gz` does not get it twice.
    assert_eq!(
        specification(Some("mine.csv.gz"), Compression::Gzip).file_name(),
        "mine.csv.gz"
    );
    assert_eq!(specification(None, Compression::Auto).file_name(), "p.csv");
}

/// An export to memory under gzip compression is one whole gzip stream
/// of the bytes that the same export without compression writes.
#[test]
fn a_gzipped_export_to_memory_holds_the_whole_export() {
    let mut engine = Engine::new();
    engine
        .load(parse("p(1, \"a\") . p(2, \"b\") ."))
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    let plain = engine
        .export_bytes(&ExportSpec::csv("p"))
        .expect("the export writes");
    let mut gzipped = ExportSpec::csv("p");
    gzipped.compression = Compression::Gzip;
    let compressed = engine.export_bytes(&gzipped).expect("the export writes");
    let mut read = Vec::new();
    Compression::Gzip
        .open_bytes(compressed.into(), "p.csv.gz")
        .read_to_end(&mut read)
        .expect("the bytes are one whole gzip stream");
    assert_eq!(read, plain);
}

/// An export directive with an option that only applies to reading fails the load.
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

/// A cursor on a predicate that a reload rebuilt is not valid,
/// and a read through it returns an error.
#[test]
fn cursors_notice_a_reload_that_rebuilds_their_predicate() {
    let mut engine = run("p(1) . p(2) . q(?x) :- p(?x) .");
    let mut cursor = engine.cursor("p").expect("the predicate exists");

    // A changed fact of `p` makes the reload drop the table of `p` and build another.
    engine
        .reload(parse("p(1) . p(3) . q(?x) :- p(?x) ."))
        .expect("the program loads");
    assert!(!engine.is_valid(&cursor));
    let refusal = engine
        .read_chunk(&mut cursor, 1)
        .expect_err("the table that the cursor was opened on is dropped");
    assert_eq!(refusal.code(), codes::CURSOR_INVALIDATED, "{refusal:?}");
}

/// An export of a predicate that no statement of the program mentions is an error,
/// and an output that the program declares but never uses is exported without rows.
#[test]
fn an_export_fails_for_a_predicate_the_program_does_not_mention() {
    let mut engine = run("p(1) .\n@output p, unused .");
    let refusal = engine
        .export_bytes(&ExportSpec::csv("typo"))
        .expect_err("no statement mentions `typo`");
    assert_eq!(refusal.code(), codes::UNKNOWN_PREDICATE, "{refusal:?}");
    assert_eq!(refusal.field("predicate"), Some("typo"));

    let bytes = engine
        .export_bytes(&ExportSpec::csv("unused"))
        .expect("a declared output is exported");
    assert!(bytes.is_empty(), "{bytes:?}");
}

/// The export methods return an error for an engine without a program,
/// also if there is nothing to write.
#[test]
fn an_export_fails_without_a_program() {
    let directory = TempDirectory::new();
    let mut engine = Engine::new();
    let refusal = engine
        .export(directory.path())
        .expect_err("no program is loaded");
    assert_eq!(refusal.code(), codes::NO_PROGRAM, "{refusal:?}");
    let refusal = engine
        .export_bytes(&ExportSpec::csv("p"))
        .expect_err("no program is loaded");
    assert_eq!(refusal.code(), codes::NO_PROGRAM, "{refusal:?}");
}

/// A source registered after a program whose text is not registered
/// gets a file id that the spans of the program do not use.
#[test]
fn a_registered_source_shares_no_file_id_with_an_unregistered_program() {
    let mut engine = Engine::new();

    // The program is parsed with the file id 0, and its text is not registered.
    engine.load(parse("p(1) .")).expect("the program loads");
    let file = engine
        .add_source("later.rls", "q(1) .")
        .expect("the text is small");
    assert_ne!(file, FileId::new(0));
    assert_eq!(engine.file_name(FileId::new(0)), None);
}

/// A trace fails until a run has reached the fixpoint,
/// both before any run and after a cancelled one.
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

    // Before any run and after a cancelled one, the stored facts are not at the
    // fixpoint, and after a completed run they are.
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
    assert!(response.proof(0).is_some());
}

/// [load_source](Engine::load_source) registers, parses, links and loads a program text.
#[test]
fn a_program_text_is_carried_through_a_loading_method() {
    let mut engine = Engine::new();
    engine
        .load_source("test.rls", "p(1) .\nq(?x) :- p(?x) .\n")
        .expect("the program loads");
    assert_eq!(
        arities(&engine),
        vec![(String::from("p"), 1), (String::from("q"), 1)]
    );
}

/// A text that does not parse fails with the parser's errors, whose spans point into the
/// registered text, and nothing is loaded.
#[test]
fn a_text_with_errors_fails_with_what_parsing_reported() {
    let mut engine = Engine::new();
    let refused = engine
        .load_source("test.rls", "p(1 .\nq(?x) :- p(?x) .\n")
        .expect_err("the first statement does not parse");
    assert!(!refused.is_empty());
    assert!(
        refused
            .iter()
            .all(|diagnostic| diagnostic.severity() == Severity::Error)
    );

    // The spans point into the registered text.
    let file = refused[0].labels()[0].span.file;
    assert_eq!(engine.file_name(file), Some("test.rls"));
    assert!(engine.predicates().is_empty(), "nothing loaded");
}

/// [check_source](Engine::check_source) reports both the parser's errors and the check's,
/// and loads nothing.
#[test]
fn a_check_collects_what_parsing_and_the_engine_report() {
    let mut engine = Engine::new();

    // One statement does not parse, and the other is unsafe.
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

/// A run's `NOW()` is the instant its facts were loaded at, however long after the load it
/// starts, and the run after an extension has the instant of the extension.
#[test]
fn a_run_evaluates_at_the_instant_its_facts_were_loaded_at() {
    let pause = || std::thread::sleep(Duration::from_millis(5));
    let mut engine = Engine::new();
    engine
        .load(parse(
            "started(NOW()) .
             same(yes) :- started(?now), NOW() = ?now .",
        ))
        .expect("the program loads");
    pause();
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(cells(&mut engine, "same"), vec![vec![String::from("yes")]]);

    pause();
    engine
        .extend(parse(
            "restarted(NOW()) .
             again(yes) :- restarted(?now), NOW() = ?now .",
        ))
        .expect("the extension loads");
    pause();
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(cells(&mut engine, "again"), vec![vec![String::from("yes")]]);
    assert_ne!(
        cells(&mut engine, "started"),
        cells(&mut engine, "restarted"),
        "each run has its own instant"
    );
}

/// A discarded suspended load withdraws what it reported, so loading again reports it once.
#[test]
fn a_discarded_load_withdraws_its_reports() {
    let program = || {
        parse(
            "@input given .
             @import p from csv { resource = \"http://example.org/p.csv\" } .",
        )
    };
    let unsupplied = |engine: &Engine| {
        engine
            .reports()
            .iter()
            .filter(|report| report.code() == codes::INPUT_UNSUPPLIED)
            .count()
    };
    let mut engine = Engine::new();
    let progress = engine
        .start_load(program())
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert!(
        matches!(progress, Progress::Suspended(_)),
        "the import is suspended"
    );
    assert_eq!(unsupplied(&engine), 1, "{:?}", engine.reports());
    assert!(engine.discard_pending());
    assert_eq!(unsupplied(&engine), 0, "{:?}", engine.reports());

    let progress = engine
        .start_load(program())
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert!(
        matches!(progress, Progress::Suspended(_)),
        "the import is suspended"
    );
    assert_eq!(unsupplied(&engine), 1, "{:?}", engine.reports());
}

/// A reload drops the reports about the program that it replaces.
/// It keeps the reports about the new program, also those of linking it,
/// and a reload that fails drops nothing.
#[test]
fn a_reload_drops_the_reports_about_the_replaced_program() {
    let codes_of =
        |engine: &Engine| -> Vec<Code> { engine.reports().iter().map(Diagnostic::code).collect() };

    let mut engine = Engine::new();
    engine
        .add_module("promises", "@output real, ghost . real(1) .")
        .expect("the name is a module name");
    engine
        .load_source("main.rls", "@input given .\nr(1) . q(?x) :- r(?x) .\n")
        .unwrap_or_else(|problems| panic!("{problems:?}"));
    assert_eq!(codes_of(&engine), [codes::INPUT_UNSUPPLIED]);

    engine
        .reload_source("main.rls", "r(1, 2) . q(?x) :- r(?x) .\n")
        .expect_err("`r` is used with two numbers of columns");
    assert_eq!(codes_of(&engine), [codes::INPUT_UNSUPPLIED]);

    engine
        .reload_source(
            "main.rls",
            "@use promises {} as g .\nanswer(?x) :- g::real(?x) .\n",
        )
        .unwrap_or_else(|problems| panic!("{problems:?}"));
    assert_eq!(codes_of(&engine), [codes::OFFERS_UNWRITTEN]);

    engine
        .reload_source("main.rls", "r(1) . q(?x) :- r(?x) .\n")
        .unwrap_or_else(|problems| panic!("{problems:?}"));
    assert_eq!(codes_of(&engine), []);
}
