//! Multi-stage reasoning: post-fixpoint rows, program extension, and reload,
//! the verbs that continue a loaded engine.
//!
//! Positive programs take new rows as free deltas.
//! Conclusions that depended on a grown predicate through negation or aggregation drop and are
//! computed again.
//! A reload replaces the program while unchanged imports stay unread.

use nemo::engine::Engine;
use nemo_physical::value::DataValue;

use crate::common::{TempDirectory, parse, rows, run};

/// One integer row.
fn row(values: &[i64]) -> Vec<DataValue> {
    values
        .iter()
        .map(|&value| DataValue::integer(value))
        .collect()
}

#[test]
fn positive_programs_take_rows_as_deltas() {
    let mut engine = run("
        edge(1, 2) . edge(2, 3) .
        reach(?x, ?y) :- edge(?x, ?y) .
        reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
    ");
    assert_eq!(engine.rows("reach").expect("reach").len(), 3);
    let derived_before = engine.derived_facts();

    engine
        .import_rows("edge", vec![row(&[3, 4])])
        .expect("rows import");
    engine.reason().expect("reasoning continues");
    // 1→2→3→4 closes into six pairs, three of them new.
    assert_eq!(engine.rows("reach").expect("reach").len(), 6);
    // Nothing was computed again, only the new conclusions were derived.
    assert_eq!(engine.derived_facts(), derived_before + 3);
}

#[test]
fn reasoning_again_without_changes_is_a_fixpoint() {
    let mut engine = run("edge(1, 2) . reach(?x, ?y) :- edge(?x, ?y) .");
    let applications = engine.applications();
    engine.reason().expect("an idle run succeeds");
    assert_eq!(engine.applications(), applications, "no rule re-applied");
}

#[test]
fn negation_recovers_from_growth() {
    let mut engine = run("
        node(1) . node(2) . blocked(2) .
        free(?x) :- node(?x), ~blocked(?x) .
    ");
    assert_eq!(rows(&mut engine, "free"), vec!["1"]);

    // Growing the negated predicate invalidates an old conclusion.
    engine
        .import_rows("blocked", vec![row(&[1])])
        .expect("rows import");
    engine.reason().expect("recovery reasons");
    assert!(rows(&mut engine, "free").is_empty());

    // Growing the positive side stays incremental.
    engine
        .import_rows("node", vec![row(&[3])])
        .expect("rows import");
    engine.reason().expect("reasoning continues");
    assert_eq!(rows(&mut engine, "free"), vec!["3"]);
}

#[test]
fn guards_recover_from_growth() {
    // The emptiness guard flips once `stop` holds any row: the old
    // conclusions must go.
    let mut engine = run("node(1) . go(?x) :- node(?x), ~stop(?y) . stop(9) :- never(9) .");
    assert_eq!(rows(&mut engine, "go"), vec!["1"]);
    engine
        .import_rows("stop", vec![row(&[7])])
        .expect("rows import");
    engine.reason().expect("recovery reasons");
    assert!(rows(&mut engine, "go").is_empty());
}

#[test]
fn aggregates_recover_from_growth() {
    let mut engine = run("
        pay(alice, 10) . pay(alice, 5) .
        total(?who, #sum(?amount per ?amount)) :- pay(?who, ?amount) .
    ");
    assert_eq!(rows(&mut engine, "total"), vec!["alice,15"]);

    engine
        .import_rows(
            "pay",
            vec![vec![DataValue::iri("alice"), DataValue::integer(7)]],
        )
        .expect("rows import");
    engine.reason().expect("recovery reasons");
    // The stale total dropped; only the new one stands.
    assert_eq!(rows(&mut engine, "total"), vec!["alice,22"]);
}

#[test]
fn recovery_cascades_through_dependents() {
    // The stale negation conclusion feeds a positive chain; dropping it
    // must take the chain along.
    let mut engine = run("
        node(1) . free(?x) :- node(?x), ~blocked(?x) .
        report(?x) :- free(?x) .
    ");
    assert_eq!(rows(&mut engine, "report"), vec!["1"]);
    engine
        .import_rows("blocked", vec![row(&[1])])
        .expect("rows import");
    engine.reason().expect("recovery reasons");
    assert!(rows(&mut engine, "report").is_empty());
}

#[test]
fn recovery_follows_growth_through_derived_predicates() {
    // `blocked` and `paid` grow only once reasoning continues,
    // from the new rows of the predicates they are derived from.
    let mut engine = run("
        node(1) . node(2) . wall(1) .
        blocked(?x) :- wall(?x) .
        free(?x) :- node(?x), ~blocked(?x) .
        pay(10) .
        paid(?amount) :- pay(?amount) .
        total(#sum(?amount)) :- paid(?amount) .
    ");
    assert_eq!(rows(&mut engine, "free"), vec!["2"]);
    assert_eq!(rows(&mut engine, "total"), vec!["10"]);

    engine
        .import_rows("wall", vec![row(&[2])])
        .expect("rows import");
    engine
        .import_rows("pay", vec![row(&[5])])
        .expect("rows import");
    engine.reason().expect("recovery reasons");
    assert!(rows(&mut engine, "free").is_empty());
    assert_eq!(rows(&mut engine, "total"), vec!["15"]);
}

#[test]
fn imported_rows_pass_the_fact_gate() {
    let mut engine = run("@declare p(v: int) . p(1) . q(?x) :- p(?x) .");
    let refusal = engine
        .import_rows("p", vec![vec![DataValue::string("no")]])
        .expect_err("a declared type refuses the row");
    assert_eq!(
        refusal.code(),
        nemo::codes::FACT_OUTSIDE_DECLARATION,
        "{refusal:?}"
    );
    assert_eq!(refusal.field("declared"), Some("int"));
    // A row has no source position, so the message names the column and a note the row.
    assert_eq!(refusal.field("column"), Some("1"));
    assert!(
        refusal
            .notes()
            .iter()
            .any(|note| note == "the row given to `import_rows` is `p(\"no\")`"),
        "{refusal:?}"
    );

    let refusal = engine
        .import_rows("p", vec![row(&[1, 2])])
        .expect_err("the arity is one predicate's");
    assert_eq!(refusal.code(), nemo::codes::ARITY_MISMATCH, "{refusal:?}");
    assert_eq!(refusal.field("arity"), Some("2"));
}

#[test]
fn imported_rows_widen_the_inferred_bounds() {
    let mut engine = run("p(1) . q(?x) :- p(?x) .");
    let before = format!("{:?}", engine.column_types("p").expect("inferred"));
    engine
        .import_rows("p", vec![vec![DataValue::string("wide")]])
        .expect("undeclared bounds widen instead of refusing");
    let after = format!("{:?}", engine.column_types("p").expect("inferred"));
    assert_ne!(before, after, "the bound widened");
    engine.reason().expect("reasoning continues");
    assert_eq!(engine.rows("q").expect("q").len(), 2);
}

#[test]
fn extension_adds_rules_over_stored_data() {
    let mut engine = run("edge(1, 2) . edge(2, 3) .");
    engine
        .extend(parse(
            "reach(?x, ?y) :- edge(?x, ?y) .
                       reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .",
        ))
        .expect("the extension compiles");
    engine.reason().expect("the new rules reason");
    assert_eq!(engine.rows("reach").expect("reach").len(), 3);
}

#[test]
fn extension_adds_facts_as_deltas() {
    let mut engine = run("edge(1, 2) . reach(?x, ?y) :- edge(?x, ?y) .");
    let derived_before = engine.derived_facts();
    engine
        .extend(parse("edge(2, 3) ."))
        .expect("the extension loads");
    engine.reason().expect("reasoning continues");
    assert_eq!(engine.rows("reach").expect("reach").len(), 2);
    assert_eq!(engine.derived_facts(), derived_before + 1);
}

#[test]
fn extension_wakes_existing_negation() {
    // The new rule can populate `blocked` for the first time; the old
    // negation conclusion drops and recomputes.
    let mut engine = run("
        node(1) . node(2) . risky(2) .
        free(?x) :- node(?x), ~blocked(?x) .
    ");
    assert_eq!(rows(&mut engine, "free").len(), 2);
    engine
        .extend(parse("blocked(?x) :- risky(?x) ."))
        .expect("the extension compiles");
    engine.reason().expect("recovery reasons");
    assert_eq!(rows(&mut engine, "free"), vec!["1"]);
}

#[test]
fn extensions_keep_the_one_arity_rule() {
    let mut engine = run("p(1) .");
    let refusal = engine
        .extend(parse("p(1, 2) ."))
        .expect_err("one predicate, one arity");
    assert_eq!(refusal.code(), nemo::codes::ARITY_MISMATCH, "{refusal:?}");
    assert_eq!(refusal.field("arity"), Some("2"));
    // The failed extension left the engine standing.
    engine.reason().expect("the engine still reasons");
    assert_eq!(engine.rows("p").expect("p").len(), 1);
}

#[test]
fn extensions_cannot_change_loaded_meaning() {
    let mut engine = run("@prefix ex: <http://example.org/> . ex:p(1) .");
    let refusal = engine
        .extend(parse("@prefix ex: <http://other.org/> . ex:q(1) ."))
        .expect_err("rebinding a prefix is refused");
    assert_eq!(
        refusal.code(),
        nemo::codes::EXTENSION_REBINDS_PREFIX,
        "{refusal:?}"
    );

    let refusal = engine
        .extend(parse("@base <http://example.org/> ."))
        .expect_err("a late base is refused");
    assert_eq!(
        refusal.code(),
        nemo::codes::EXTENSION_DECLARES_BASE,
        "{refusal:?}"
    );

    // A new prefix, and the same binding again, are both fine.
    engine
        .extend(parse(
            "@prefix ex: <http://example.org/> . @prefix other: <http://other.org/> . other:q(1) .",
        ))
        .expect("consistent declarations extend");
}

#[test]
fn extensions_cannot_redefine_parameters() {
    let mut engine = run("@parameter $limit = 2 . big(?x) :- n(?x), ?x > $limit . n(1) . n(3) .");
    let refusal = engine
        .extend(parse("@parameter $limit = 0 ."))
        .expect_err("the loaded rules compiled with the value");
    assert_eq!(
        refusal.code(),
        nemo::codes::EXTENSION_REDEFINES_PARAMETER,
        "{refusal:?}"
    );
    assert_eq!(
        refusal.message(),
        "an extension cannot redefine parameter `$limit`"
    );
    engine
        .extend(parse(
            "@parameter $floor = 0 . low(?x) :- n(?x), ?x < $floor .",
        ))
        .expect("a new parameter extends");
}

#[test]
fn late_declarations_check_the_store() {
    let mut engine = run("p(\"text\") .");
    let refusal = engine
        .extend(parse("@declare p(v: int) ."))
        .expect_err("the store already violates the declaration");
    assert_eq!(
        refusal.code(),
        nemo::codes::STORED_VALUES_EXCLUDED,
        "{refusal:?}"
    );
    assert_eq!(refusal.field("stored"), Some("string"));
    engine
        .extend(parse("@declare q(v: int) . q(1) ."))
        .expect("a declaration over fresh data extends");
}

#[test]
fn reload_retracts_rules() {
    let mut engine = run("
        node(1) . blocked(1) .
        free(?x) :- node(?x), ~blocked(?x) .
        blocked(?x) :- suspect(?x) .
        suspect(9) :- never(9) .
    ");
    assert!(rows(&mut engine, "free").is_empty());
    // Without the blocking fact, the conclusion returns.
    engine
        .reload(parse(
            "
            node(1) .
            free(?x) :- node(?x), ~blocked(?x) .
            blocked(?x) :- suspect(?x) .
            suspect(9) :- never(9) .
        ",
        ))
        .expect("the replacement loads");
    engine.reason().expect("recomputation reasons");
    assert_eq!(rows(&mut engine, "free"), vec!["1"]);
}

#[test]
fn reload_keeps_unchanged_imports_unread() {
    // The imported file is deleted after loading: a reload that kept
    // the predicate's data must succeed without it, and one that
    // changed the predicate's definition must fail trying to re-read.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_multistage_reload.csv");
    std::fs::write(&path, "1\n2\n").expect("the temp file writes");
    let source = "@import p from csv{resource=\"nemo_multistage_reload.csv\", format=(int)} .
                  q(?x) :- p(?x) .";
    let mut engine = Engine::new();
    engine.set_import_directory(directory);
    engine.load(parse(source)).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(engine.rows("q").expect("q").len(), 2);
    std::fs::remove_file(&path).expect("the temp file deletes");

    // Only a rule changes: the import stays unread, the data stands.
    let changed = "@import p from csv{resource=\"nemo_multistage_reload.csv\", format=(int)} .
                   q(?x) :- p(?x) . r(?x) :- p(?x) .";
    engine
        .reload(parse(changed))
        .expect("unchanged imports stay unread");
    engine.reason().expect("recomputation reasons");
    assert_eq!(engine.rows("r").expect("r").len(), 2);

    // A changed fact of `p` rebuilds the predicate, which must read again.
    let rebuilt = "@import p from csv{resource=\"nemo_multistage_reload.csv\", format=(int)} .
                   p(3) . q(?x) :- p(?x) .";
    engine
        .reload(parse(rebuilt))
        .expect_err("the rebuilt predicate re-reads its import");
}

#[test]
fn reload_keeps_imported_rows_of_unchanged_predicates() {
    let mut engine = run("p(1) . q(?x) :- p(?x) .");
    engine
        .import_rows("p", vec![row(&[2])])
        .expect("rows import");
    engine.reason().expect("reasoning continues");
    assert_eq!(engine.rows("q").expect("q").len(), 2);

    // `p` is unchanged: the rows added to it through `import_rows` survive the reload.
    engine
        .reload(parse("p(1) . q(?x) :- p(?x) . r(?x) :- p(?x) ."))
        .expect("the replacement loads");
    engine.reason().expect("recomputation reasons");
    assert_eq!(engine.rows("r").expect("r").len(), 2);

    // `p` changes: the rows added to it through `import_rows` drop, with a report.
    engine
        .reload(parse("p(7) . q(?x) :- p(?x) ."))
        .expect("the replacement loads");
    engine.reason().expect("recomputation reasons");
    assert_eq!(rows(&mut engine, "q"), vec!["7"]);
    assert!(
        engine
            .reports()
            .iter()
            .any(|report| report.code() == nemo::codes::IMPORTED_ROWS_DROPPED),
        "{:?}",
        engine.reports()
    );
}

#[test]
fn reload_of_the_same_program_recomputes() {
    let source = "p(1) . q(?x) :- p(?x) .";
    let mut engine = run(source);
    engine.reload(parse(source)).expect("the reload loads");
    assert_eq!(engine.derived_facts(), 0, "derived rows dropped");
    engine.reason().expect("recomputation reasons");
    assert_eq!(engine.rows("q").expect("q").len(), 1);
}

#[test]
fn open_cursors_notice_recovery() {
    let mut engine = run("node(1) . free(?x) :- node(?x), ~blocked(?x) .");
    let mut cursor = engine.cursor("free").expect("the cursor opens");
    engine
        .import_rows("blocked", vec![row(&[1])])
        .expect("rows import");
    engine.reason().expect("recovery reasons");
    assert!(!engine.is_current(&cursor));
    let refusal = engine
        .read_chunk(&mut cursor, 16)
        .expect_err("the world shifted under the cursor");
    assert_eq!(refusal.code(), nemo::codes::CURSOR_INVALIDATED);
}

#[test]
fn the_checks_tell_which_methods_fail() {
    let mut engine = Engine::new();
    assert!(!engine.has_program());
    let refusal = engine.reason().expect_err("there is nothing to reason");
    assert_eq!(refusal.code(), nemo::codes::NO_PROGRAM);

    engine
        .load(parse("p(1) . q(?x) :- p(?x) ."))
        .expect("the program loads");
    assert!(engine.has_program());
    let refusal = engine
        .load(parse("r(1) ."))
        .expect_err("one program per engine");
    assert_eq!(refusal.code(), nemo::codes::ALREADY_LOADED);

    assert!(!engine.is_at_fixpoint());
    engine.reason().expect("reasoning reaches the fixpoint");
    assert!(engine.is_at_fixpoint());
    engine
        .import_rows("p", vec![row(&[2])])
        .expect("rows import");
    assert!(!engine.is_at_fixpoint());

    let cursor = engine.cursor("q").expect("the cursor opens");
    assert!(engine.is_current(&cursor));
}

#[test]
fn counters_track_the_store_through_recovery() {
    let mut engine = run("
        node(1) . node(2) .
        free(?x) :- node(?x), ~blocked(?x) .
    ");
    engine
        .import_rows("blocked", vec![row(&[1]), row(&[2])])
        .expect("rows import");
    engine.reason().expect("recovery reasons");
    let stored: usize = ["node", "blocked", "free"]
        .iter()
        .map(|predicate| engine.count(predicate).expect("the predicate exists"))
        .sum();
    assert_eq!(engine.loaded_facts() + engine.derived_facts(), stored);
}

/// A reload whose rules read past what a pruning plan stored reads the import whole again,
/// and nothing is refused, with the passes on and with imports retained.
#[test]
fn reloads_reread_imports_the_new_rules_read_past() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_multistage_reread.csv");
    std::fs::write(&path, "1,5\n2,7\n").expect("the temp file writes");
    let first = "@import p from csv{resource=\"nemo_multistage_reread.csv\", format=(int,int)} .
q(?a) :- p(?a, 5) .
";
    let second = "@import p from csv{resource=\"nemo_multistage_reread.csv\", format=(int,int)} .
q(?a) :- p(?a, 5) .
s(?a, ?v) :- p(?a, ?v) .
";
    for retained in [false, true] {
        let mut engine = Engine::new();
        if retained {
            engine.retain_imports();
        }
        engine.set_import_directory(directory);
        engine.load(parse(first)).expect("the program loads");
        engine.reason().expect("reasoning reaches the fixpoint");
        assert_eq!(rows(&mut engine, "q"), vec!["1"]);
        engine
            .reload(parse(second))
            .expect("the import is read again");
        engine.reason().expect("reasoning reaches the fixpoint");
        assert_eq!(rows(&mut engine, "s"), vec!["1,5", "2,7"]);
    }
}

/// A failed extension leaves no registration behind: what it named can
/// be named again, at any arity.
#[test]
fn failed_extensions_leave_no_registrations_behind() {
    let mut engine = run("p(1) . q(?x) :- p(?x) .");
    let failing = parse(
        "r(?x) :- n(?x, ?y) .
         @import n from csv { resource = \"nemo_multistage_missing.csv\" } .",
    );
    engine
        .extend(failing)
        .expect_err("the import has no file to read");
    assert!(
        !engine
            .predicates()
            .iter()
            .any(|info| info.name == "n" || info.name == "r"),
        "{:?}",
        engine.predicates()
    );
    engine
        .extend(parse("r(?x) :- n(?x) . n(1) ."))
        .expect("n is free to be one column wide");
    engine.reason().expect("reasoning continues");
    assert_eq!(rows(&mut engine, "r"), vec!["1"]);
}
