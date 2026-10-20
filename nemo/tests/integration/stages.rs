//! Stages an embedder adds to the engine's pipeline: they run after the
//! modules stage, at every load, and over every extension, where the
//! loaded program's statements are frozen.

use std::sync::{Arc, Mutex};

use nemo::{
    codes,
    engine::{Engine, PredicateRole},
    model::{Fact, Identifier, ImportDirective, Literal, Statement, Term},
    pipeline::{Commit, Transformation},
    trace::{TraceNode, query::TreeForTableQuery},
};
use nemo_physical::diagnostics::Diagnostic;

use crate::common::{arities, parse, roles, rows};

/// A stage noting, per run, how many statements it may rewrite and how many it sees.
///
/// It rewrites nothing.
struct Observe {
    /// Per run, the rewritable and the visible statement counts.
    seen: Arc<Mutex<Vec<(usize, usize)>>>,
}

impl Transformation for Observe {
    fn name(&self) -> &'static str {
        "observe"
    }

    fn apply(&self, commit: &mut Commit<'_>) -> Result<(), Diagnostic> {
        self.seen
            .lock()
            .expect("no test thread panicked holding the lock")
            .push((commit.ids().len(), commit.all_ids().len()));
        Ok(())
    }
}

#[test]
fn an_added_stage_runs_at_load_and_over_extensions() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut engine = Engine::new();
    engine.stages_mut().push(Observe { seen: seen.clone() });
    engine
        .load(parse(
            "noise(1) . keep(2) . out(?x) :- noise(?x) . out(?x) :- keep(?x) .",
        ))
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    engine
        .extend(parse("noise(3) . keep(4) ."))
        .expect("the extension loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    assert_eq!(rows(&mut engine, "out"), ["1", "2", "3", "4"]);
    // At load the stage may rewrite everything; over the extension only
    // its two statements, beside the four the load left.
    assert_eq!(
        *seen
            .lock()
            .expect("no test thread panicked holding the lock"),
        [(4, 4), (2, 6)]
    );
}

#[cfg(feature = "sparql")]
#[test]
fn the_engines_own_stages_take_neighbours() {
    let mut engine = Engine::new();
    assert_eq!(engine.stages_mut().names(), ["sparql"]);
    engine.stages_mut().insert_before(
        "sparql",
        Observe {
            seen: Arc::new(Mutex::new(Vec::new())),
        },
    );
    assert_eq!(engine.stages_mut().names(), ["observe", "sparql"]);
}

/// A stage removing the facts of one predicate: it changes what the
/// program says, which no stage may.
struct DropFacts(&'static str);

impl Transformation for DropFacts {
    fn name(&self) -> &'static str {
        "drop-facts"
    }

    fn apply(&self, commit: &mut Commit<'_>) -> Result<(), Diagnostic> {
        let predicate = Identifier::bare(self.0);
        for id in commit.ids() {
            if matches!(commit.statement(id), Statement::Fact(fact) if fact.predicate == predicate)
            {
                commit.remove(id);
            }
        }
        Ok(())
    }
}

#[test]
#[should_panic(expected = "without declaring it")]
fn a_stage_removing_facts_panics() {
    let mut engine = Engine::new();
    engine.stages_mut().push(DropFacts("noise"));
    let _ = engine.load(parse("noise(1) . keep(2) ."));
}

/// Dropping the facts of a predicate is an edit before the load: what
/// the engine loads is the program, and every result refers to it.
#[test]
fn an_edit_before_the_load_is_the_program() {
    let mut program = parse("noise(1) . keep(2) . out(?x) :- noise(?x) . out(?x) :- keep(?x) .");
    let noise = Identifier::bare("noise");
    program
        .statements
        .retain(|statement| !matches!(statement, Statement::Fact(fact) if fact.predicate == noise));
    let mut engine = Engine::new();
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "out"), ["2"]);

    let fact = Fact::parse("out(2) .").expect("the fact parses");
    let response = engine.trace(&[fact]).expect("tracing answers");
    let TraceNode::Derived { rule, .. } = response.facts[0].tree.as_ref().expect("a proof tree")
    else {
        panic!("a derived fact gets a rule node");
    };
    // The edited program's third statement: its ids are positions in
    // what was loaded.
    assert_eq!(rule.rule, 2);
    assert_eq!(rule.text, "out(?x) :- keep(?x) .");
}

/// A stage splitting long rule bodies: the first two positive literals
/// become a rule of their own.
struct SplitBodies;

impl Transformation for SplitBodies {
    fn name(&self) -> &'static str {
        "split-bodies"
    }

    fn apply(&self, commit: &mut Commit<'_>) -> Result<(), Diagnostic> {
        for id in commit.ids() {
            let atoms: Vec<usize> = match commit.statement(id) {
                Statement::Rule(rule) if rule.body.len() > 2 => rule
                    .body
                    .iter()
                    .enumerate()
                    .filter(|(_, literal)| matches!(literal, Literal::Positive(_)))
                    .map(|(position, _)| position)
                    .take(2)
                    .collect(),
                _ => continue,
            };
            if atoms.len() == 2 {
                let name = commit.fresh("part");
                commit.fold(id, &atoms, name);
            }
        }
        Ok(())
    }
}

#[test]
fn a_folded_rule_traces_as_written() {
    let mut engine = Engine::new();
    engine.stages_mut().push(SplitBodies);
    engine
        .load(parse(
            "e(1, 2) . e(2, 3) . f(3) .\np(?x) :- e(?x, ?y), e(?y, ?z), f(?z) .",
        ))
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(rows(&mut engine, "p"), ["1"]);

    let fact = Fact::parse("p(1) .").expect("the fact parses");
    let response = engine.trace(&[fact]).expect("tracing answers");
    let TraceNode::Derived {
        rule,
        premises,
        assignment,
        ..
    } = response.facts[0].tree.as_ref().expect("a proof tree")
    else {
        panic!("a derived fact gets a rule node");
    };
    assert_eq!(rule.text, "p(?x) :- e(?x, ?y), e(?y, ?z), f(?z) .");
    assert_eq!(rule.rule, 3, "the rule is the fourth written statement");
    let premises: Vec<String> = premises
        .iter()
        .map(|premise| premise.fact().to_string())
        .collect();
    assert_eq!(premises, ["e(1, 2)", "e(2, 3)", "f(3)"]);
    // The variable the fold kept to itself comes back from its rule.
    let bindings: Vec<String> = assignment
        .iter()
        .map(|binding| format!("{}={}", binding.variable, binding.value))
        .collect();
    assert_eq!(bindings, ["?x=1", "?y=2", "?z=3"]);
    assert!(
        !serde_json::to_string(&response)
            .expect("the response serializes")
            .contains("part1"),
        "the folded predicate stays hidden"
    );
}

#[test]
fn queries_over_a_folded_rule_name_the_written_rule() {
    let mut engine = Engine::new();
    engine.stages_mut().push(SplitBodies);
    engine
        .load(parse(
            "e(1, 2) . e(2, 3) . e(5, 6) . f(3) .\np(?x) :- e(?x, ?y), e(?y, ?z), f(?z) .",
        ))
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    // A tree query expands the written rule; the folded predicate never
    // shows, and its premises stand at the written literals.
    let response = engine
        .tree_for_table(TreeForTableQuery {
            predicate: "p".into(),
            table_entries: None,
        })
        .expect("the query answers");
    let json = serde_json::to_string(&response).expect("the response serializes");
    assert!(
        !json.contains("part1"),
        "the folded predicate shows: {json}"
    );
    let expansion = response
        .child_information
        .as_ref()
        .expect("the server chose a rule");
    let facts = |index: usize| -> Vec<String> {
        let node = &expansion.children[index];
        node.table_entries
            .entries
            .iter()
            .map(|entry| format!("{}({})", node.predicate, entry.term_tuple.join(", ")))
            .collect()
    };
    assert_eq!(expansion.rule.id, 4);
    assert_eq!(facts(0), ["e(1, 2)"]);
    assert_eq!(facts(1), ["e(2, 3)"]);
    assert_eq!(facts(2), ["f(3)"]);
}

#[test]
fn the_listings_name_the_written_predicates_only() {
    let mut engine = Engine::new();
    engine.stages_mut().push(SplitBodies);
    engine
        .load(parse(
            "e(1, 2) . e(2, 3) . f(3) .\np(?x) :- e(?x, ?y), e(?y, ?z), f(?z) .",
        ))
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    assert_eq!(
        arities(&engine),
        [("e".into(), 2), ("f".into(), 1), ("p".into(), 1)]
    );
    assert_eq!(
        roles(&engine),
        [
            ("e".into(), PredicateRole::Extensional),
            ("f".into(), PredicateRole::Extensional),
            ("p".into(), PredicateRole::Derived),
        ]
    );
    let refusal = engine
        .rows("part1")
        .expect_err("the folded predicate is hidden");
    assert_eq!(refusal.code(), codes::UNKNOWN_PREDICATE);
    // The folded predicate's row counts toward neither figure.
    assert_eq!((engine.loaded_facts(), engine.derived_facts()), (3, 1));
}

/// A stage replacing the rule of `event` by an import of what it
/// derives.
struct Precomputed;

impl Transformation for Precomputed {
    fn name(&self) -> &'static str {
        "precomputed"
    }

    fn apply(&self, commit: &mut Commit<'_>) -> Result<(), Diagnostic> {
        let event = Identifier::bare("event");
        for id in commit.ids() {
            if matches!(commit.statement(id), Statement::Rule(rule) if rule.head[0].predicate == event)
            {
                let import = ImportDirective::new(
                    event.clone(),
                    Identifier::bare("csv"),
                    vec![(Term::constant("resource"), Term::string("events.csv"))],
                );
                commit.extract_rule(id, import, &[]);
            }
        }
        Ok(())
    }
}

#[test]
fn a_rule_a_stage_replaces_by_an_import_stays_derived_and_whole() {
    let mut engine = Engine::new();
    engine.stages_mut().push(Precomputed);
    engine.add_resource("events.csv", b"1,2\n3,4\n".to_vec());
    engine
        .load(parse(
            "source(1, 2) . source(3, 4) .\n\
             event(?e, ?n) :- source(?e, ?n) .\n\
             kind(?n) :- event(_, ?n) .",
        ))
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    // `kind` reads one column of `event`, and the import still stores
    // both: the user derives `event`.
    assert_eq!(rows(&mut engine, "event"), ["1,2", "3,4"]);
    assert_eq!(rows(&mut engine, "kind"), ["2", "4"]);
    assert_eq!(
        roles(&engine),
        [
            ("event".into(), PredicateRole::Derived),
            ("kind".into(), PredicateRole::Derived),
            ("source".into(), PredicateRole::Extensional),
        ]
    );
    // What the import stored for `event` counts as derived.
    assert_eq!((engine.loaded_facts(), engine.derived_facts()), (2, 4));
}

/// A stage defining a predicate by a rule that translation refuses:
/// its head variable `?y` occurs in no body literal.
struct UnsafeDefinition;

impl Transformation for UnsafeDefinition {
    fn name(&self) -> &'static str {
        "unsafe-definition"
    }

    fn apply(&self, commit: &mut Commit<'_>) -> Result<(), Diagnostic> {
        let Some(source) = commit
            .ids()
            .into_iter()
            .find(|&id| matches!(commit.statement(id), Statement::Rule(_)))
        else {
            return Ok(());
        };
        let Statement::Rule(rule) = parse("helper(?x, ?y) :- keep(?x) .").statements.remove(0)
        else {
            panic!("the text is a rule");
        };
        commit.define(rule, &[source]);
        Ok(())
    }
}

#[test]
#[should_panic(
    expected = "the pipeline stages `unsafe-definition` wrote a program that translation refuses"
)]
fn a_stage_writing_what_translation_refuses_panics() {
    let mut engine = Engine::new();
    engine.stages_mut().push(UnsafeDefinition);
    let _ = engine.load(parse("keep(1) .\nout(?x) :- keep(?x) ."));
}
