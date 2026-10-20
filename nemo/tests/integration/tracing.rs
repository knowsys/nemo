//! Tracing: proof trees for derived facts, derived again by grounded backward search.
//!
//! The oracle is a proof checker over the written program:
//! every node's fact must hold in the store,
//! every derivation must be an instance of its written rule
//! (the head and each premise its literal under the assignment),
//! and every leaf must be extensional.
//! A tree that passes is a valid proof whatever path the search took.

mod query;

use std::collections::HashMap;

use nemo::{
    engine::Engine,
    model::{Atom, Fact, Literal, Program, Rule, TermKind, VariableKind},
    trace::{AssertedOrigin, FactText, TraceNode, TraceResponse},
};
use nemo_physical::value::DataValue;

use crate::common::{TempDirectory, parse_into, run};

/// Traces one fact given in fact syntax.
fn trace(engine: &mut Engine, fact: &str) -> TraceResponse {
    let fact = Fact::parse(fact).expect("the fact parses");
    engine.trace(&[fact]).expect("tracing answers")
}

/// The rows of a predicate rendered canonically, for the checker.
fn stored(engine: &mut Engine, predicate: &str) -> Vec<Vec<String>> {
    engine
        .rows(predicate)
        .expect("the predicate exists")
        .into_iter()
        .map(|row| row.iter().map(ToString::to_string).collect())
        .collect()
}

/// Checks a proof tree, validating every node against the engine and the written program:
/// facts hold, derivations are instances of their written rules, leaves are extensional.
fn check(engine: &mut Engine, node: &TraceNode) {
    let fact = node.fact().clone();
    let rows = stored(engine, &fact.predicate);
    assert!(
        rows.contains(&fact.terms),
        "{fact} is claimed but not stored"
    );
    match node {
        // A repeated node's derivation is checked where the tree expands it.
        TraceNode::Asserted { .. } | TraceNode::Repeated { .. } => {}
        TraceNode::Derived {
            rule,
            premises,
            absences,
            assignment,
            aggregate,
            step,
            ..
        } => {
            assert!(*step >= 1, "derivations happen at reasoning steps");
            let parsed = Rule::parse(&rule.text).expect("rule texts re-parse");
            let values: HashMap<String, String> = assignment
                .iter()
                .map(|binding| (binding.variable.clone(), binding.value.clone()))
                .collect();
            assert!(
                parsed
                    .head
                    .iter()
                    .any(|atom| instance_of(atom, &fact, &values)),
                "{fact} is no instance of a head of {}",
                rule.text
            );
            let mut positives = Vec::new();
            let mut negatives = Vec::new();
            for literal in &parsed.body {
                match literal {
                    Literal::Positive(atom) => positives.push(atom),
                    Literal::Negative(atom) => negatives.push(atom),
                    Literal::Operation(_) => {}
                }
            }
            if aggregate.is_some() {
                assert!(
                    premises.is_empty() || premises.len() == positives.len(),
                    "an aggregate node carries either a witness or a summary"
                );
            } else {
                assert_eq!(
                    premises.len(),
                    positives.len(),
                    "one premise per positive body atom of {}",
                    rule.text
                );
            }
            for (premise, atom) in premises.iter().zip(&positives) {
                assert!(
                    instance_of(atom, premise.fact(), &values),
                    "{} is no instance of `{atom}` in {}",
                    premise.fact(),
                    rule.text
                );
                check(engine, premise);
            }
            assert_eq!(absences.len(), negatives.len());
            for (absence, atom) in absences.iter().zip(&negatives) {
                assert!(
                    instance_of(atom, absence, &values),
                    "{absence} is no instance of `~{atom}` in {}",
                    rule.text
                );
            }
        }
    }
}

/// Whether a fact is an instance of a written atom under an
/// assignment: same predicate, and every variable the assignment binds
/// takes its value. Named arguments and constants are left to the
/// engine.
fn instance_of(atom: &Atom, fact: &FactText, values: &HashMap<String, String>) -> bool {
    let predicate = atom.predicate.to_string();
    if predicate.trim_start_matches('<').trim_end_matches('>') != fact.predicate {
        return false;
    }
    if atom.labels.is_some() {
        return true;
    }
    atom.terms.len() == fact.terms.len()
        && atom.terms.iter().zip(&fact.terms).all(|(term, value)| {
            let TermKind::Variable(variable) = &term.kind else {
                return true;
            };
            if variable.kind != VariableKind::Universal {
                return true;
            }
            values
                .get(&variable.to_string())
                .is_none_or(|bound| bound == value)
        })
}

#[test]
fn a_chain_derivation_traces_to_its_asserted_leaves() {
    let mut engine = run("
        edge(a, b) . edge(b, c) . edge(c, d) .
        reach(?x, ?y) :- edge(?x, ?y) .
        reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
    ");
    let response = trace(&mut engine, "reach(a, d) .");
    assert!(response.facts[0].holds);
    let tree = response.facts[0].tree.as_ref().expect("a proof tree");
    check(&mut engine, tree);

    // The chain bottoms out in asserted edges, each attributed to the
    // program with a source position.
    fn leaves(node: &TraceNode, out: &mut Vec<(String, bool)>) {
        match node {
            TraceNode::Asserted {
                fact,
                origin,
                defined_at,
            } => out.push((
                fact.to_string(),
                *origin == AssertedOrigin::Program && !defined_at.is_empty(),
            )),
            TraceNode::Derived { premises, .. } => {
                for premise in premises {
                    leaves(premise, out);
                }
            }
            TraceNode::Repeated { .. } => {}
        }
    }
    let mut collected = Vec::new();
    leaves(tree, &mut collected);
    assert_eq!(collected.len(), 3, "three edges ground the chain");
    assert!(
        collected.iter().all(|(_, attributed)| *attributed),
        "every leaf is a program fact with a source position: {collected:?}"
    );

    // The ASCII rendering shows instantiated rules.
    let ascii = response.ascii();
    assert!(ascii.contains("reach(a, d) :-"), "{ascii}");
    assert!(ascii.contains("└─"), "{ascii}");
}

#[test]
fn derivations_are_traced_at_the_instant_of_their_run() {
    let mut engine = run("
        item(a) .
        stamped(?x, ?t) :- item(?x), ?t = NOW() .
        current(?x) :- stamped(?x, ?t), ?t = NOW() .
    ");
    // The clock moves on between reasoning and tracing.
    std::thread::sleep(std::time::Duration::from_millis(5));
    let response = trace(&mut engine, "current(a) .");
    assert!(response.facts[0].holds);
    let tree = response.facts[0].tree.as_ref().expect("a proof tree");
    check(&mut engine, tree);
}

#[test]
fn an_underivable_fact_reports_not_derived() {
    let mut engine = run("
        edge(a, b) .
        reach(?x, ?y) :- edge(?x, ?y) .
    ");
    let response = trace(&mut engine, "reach(b, a) .");
    assert!(!response.facts[0].holds);
    assert!(response.facts[0].tree.is_none());
    assert_eq!(response.ascii(), " reach(b, a) does not hold\n");
    let list = response.inferences();
    assert!(list.final_conclusions.is_empty());
    assert_eq!(list.not_derived, vec!["reach(b, a)"]);
}

#[test]
fn an_asserted_fact_traces_to_a_leaf() {
    let mut engine = run("edge(a, b) . reach(?x, ?y) :- edge(?x, ?y) .");
    let response = trace(&mut engine, "edge(a, b) .");
    let tree = response.facts[0].tree.as_ref().expect("a proof tree");
    assert!(matches!(
        tree,
        TraceNode::Asserted {
            origin: AssertedOrigin::Program,
            ..
        }
    ));
}

#[test]
fn assignments_name_the_source_variables() {
    let mut engine = run("
        edge(a, b) . edge(b, c) .
        reach(?x, ?z) :- edge(?x, ?y), edge(?y, ?z) .
    ");
    let response = trace(&mut engine, "reach(a, c) .");
    let TraceNode::Derived { assignment, .. } =
        response.facts[0].tree.as_ref().expect("a proof tree")
    else {
        panic!("a derived fact gets a rule node");
    };
    let mut bindings: Vec<(String, String)> = assignment
        .iter()
        .map(|binding| (binding.variable.clone(), binding.value.clone()))
        .collect();
    bindings.sort();
    assert_eq!(
        bindings,
        vec![
            ("?x".to_owned(), "a".to_owned()),
            ("?y".to_owned(), "b".to_owned()),
            ("?z".to_owned(), "c".to_owned()),
        ]
    );
}

#[test]
fn negation_shows_as_absence_patterns() {
    let mut engine = run("
        node(a) . node(b) . node(c) . edge(a, b) .
        isolated(?x) :- node(?x), ~edge(?x, ?y), ~edge(?z, ?x) .
    ");
    let response = trace(&mut engine, "isolated(c) .");
    assert!(response.facts[0].holds);
    let tree = response.facts[0].tree.as_ref().expect("a proof tree");
    check(&mut engine, tree);
    let TraceNode::Derived {
        premises, absences, ..
    } = tree
    else {
        panic!("a derived fact gets a rule node");
    };
    assert_eq!(premises.len(), 1, "one positive premise: node(c)");
    assert_eq!(absences.len(), 2, "both negations appear as absences");
    let rendered: Vec<String> = absences.iter().map(ToString::to_string).collect();
    assert_eq!(
        rendered,
        ["edge(c, ?y)", "edge(?z, c)"],
        "shared columns ground, private ones stay as written"
    );
}

#[test]
fn aggregates_summarize_their_group() {
    let mut engine = run("
        data(g, 1) . data(g, 2) . data(g, 3) . data(h, 5) .
        total(?g, #count(?v)) :- data(?g, ?v) .
    ");
    let response = trace(&mut engine, "total(g, 3) .");
    assert!(response.facts[0].holds);
    let tree = response.facts[0].tree.as_ref().expect("a proof tree");
    check(&mut engine, tree);
    let TraceNode::Derived { aggregate, .. } = tree else {
        panic!("a derived fact gets a rule node");
    };
    assert_eq!(
        aggregate
            .expect("an aggregate node carries its summary")
            .matches,
        3,
        "the group's three rows are summarized, not inlined"
    );
}

#[test]
fn a_min_result_carries_its_witness() {
    let mut engine = run("
        price(book, 7) . price(book, 3) . price(book, 9) .
        cheapest(?item, #min(?p)) :- price(?item, ?p) .
    ");
    let response = trace(&mut engine, "cheapest(book, 3) .");
    let tree = response.facts[0].tree.as_ref().expect("a proof tree");
    check(&mut engine, tree);
    let TraceNode::Derived {
        premises,
        aggregate,
        ..
    } = tree
    else {
        panic!("a derived fact gets a rule node");
    };
    assert_eq!(aggregate.expect("summary").matches, 3);
    assert_eq!(premises.len(), 1, "the minimum's witness row is inlined");
    assert_eq!(premises[0].fact().to_string(), "price(book, 3)");
}

#[test]
fn existential_derivations_trace_through_their_nulls() {
    let mut engine = run("
        person(alice) .
        parent(?x, !y) :- person(?x) .
        parented(?x) :- parent(?x, ?y) .
    ");
    let response = trace(&mut engine, "parented(alice) .");
    assert!(response.facts[0].holds);
    let tree = response.facts[0].tree.as_ref().expect("a proof tree");
    check(&mut engine, tree);
    // The premise is the created fact, whose null renders in data
    // syntax; its own proof grounds the frontier back to the person.
    let TraceNode::Derived { premises, .. } = tree else {
        panic!("a derived fact gets a rule node");
    };
    let created = &premises[0];
    assert!(
        created.fact().terms[1].starts_with("_:"),
        "the null renders as data syntax: {}",
        created.fact()
    );
    let TraceNode::Derived { premises, .. } = created else {
        panic!("the created fact is itself derived");
    };
    assert_eq!(premises[0].fact().to_string(), "person(alice)");
}

#[test]
fn imported_rows_trace_to_their_leaves_and_dirty_engines_refuse() {
    let mut engine = run("
        edge(a, b) .
        reach(?x, ?y) :- edge(?x, ?y) .
        reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
    ");
    engine
        .import_rows("edge", [vec![DataValue::iri("b"), DataValue::iri("c")]])
        .expect("post-fixpoint rows land");

    // Pending updates refuse tracing: traces describe the reasoned
    // store, never a mid-state one.
    let fact = Fact::parse("reach(a, b) .").expect("the fact parses");
    assert!(!engine.is_at_fixpoint());
    let refusal = engine.trace(&[fact]).expect_err("a dirty engine refuses");
    assert_eq!(refusal.code(), nemo::codes::PENDING_UPDATES);

    engine.reason().expect("reasoning continues");
    let response = trace(&mut engine, "reach(a, c) .");
    let tree = response.facts[0].tree.as_ref().expect("a proof tree");
    check(&mut engine, tree);
    fn finds_import_rows(node: &TraceNode) -> bool {
        match node {
            TraceNode::Asserted { origin, .. } => *origin == AssertedOrigin::ImportRows,
            TraceNode::Derived { premises, .. } => premises.iter().any(finds_import_rows),
            TraceNode::Repeated { .. } => false,
        }
    }
    assert!(
        finds_import_rows(tree),
        "the edge added through `import_rows` appears as a leaf of that origin"
    );
}

#[test]
fn rule_names_reach_the_protocol() {
    let mut engine = run("
        edge(a, b) .
        #[name(\"base case\")]
        reach(?x, ?y) :- edge(?x, ?y) .
    ");
    let response = trace(&mut engine, "reach(a, b) .");
    let TraceNode::Derived { rule, .. } = response.facts[0].tree.as_ref().expect("a proof tree")
    else {
        panic!("a derived fact gets a rule node");
    };
    assert_eq!(rule.name.as_deref(), Some("base case"));
    assert!(rule.source.is_some(), "the rule has a source position");
}

#[test]
fn module_rules_attribute_back_to_their_use() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    std::fs::write(
        directory.join("closure.rls"),
        "
        @input edge(from: any, to: any) .
        reach(?x, ?y) :- edge(?x, ?y) .
        reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
        @output reach .
        ",
    )
    .expect("the module writes");
    let mut engine = Engine::new();
    let source = "
        edge(a, b) .
        @use closure { edge = edge } as closure .
        path(?x, ?y) :- closure::reach(?x, ?y) .
        ";
    // A `@use` resolves against the directory of the file it stands in.
    let file = engine
        .add_source(directory.join("main.rls").display().to_string(), source)
        .expect("the text is small");
    let (program, diagnostics) = Program::parse(source, file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let program = engine.link(program).expect("the program links");
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    let response = trace(&mut engine, "path(a, b) .");
    let tree = response.facts[0].tree.as_ref().expect("a proof tree");
    check(&mut engine, tree);
    // The instantiated module rule carries provenance back to the
    // statements it derives from.
    let TraceNode::Derived { premises, .. } = tree else {
        panic!("a derived fact gets a rule node");
    };
    let TraceNode::Derived { rule, .. } = &premises[0] else {
        panic!("the module rule derived the reach fact");
    };
    // It stands in the module file, instantiated by the program's `@use`.
    assert_eq!(rule.instances.len(), 1, "{:?}", rule.instances);
    assert!(
        rule.instances[0].text.starts_with("@use closure"),
        "{:?}",
        rule.instances
    );
    assert!(
        rule.source
            .as_ref()
            .is_some_and(|source| source.file.ends_with("closure.rls")),
        "{:?}",
        rule.source
    );
}

#[test]
fn shared_subderivations_deduplicate_in_the_inference_list() {
    let mut engine = run("
        p(a) .
        q(?x) :- p(?x) .
        r(?x) :- q(?x), p(?x) .
    ");
    let response = trace(&mut engine, "r(a) .");
    let list = response.inferences();
    assert_eq!(list.final_conclusions, vec!["r(a)"]);
    let conclusions: Vec<&str> = list
        .inferences
        .iter()
        .map(|inference| inference.conclusion.as_str())
        .collect();
    assert_eq!(
        conclusions,
        vec!["r(a)", "q(a)", "p(a)"],
        "parents precede children, each conclusion once"
    );
    assert!(
        list.inferences[2].rule.is_none(),
        "asserted facts carry no rule"
    );
}

#[test]
fn a_tree_expands_each_derived_fact_once() {
    let mut engine = run("
        e(1) .
        p(?x) :- e(?x) .
        q(?x) :- p(?x) .
        r(?x) :- p(?x) .
        s(?x) :- q(?x), r(?x) .
    ");
    let response = trace(&mut engine, "s(1) .");
    let tree = response.facts[0].tree.as_ref().expect("s(1) is derived");
    check(&mut engine, tree);
    let TraceNode::Derived { premises, .. } = tree else {
        panic!("s(1) is derived");
    };
    let [
        TraceNode::Derived { premises: q, .. },
        TraceNode::Derived { premises: r, .. },
    ] = premises.as_slice()
    else {
        panic!("q(1) and r(1) are derived");
    };
    assert!(
        matches!(&q[0], TraceNode::Derived { .. }),
        "p(1) expands under q(1)"
    );
    assert!(
        matches!(&r[0], TraceNode::Repeated { fact } if fact.to_string() == "p(1)"),
        "p(1) repeats under r(1)"
    );
    assert!(response.ascii().contains("p(1) (shown above)"));
    let conclusions: Vec<String> = response
        .inferences()
        .inferences
        .into_iter()
        .map(|inference| inference.conclusion)
        .collect();
    assert_eq!(conclusions, ["s(1)", "q(1)", "p(1)", "e(1)", "r(1)"]);
}

#[test]
fn a_proof_sharing_its_premises_stays_as_large_as_its_facts() {
    // Each `d` stands on the one before it twice, once through `f`:
    // unfolded, the proof of d(20) would hold a million nodes.
    let mut engine = run("
        d(0) .
        f(?i) :- d(?i) .
        d(?i + 1) :- d(?i), f(?i), ?i < 20 .
    ");
    let response = trace(&mut engine, "d(20) .");
    let tree = response.facts[0].tree.as_ref().expect("d(20) is derived");
    check(&mut engine, tree);
    let size = serde_json::to_string(&response)
        .expect("the response serializes")
        .len();
    assert!(size < 100_000, "the response takes {size} bytes");
}

#[test]
fn responses_serialize_to_one_json_format() {
    let mut engine = run("edge(a, b) . reach(?x, ?y) :- edge(?x, ?y) .");
    let response = trace(&mut engine, "reach(a, b) .");
    let json = serde_json::to_string(&response).expect("the response serializes");
    assert!(json.contains("\"version\":1"), "{json}");
    assert!(json.contains("\"kind\":\"derived\""), "{json}");
    let back: TraceResponse = serde_json::from_str(&json).expect("the response round-trips");
    assert_eq!(back.facts.len(), 1);
}

/// A trace names facts by their written columns.
/// An engine that retains its imports stores them that way,
/// and the leaf of an imported fact is the fact as written.
#[test]
fn imported_facts_trace_at_their_written_width_when_retained() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_tracing_retained.csv");
    std::fs::write(&path, "a,label,7\nb,other,2\n").expect("the temp file writes");
    let source =
        "@import p from csv{resource=\"nemo_tracing_retained.csv\", format=(string,string,int)} .
big(?x) :- p(?x, ?label, ?n), ?n > 5 .
";
    let mut engine = Engine::new();
    engine.retain_imports();
    let program = parse_into(&mut engine, source);
    engine.set_import_directory(directory);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    let response = trace(&mut engine, "big(\"a\") .");
    assert!(response.facts[0].holds);
    let tree = response.facts[0].tree.as_ref().expect("a proof tree");
    check(&mut engine, tree);
    let TraceNode::Derived { premises, .. } = tree else {
        panic!("a derived fact: {tree:?}");
    };
    assert_eq!(
        premises[0].fact().to_string(),
        "p(\"a\", \"label\", 7)",
        "the leaf is the imported fact as written"
    );
    assert_eq!(engine.count("p").expect("the import stands"), 2);
}

/// Without retained imports, a column no rule reads is never stored:
/// the leaf shows the written variable at its position.
#[test]
fn unread_import_columns_trace_as_written() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_tracing_pruned.csv");
    std::fs::write(
        &path,
        "a,label,7
b,other,2
",
    )
    .expect("the temp file writes");
    let source =
        "@import p from csv{resource=\"nemo_tracing_pruned.csv\", format=(string,string,int)} .
big(?x) :- p(?x, ?label, ?n), ?n > 5 .
@output big .
";
    let mut engine = Engine::new();
    let program = parse_into(&mut engine, source);
    engine.set_import_directory(directory);
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    let response = trace(&mut engine, "big(\"a\") .");
    let TraceNode::Derived {
        premises,
        assignment,
        ..
    } = response.facts[0].tree.as_ref().expect("a proof tree")
    else {
        panic!("a derived fact gets a rule node");
    };
    assert_eq!(premises[0].fact().to_string(), "p(\"a\", ?label, 7)");
    assert!(
        assignment
            .iter()
            .all(|binding| binding.variable != "?label"),
        "the run never had the label"
    );
}

/// Tracing by stored rows pages through a predicate: every page's
/// proofs check, the pages add up to the predicate, and paging over
/// several predicates hands each page to the sink as it is ready.
#[test]
fn stored_rows_trace_page_by_page() {
    let mut engine = run("
        edge(a, b) . edge(b, c) . edge(c, d) .
        reach(?x, ?y) :- edge(?x, ?y) .
        reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
        far(?x) :- reach(?x, d) .
    ");
    let total = engine.count("reach").expect("the predicate exists");
    assert_eq!(total, 6);

    let mut traced = 0;
    let mut start = 0;
    loop {
        let response = engine.trace_rows("reach", start, 4).expect("a page traces");
        if response.facts.is_empty() {
            break;
        }
        for fact in &response.facts {
            assert!(fact.holds, "a stored row holds: {fact:?}");
            check(&mut engine, fact.tree.as_ref().expect("a proof tree"));
        }
        traced += response.facts.len();
        start += response.facts.len();
    }
    assert_eq!(traced, total, "the pages add up to the predicate");

    let mut pages = Vec::new();
    engine
        .trace_predicates(
            &[String::from("far"), String::from("reach")],
            4,
            |response| {
                pages.push(response.facts.len());
                Ok(())
            },
        )
        .expect("every page traces");
    assert_eq!(pages, [3, 4, 2], "far in one page, reach in two");

    let refusal = engine
        .trace_rows("nowhere", 0, 4)
        .expect_err("an unknown predicate is refused");
    assert_eq!(
        refusal.code(),
        nemo::codes::UNKNOWN_PREDICATE,
        "{refusal:?}"
    );
    assert_eq!(refusal.field("predicate"), Some("nowhere"));
}

/// A traced fact naming a blank node the program never created is a
/// refusal by name, not a bug report.
#[test]
fn blank_nodes_the_program_never_created_are_refused_by_name() {
    let mut engine = run("p(_:a) . q(?x) :- p(?x) .");
    let fact = Fact::parse("q(_:nope) .").expect("a fact");
    let problem = engine.trace(&[fact]).expect_err("no such blank node");
    assert_eq!(
        problem.code(),
        nemo::codes::UNKNOWN_BLANK_NODE,
        "{problem:?}"
    );
}
