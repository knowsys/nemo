//! Tracing: derivations of facts, found by grounded backward search.
//!
//! The oracle is a proof checker over the written program:
//! every node's fact must hold in the store,
//! every derivation must be an instance of its written rule
//! (the head and each premise its literal under the assignment),
//! and every premise must come before the node it supports.
//! A response that passes is a valid proof.

mod query;

use std::collections::HashMap;

use nemo::{
    engine::Engine,
    model::{Atom, Fact, Literal, Program, Rule, TermKind, VariableKind},
    trace::{Proof, TraceResponse},
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

/// Splits a fact in term syntax into its predicate and its terms.
fn split_fact(fact: &str) -> (String, Vec<String>) {
    let open = if fact.starts_with('<') {
        fact.find('>').expect("a closed predicate") + 1
    } else {
        fact.find('(').expect("an argument list")
    };
    let predicate = fact[..open]
        .trim_start_matches('<')
        .trim_end_matches('>')
        .to_owned();
    let inner = &fact[open + 1..fact.len() - 1];
    let mut terms = Vec::new();
    let (mut depth, mut quoted, mut escaped, mut start) = (0, false, false, 0);
    for (position, character) in inner.char_indices() {
        match character {
            _ if escaped => escaped = false,
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            '(' | '[' | '{' | '<' if !quoted => depth += 1,
            ')' | ']' | '}' | '>' if !quoted => depth -= 1,
            ',' if !quoted && depth == 0 => {
                terms.push(inner[start..position].trim().to_owned());
                start = position + 1;
            }
            _ => {}
        }
    }
    if !inner.trim().is_empty() {
        terms.push(inner[start..].trim().to_owned());
    }
    (predicate, terms)
}

/// Checks a response, validating every node against the engine and the written program:
/// facts hold, derivations are instances of their written rules,
/// and premises come before the nodes they support.
fn check(engine: &mut Engine, response: &TraceResponse) {
    for traced in &response.facts {
        if let Some(node) = traced.node {
            assert_eq!(
                response.nodes[node].fact, traced.fact,
                "a fact's node is the fact"
            );
        }
    }
    for id in 0..response.nodes.len() {
        check_node(engine, response.node_proof(id));
    }
}

/// Checks one node of a response, as [check] describes.
fn check_node(engine: &mut Engine, proof: Proof<'_>) {
    let (predicate, terms) = split_fact(proof.fact());
    let rows = stored(engine, &predicate);
    assert!(
        rows.contains(&terms),
        "{} is claimed but not stored",
        proof.fact()
    );
    let (Some(rule), Some(derivation)) = (proof.rule(), proof.derivation()) else {
        return;
    };
    assert!(
        derivation.step >= 1,
        "derivations happen at reasoning steps"
    );
    let parsed = Rule::parse(&rule.text).expect("rule texts re-parse");
    let values: HashMap<String, String> = derivation
        .assignment
        .iter()
        .map(|binding| (binding.variable.clone(), binding.value.clone()))
        .collect();
    assert!(
        parsed
            .head
            .iter()
            .any(|atom| instance_of(atom, proof.fact(), &values)),
        "{} is not an instance of a head of {}",
        proof.fact(),
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
    let premises: Vec<Proof<'_>> = proof.premises().collect();
    if derivation.aggregate.is_some() {
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
            premise.id() < proof.id(),
            "the premise {} comes before {}",
            premise.fact(),
            proof.fact()
        );
        assert!(
            instance_of(atom, premise.fact(), &values),
            "{} is not an instance of `{atom}` in {}",
            premise.fact(),
            rule.text
        );
    }
    assert_eq!(derivation.absences.len(), negatives.len());
    for (absence, atom) in derivation.absences.iter().zip(&negatives) {
        assert!(
            instance_of(atom, absence, &values),
            "{absence} is not an instance of `~{atom}` in {}",
            rule.text
        );
    }
}

/// Whether a fact is an instance of a written atom under an
/// assignment: same predicate, and every variable the assignment binds
/// takes its value. Named arguments and constants are left to the
/// engine.
fn instance_of(atom: &Atom, fact: &str, values: &HashMap<String, String>) -> bool {
    let (predicate, terms) = split_fact(fact);
    let written = atom.predicate.to_string();
    if written.trim_start_matches('<').trim_end_matches('>') != predicate {
        return false;
    }
    if atom.labels.is_some() {
        return true;
    }
    atom.terms.len() == terms.len()
        && atom.terms.iter().zip(&terms).all(|(term, value)| {
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
fn a_chain_derivation_traces_to_its_loaded_leaves() {
    let mut engine = run("
        edge(a, b) . edge(b, c) . edge(c, d) .
        reach(?x, ?y) :- edge(?x, ?y) .
        reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
    ");
    let response = trace(&mut engine, "reach(a, d) .");
    assert!(response.proof(0).is_some());
    check(&mut engine, &response);

    // The chain bottoms out in the loaded edges, which no rule derives.
    let leaves: Vec<&str> = response
        .nodes
        .iter()
        .filter(|node| node.derivation.is_none())
        .map(|node| node.fact.as_str())
        .collect();
    assert_eq!(leaves, ["edge(a, b)", "edge(b, c)", "edge(c, d)"]);

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
    assert!(response.proof(0).is_some());
    check(&mut engine, &response);
}

#[test]
fn an_underivable_fact_reports_not_derived() {
    let mut engine = run("
        edge(a, b) .
        reach(?x, ?y) :- edge(?x, ?y) .
    ");
    let response = trace(&mut engine, "reach(b, a) .");
    assert_eq!(response.facts[0].node, None);
    assert!(response.nodes.is_empty());
    assert_eq!(response.ascii(), " reach(b, a) does not hold\n");
}

#[test]
fn a_loaded_fact_traces_to_a_leaf() {
    let mut engine = run("edge(a, b) . reach(?x, ?y) :- edge(?x, ?y) .");
    let response = trace(&mut engine, "edge(a, b) .");
    let proof = response.proof(0).expect("the fact holds");
    assert!(proof.derivation().is_none());
    assert!(response.rules.is_empty());
}

#[test]
fn assignments_name_the_source_variables() {
    let mut engine = run("
        edge(a, b) . edge(b, c) .
        reach(?x, ?z) :- edge(?x, ?y), edge(?y, ?z) .
    ");
    let response = trace(&mut engine, "reach(a, c) .");
    let derivation = response
        .proof(0)
        .and_then(|proof| proof.derivation())
        .expect("a derived fact");
    let mut bindings: Vec<(String, String)> = derivation
        .assignment
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
    check(&mut engine, &response);
    let proof = response.proof(0).expect("the fact holds");
    assert_eq!(proof.premises().count(), 1, "one positive premise: node(c)");
    let absences = &proof.derivation().expect("a derived fact").absences;
    assert_eq!(
        absences,
        &["edge(c, ?y)", "edge(?z, c)"],
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
    check(&mut engine, &response);
    let derivation = response
        .proof(0)
        .and_then(|proof| proof.derivation())
        .expect("a derived fact");
    assert_eq!(
        derivation
            .aggregate
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
    check(&mut engine, &response);
    let proof = response.proof(0).expect("the fact holds");
    let summary = proof
        .derivation()
        .and_then(|derivation| derivation.aggregate);
    assert_eq!(summary.expect("summary").matches, 3);
    let premises: Vec<&str> = proof.premises().map(|premise| premise.fact()).collect();
    assert_eq!(
        premises,
        ["price(book, 3)"],
        "the minimum's witness row is inlined"
    );
}

#[test]
fn existential_derivations_trace_through_their_nulls() {
    let mut engine = run("
        person(alice) .
        parent(?x, !y) :- person(?x) .
        parented(?x) :- parent(?x, ?y) .
    ");
    let response = trace(&mut engine, "parented(alice) .");
    check(&mut engine, &response);
    // The premise is the created fact, whose null renders in data
    // syntax; its own proof grounds the frontier back to the person.
    let proof = response.proof(0).expect("the fact holds");
    let created = proof.premises().next().expect("a premise");
    assert!(
        split_fact(created.fact()).1[1].starts_with("_:"),
        "the null renders as data syntax: {}",
        created.fact()
    );
    assert!(
        created.rule().is_some(),
        "the created fact is itself derived"
    );
    let premises: Vec<&str> = created.premises().map(|premise| premise.fact()).collect();
    assert_eq!(premises, ["person(alice)"]);
}

#[test]
fn added_rows_trace_to_their_leaves_and_dirty_engines_refuse() {
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
    check(&mut engine, &response);
    assert!(
        response
            .nodes
            .iter()
            .any(|node| node.fact == "edge(b, c)" && node.derivation.is_none()),
        "the edge added through `import_rows` appears as a leaf"
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
    let rule = response
        .proof(0)
        .and_then(|proof| proof.rule())
        .expect("a derived fact");
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
    let (program, diagnostics) = Program::parse_with_file(source, file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let program = engine.link(program).expect("the program links");
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    let response = trace(&mut engine, "path(a, b) .");
    check(&mut engine, &response);
    // The instantiated module rule carries provenance back to the
    // statements it derives from.
    let proof = response.proof(0).expect("the fact holds");
    let rule = proof
        .premises()
        .next()
        .and_then(|premise| premise.rule())
        .expect("the module rule derived the reach fact");
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
fn a_shared_premise_is_one_node() {
    let mut engine = run("
        p(a) .
        q(?x) :- p(?x) .
        r(?x) :- q(?x), p(?x) .
    ");
    let response = trace(&mut engine, "r(a) .");
    check(&mut engine, &response);
    let facts: Vec<&str> = response
        .nodes
        .iter()
        .map(|node| node.fact.as_str())
        .collect();
    assert_eq!(
        facts,
        ["p(a)", "q(a)", "r(a)"],
        "premises come first, each fact once"
    );
    assert_eq!(response.facts[0].node, Some(2));
    assert_eq!(response.rules.len(), 2, "each rule is listed once");
}

#[test]
fn a_fact_several_derivations_use_is_one_node() {
    let mut engine = run("
        e(1) .
        p(?x) :- e(?x) .
        q(?x) :- p(?x) .
        r(?x) :- p(?x) .
        s(?x) :- q(?x), r(?x) .
    ");
    let response = trace(&mut engine, "s(1) .");
    check(&mut engine, &response);
    let proof = response.proof(0).expect("s(1) is derived");
    let [q, r]: [Proof<'_>; 2] = proof
        .premises()
        .collect::<Vec<_>>()
        .try_into()
        .expect("two premises");
    let p_under_q = q.premises().next().expect("p(1) under q(1)");
    let p_under_r = r.premises().next().expect("p(1) under r(1)");
    assert_eq!(p_under_q.id(), p_under_r.id(), "p(1) is one node");
    assert!(response.ascii().contains("p(1) (shown above)"));
    let inferences: Vec<String> = (0..response.nodes.len())
        .map(|id| response.node_proof(id))
        .filter(|proof| proof.rule().is_some())
        .map(|proof| proof.to_string())
        .collect();
    assert_eq!(
        inferences,
        [
            "p(1) :- e(1) .",
            "q(1) :- p(1) .",
            "r(1) :- p(1) .",
            "s(1) :- q(1), r(1) ."
        ]
    );
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
    assert!(response.proof(0).is_some(), "d(20) is derived");
    check(&mut engine, &response);
    let size = serde_json::to_string(&response)
        .expect("the response serializes")
        .len();
    assert!(size < 100_000, "the response takes {size} bytes");
}

#[test]
fn recursion_over_a_cyclic_graph_traces_in_polynomial_time() {
    // A random graph of many cycles, whose reachability facts share their blocks with their
    // premises. Each fact is proven from facts added before it, so the cycles cost nothing.
    let mut engine = run("
        edge(n0, n12) . edge(n0, n39) . edge(n1, n34) . edge(n2, n0) . edge(n2, n37) .
        edge(n3, n11) . edge(n5, n27) . edge(n5, n32) . edge(n5, n38) . edge(n6, n12) .
        edge(n7, n16) . edge(n7, n23) . edge(n7, n38) . edge(n8, n31) . edge(n8, n38) .
        edge(n9, n24) . edge(n9, n37) . edge(n10, n1) . edge(n11, n1) . edge(n12, n9) .
        edge(n12, n16) . edge(n12, n17) . edge(n12, n23) . edge(n12, n24) . edge(n12, n26) .
        edge(n13, n33) . edge(n16, n26) . edge(n16, n28) . edge(n16, n39) . edge(n17, n19) .
        edge(n17, n31) . edge(n17, n39) . edge(n18, n6) . edge(n18, n35) . edge(n19, n22) .
        edge(n19, n23) . edge(n20, n1) . edge(n20, n25) . edge(n21, n5) . edge(n21, n33) .
        edge(n21, n35) . edge(n23, n10) . edge(n23, n15) . edge(n23, n26) . edge(n23, n31) .
        edge(n24, n4) . edge(n24, n18) . edge(n24, n33) . edge(n25, n6) . edge(n26, n5) .
        edge(n26, n39) . edge(n27, n38) . edge(n28, n3) . edge(n28, n15) . edge(n28, n31) .
        edge(n29, n20) . edge(n30, n14) . edge(n30, n19) . edge(n30, n23) . edge(n30, n31) .
        edge(n31, n10) . edge(n31, n12) . edge(n31, n16) . edge(n31, n17) . edge(n31, n19) .
        edge(n32, n1) . edge(n32, n28) . edge(n32, n36) . edge(n33, n12) . edge(n33, n27) .
        edge(n36, n5) . edge(n36, n35) . edge(n37, n3) . edge(n37, n7) . edge(n37, n23) .
        edge(n38, n17) . edge(n38, n27) . edge(n38, n30) . edge(n39, n24) . edge(n39, n36) .
        reach(?x, ?y) :- edge(?x, ?y) .
        reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
    ");
    let response = trace(&mut engine, "reach(n29, n14) .");
    assert!(response.proof(0).is_some(), "reach(n29, n14) is derived");
    check(&mut engine, &response);
}

#[test]
fn responses_serialize_to_one_json_format() {
    let mut engine = run("edge(a, b) . reach(?x, ?y) :- edge(?x, ?y) .");
    let response = trace(&mut engine, "reach(a, b) .");
    let json = serde_json::to_string(&response).expect("the response serializes");
    assert!(
        json.starts_with(r#"{"version":1,"facts":[{"fact":"reach(a, b)","node":1}],"nodes":["#),
        "{json}"
    );
    let back: TraceResponse = serde_json::from_str(&json).expect("the response round-trips");
    assert_eq!(back, response);
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
    check(&mut engine, &response);
    let proof = response.proof(0).expect("the fact holds");
    assert_eq!(
        proof.premises().next().map(|premise| premise.fact()),
        Some("p(\"a\", \"label\", 7)"),
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
    let proof = response.proof(0).expect("the fact holds");
    assert_eq!(
        proof.premises().next().map(|premise| premise.fact()),
        Some("p(\"a\", ?label, 7)")
    );
    assert!(
        proof
            .derivation()
            .expect("a derived fact")
            .assignment
            .iter()
            .all(|binding| binding.variable != "?label"),
        "the run never had the label"
    );
}

/// Tracing every stored row of predicates hands out one response in parts:
/// together they check, every row and named fact is one node,
/// and a fact several rows use is proven and listed once.
#[test]
fn stored_rows_trace_into_one_response_in_parts() {
    let mut engine = run("
        edge(a, b) . edge(b, c) . edge(c, d) .
        reach(?x, ?y) :- edge(?x, ?y) .
        reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
        far(?x) :- reach(?x, d) .
    ");
    let named = [
        Fact::parse("reach(a, d) .").expect("the fact parses"),
        Fact::parse("reach(d, a) .").expect("the fact parses"),
    ];
    let mut response = TraceResponse {
        version: nemo::trace::VERSION,
        facts: Vec::new(),
        nodes: Vec::new(),
        rules: Vec::new(),
    };
    let mut parts = 0;
    response.facts = engine
        .trace_predicates(
            &[String::from("far"), String::from("reach")],
            &named,
            |part| {
                parts += 1;
                response.nodes.extend(part.nodes);
                response.rules.extend(part.rules);
                Ok(())
            },
        )
        .expect("every row traces");
    check(&mut engine, &response);
    assert_eq!(
        parts, 3,
        "one part per predicate, and one for the named facts"
    );

    let mut facts: Vec<&str> = response
        .nodes
        .iter()
        .map(|node| node.fact.as_str())
        .collect();
    let listed = facts.len();
    facts.sort_unstable();
    facts.dedup();
    assert_eq!(facts.len(), listed, "each fact is one node");
    // The three far facts, the six reach facts, and the three edges they stand on.
    assert_eq!(listed, 12);
    assert_eq!(response.rules.len(), 3, "each rule is listed once");
    assert!(
        response.facts[0]
            .node
            .is_some_and(|node| response.nodes[node].fact == "reach(a, d)")
    );
    assert_eq!(response.facts[1].node, None);

    let refusal = engine
        .trace_predicates(&[String::from("nowhere")], &[], |_| Ok(()))
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
