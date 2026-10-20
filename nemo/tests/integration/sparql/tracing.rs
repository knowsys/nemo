//! Traces and explanation queries: a fetched fact traces to its bindings, and the rules and
//! remote premises of a rewritten program show as written, never the stage's own predicates.

use nemo::{
    engine::Engine,
    model::Fact,
    trace::{
        TraceNode,
        query::{
            EntriesQuery, EntryQuery, NodeResponse, QueryChildren, QueryNode,
            TableEntriesForTreeNodesQuery, TreeForTableQuery,
        },
    },
};

use crate::common::{Responder, parse_into};

use super::{
    ANCESTORS, ANCESTORS_GENERIC, generic_ancestors, parents, parents_generic, written_only,
};

/// Returns the facts of a query response's node, rendered, with whether each carries an entry
/// id.
///
/// The response must name nothing a stage introduced.
fn node_facts(response: &[NodeResponse], address: &[usize]) -> Vec<(String, bool)> {
    let json = serde_json::to_string(response).expect("the response serializes");
    assert!(
        !json.contains("_query") && !json.contains("_bindings"),
        "a stage's predicate shows: {json}"
    );
    let node = response
        .iter()
        .find(|node| node.address_in_tree == address)
        .unwrap_or_else(|| panic!("no node at {address:?}"));
    node.table_entries
        .entries
        .iter()
        .map(|entry| {
            (
                format!("{}({})", node.predicate, entry.term_tuple.join(", ")),
                entry.entry_id.is_some(),
            )
        })
        .collect()
}

/// A selection of facts by their terms.
fn selecting(terms: &str) -> Option<EntriesQuery> {
    Some(EntriesQuery {
        queries: vec![EntryQuery::Terms(terms.into())],
        pagination: None,
    })
}

#[test]
fn fetched_facts_trace_to_their_bindings() {
    let mut engine = Engine::new();
    let endpoint = parents();
    // A trace names facts by their written columns, so the import
    // passes must not prune the column no rule reads.
    engine.retain_imports();
    let program = parse_into(&mut engine, ANCESTORS);
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    let fact = Fact::parse("parent(<urn:b>, <urn:c>) .").expect("parses");
    let response = engine.trace(&[fact]).expect("tracing answers");
    assert!(response.facts[0].holds);
    let TraceNode::Derived {
        rule,
        premises,
        assignment,
        ..
    } = response.facts[0].tree.as_ref().expect("a proof tree")
    else {
        panic!("a fetched fact is explained by its import");
    };
    // The rule is the import directive, with a source position.
    assert!(
        rule.text.starts_with("@import parent(?c, ?p) from sparql"),
        "{}",
        rule.text
    );
    assert!(rule.source.is_some());
    // Its one premise is the binding row the endpoint was asked with.
    assert_eq!(premises.len(), 1);
    assert_eq!(premises[0].fact().to_string(), "anc(<urn:b>)");
    let mut bindings: Vec<(String, String)> = assignment
        .iter()
        .map(|binding| (binding.variable.clone(), binding.value.clone()))
        .collect();
    bindings.sort();
    assert_eq!(
        bindings,
        vec![
            ("?c".to_owned(), "<urn:b>".to_owned()),
            ("?p".to_owned(), "<urn:c>".to_owned()),
        ]
    );
}

#[test]
fn rewritten_rules_trace_as_written() {
    let mut engine = Engine::new();
    let endpoint = parents_generic();
    let program = parse_into(&mut engine, ANCESTORS_GENERIC);
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    let fact = Fact::parse("anc(<urn:c>) .").expect("parses");
    let response = engine.trace(&[fact]).expect("tracing answers");
    written_only(&response);
    let TraceNode::Derived { rule, premises, .. } =
        response.facts[0].tree.as_ref().expect("a proof tree")
    else {
        panic!("a derived fact gets a rule node");
    };
    // The rule as written, by its position among the written statements.
    assert_eq!(
        rule.text,
        "anc(?p) :- anc(?c), triple(?c, <urn:parent>, ?p) ."
    );
    assert_eq!(rule.rule, 2);
    // One premise per written literal: the local one derived, the remote
    // one matched at the endpoint.
    assert_eq!(premises.len(), 2);
    assert_eq!(premises[0].fact().to_string(), "anc(<urn:b>)");
    assert!(matches!(premises[0], TraceNode::Derived { .. }));
    assert_eq!(
        premises[1].fact().to_string(),
        "triple(<urn:b>, <urn:parent>, <urn:c>)"
    );
    let TraceNode::Asserted { defined_at, .. } = &premises[1] else {
        panic!("a remote premise is a leaf");
    };
    assert_eq!(defined_at.len(), 1, "the written import supplies it");
}

#[test]
fn values_the_endpoint_never_returned_show_as_written() {
    let fetcher = Responder::endpoint(|text| {
        assert!(text.starts_with("SELECT ?e ?n"), "{text}");
        String::from("?e\t?n\n<urn:e>\t<urn:n>\n")
    });
    let mut engine = Engine::new();
    let endpoint = fetcher;
    let program = parse_into(
        &mut engine,
        r#"
@import triple from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
} .
event(<urn:e>) .
next(?e, ?n) :- event(?e), triple(?e, <urn:p1>, ?stmt), triple(?stmt, <urn:p2>, ?n) .
@output next .
"#,
    );
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    let fact = Fact::parse("next(<urn:e>, <urn:n>) .").expect("parses");
    let response = engine.trace(&[fact]).expect("tracing answers");
    written_only(&response);
    let TraceNode::Derived {
        premises,
        assignment,
        ..
    } = response.facts[0].tree.as_ref().expect("a proof tree")
    else {
        panic!("a derived fact gets a rule node");
    };
    let premises: Vec<String> = premises
        .iter()
        .map(|premise| premise.fact().to_string())
        .collect();
    assert_eq!(
        premises,
        [
            "event(<urn:e>)",
            "triple(<urn:e>, <urn:p1>, ?stmt)",
            "triple(?stmt, <urn:p2>, <urn:n>)",
        ]
    );
    // The assignment lists what the run had, and ?stmt it never had.
    let mut variables: Vec<&str> = assignment
        .iter()
        .map(|binding| binding.variable.as_str())
        .collect();
    variables.sort_unstable();
    assert_eq!(variables, ["?e", "?n"]);
}

#[test]
fn a_rule_folded_into_its_import_traces_as_a_derivation() {
    let fetcher = Responder::endpoint(|text| {
        assert!(text.starts_with("SELECT ?e"), "{text}");
        String::from("?e\n<urn:x>\n")
    });
    let mut engine = Engine::new();
    let endpoint = fetcher;
    let program = parse_into(
        &mut engine,
        r#"
@import triple from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
} .
event(?e) :- triple(?e, <urn:type>, <urn:Event>) .
@output event .
"#,
    );
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    let fact = Fact::parse("event(<urn:x>) .").expect("parses");
    let response = engine.trace(&[fact]).expect("tracing answers");
    written_only(&response);
    let TraceNode::Derived {
        rule,
        premises,
        step,
        ..
    } = response.facts[0].tree.as_ref().expect("a proof tree")
    else {
        panic!("the import replaced the written rule");
    };
    assert_eq!(
        rule.text,
        "event(?e) :- triple(?e, <urn:type>, <urn:Event>) ."
    );
    assert_eq!(*step, 0, "the import was fetched while loading");
    assert_eq!(premises.len(), 1);
    assert_eq!(
        premises[0].fact().to_string(),
        "triple(<urn:x>, <urn:type>, <urn:Event>)"
    );
}

#[test]
fn predicates_a_stage_introduced_cannot_be_traced() {
    let mut engine = Engine::new();
    let endpoint = parents_generic();
    let program = parse_into(&mut engine, ANCESTORS_GENERIC);
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    let fact = Fact::parse("triple_query1(<urn:a>, <urn:b>) .").expect("parses");
    let problem = engine.trace(&[fact]).expect_err("no such predicate");
    assert_eq!(
        problem.code(),
        nemo::codes::UNKNOWN_PREDICATE,
        "{problem:?}"
    );
    assert_eq!(problem.field("predicate"), Some("triple_query1"));
}

#[test]
fn shaped_queries_name_the_written_rules_of_a_rewritten_program() {
    let mut engine = generic_ancestors();
    let response = engine
        .table_entries_for_tree_nodes(TableEntriesForTreeNodesQuery {
            predicate: "anc".into(),
            table_entries: selecting("<urn:c>"),
            // The recursive rule, the third written statement.
            child_information: Some(QueryChildren {
                rule: 2,
                head_index: 0,
                children: Vec::new(),
            }),
        })
        .expect("the query answers");
    assert_eq!(
        node_facts(&response, &[]),
        [("anc(<urn:c>)".to_owned(), true)]
    );
    assert_eq!(
        node_facts(&response, &[0]),
        [("anc(<urn:b>)".to_owned(), true)]
    );
    // The remote premise, matched at the endpoint and never stored.
    assert_eq!(
        node_facts(&response, &[1]),
        [("triple(<urn:b>, <urn:parent>, <urn:c>)".to_owned(), false)]
    );
}

#[test]
fn shapeless_queries_expand_rewritten_rules_as_written() {
    let mut engine = generic_ancestors();
    let response = engine
        .tree_for_table(TreeForTableQuery {
            predicate: "anc".into(),
            table_entries: selecting("<urn:b>"),
        })
        .expect("the query answers");
    let json = serde_json::to_string(&response).expect("the response serializes");
    assert!(
        !json.contains("_query") && !json.contains("_bindings"),
        "a stage's predicate shows: {json}"
    );
    let expansion = response
        .child_information
        .as_ref()
        .expect("the server chose a rule");
    assert_eq!(expansion.rule.id, 2);
    // Only the written rule reads and writes anc: no binding rule.
    let above: Vec<usize> = response
        .possible_rules_above
        .iter()
        .map(|rule| rule.id)
        .collect();
    assert_eq!(above, [2]);
    let facts = |index: usize| -> Vec<(String, bool)> {
        let node = &expansion.children[index];
        node.table_entries
            .entries
            .iter()
            .map(|entry| {
                (
                    format!("{}({})", node.predicate, entry.term_tuple.join(", ")),
                    entry.entry_id.is_some(),
                )
            })
            .collect()
    };
    assert_eq!(facts(0), [("anc(<urn:a>)".to_owned(), true)]);
    assert_eq!(
        facts(1),
        [("triple(<urn:a>, <urn:parent>, <urn:b>)".to_owned(), false)]
    );
}

#[test]
fn remote_premises_take_no_selectors() {
    let mut engine = generic_ancestors();
    let problem = engine
        .table_entries_for_tree_nodes(TableEntriesForTreeNodesQuery {
            predicate: "anc".into(),
            table_entries: None,
            child_information: Some(QueryChildren {
                rule: 2,
                head_index: 0,
                children: vec![
                    QueryNode::default(),
                    QueryNode {
                        table_entries: Some(EntriesQuery {
                            queries: vec![EntryQuery::Entry(0)],
                            pagination: None,
                        }),
                        child_information: None,
                    },
                ],
            }),
        })
        .expect_err("a remote premise is not stored");
    assert_eq!(problem.code(), nemo::codes::REMOTE_SELECTORS, "{problem:?}");
}
