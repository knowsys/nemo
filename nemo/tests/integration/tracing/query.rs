//! The explanation view's queries: `tableEntriesForTreeNodes` runs
//! the valid/filter two-pass over the shape it is given,
//! `treeForTable` expands server-side.

use nemo::{
    engine::Engine,
    trace::query::{
        EntriesQuery, EntryQuery, NodeResponse, Pagination, QueryChildren, QueryNode,
        TableEntriesForTreeNodesQuery, TreeForTableQuery, TreeForTableResponse,
    },
};

use crate::common::run;

/// A chain program: three edges, reach as their closure.
fn chain() -> Engine {
    run("
        edge(a, b) . edge(b, c) . edge(a, d) .
        reach(?x, ?y) :- edge(?x, ?y) .
        reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
    ")
}

/// A selection of the given facts.
fn selecting(queries: Vec<EntryQuery>) -> Option<EntriesQuery> {
    Some(EntriesQuery {
        queries,
        pagination: None,
    })
}

/// A `treeForTable` query over a predicate, selecting the given
/// facts.
fn tree(predicate: &str, queries: Vec<EntryQuery>) -> TreeForTableQuery {
    TreeForTableQuery {
        predicate: predicate.into(),
        table_entries: selecting(queries),
    }
}

/// A node's facts, rendered.
fn rendered(predicate: &str, node: &nemo::trace::query::TableEntries) -> Vec<String> {
    node.entries
        .iter()
        .map(|entry| format!("{predicate}({})", entry.term_tuple.join(", ")))
        .collect()
}

/// The facts of the tree node at the given address, rendered.
fn tree_facts(response: &TreeForTableResponse, address: &[usize]) -> Vec<String> {
    let mut node = response;
    for &index in address {
        node = &node
            .child_information
            .as_ref()
            .unwrap_or_else(|| panic!("no children above {address:?}"))
            .children[index];
    }
    rendered(&node.predicate, &node.table_entries)
}

/// The facts of the listed node at the given address, rendered.
fn node_facts(response: &[NodeResponse], address: &[usize]) -> Vec<String> {
    let node = response
        .iter()
        .find(|node| node.address_in_tree == address)
        .unwrap_or_else(|| panic!("no node at {address:?}"));
    rendered(&node.predicate, &node.table_entries)
}

#[test]
fn a_tree_query_expands_at_the_instant_of_the_run() {
    let mut engine = run("
        item(a) .
        stamped(?x, ?t) :- item(?x), ?t = NOW() .
        current(?x) :- stamped(?x, ?t), ?t = NOW() .
    ");
    // The clock moves on between reasoning and the query.
    std::thread::sleep(std::time::Duration::from_millis(5));
    let response = engine
        .tree_for_table(tree("current", vec![EntryQuery::Terms("a".into())]))
        .expect("the query answers");
    assert!(
        response.child_information.is_some(),
        "the rule derives the fact"
    );
}

#[test]
fn a_tree_query_expands_to_the_asserted_edges() {
    let mut engine = chain();
    let response = engine
        .tree_for_table(tree("reach", vec![EntryQuery::Terms("a, c".into())]))
        .expect("the query answers");
    // reach(a, c) comes only from the recursive rule; its premises
    // are reach(a, b) and edge(b, c), and reach(a, b) expands once
    // more to edge(a, b).
    assert_eq!(tree_facts(&response, &[]), ["reach(a, c)"]);
    let expansion = response
        .child_information
        .as_ref()
        .expect("the server chose a rule");
    assert_eq!(expansion.rule.id, 4, "the recursive rule, statement 4");
    assert_eq!(expansion.rule.relevant_head_predicate.name, "reach");
    assert_eq!(
        expansion.rule.relevant_head_predicate.parameters,
        ["?x", "?z"]
    );
    assert_eq!(tree_facts(&response, &[1]), ["edge(b, c)"]);
    assert_eq!(tree_facts(&response, &[0]), ["reach(a, b)"]);
    assert_eq!(tree_facts(&response, &[0, 0]), ["edge(a, b)"]);
    // The asserted edge is not derivable by any rule: expansion
    // stops there.
    let leaf = &response.child_information.as_ref().unwrap().children[0]
        .child_information
        .as_ref()
        .unwrap()
        .children[0];
    assert!(leaf.child_information.is_none());
    let above: Vec<usize> = leaf
        .possible_rules_above
        .iter()
        .map(|rule| rule.id)
        .collect();
    assert_eq!(above, [3, 4], "edge feeds both reach rules");
    assert!(leaf.possible_rules_below.is_empty());
}

#[test]
fn a_tree_over_two_recursive_premises_stays_bounded() {
    // Each node over the cycle needs both recursive premises again,
    // so the tree doubles on every level down to the depth bound.
    let mut engine = run("
        e(a, b) . e(b, a) .
        t(?x, ?y) :- e(?x, ?y) .
        t(?x, ?z) :- t(?x, ?y), t(?y, ?z) .
    ");
    let response = engine
        .tree_for_table(tree("t", vec![EntryQuery::Terms("a, a".into())]))
        .expect("the query answers");
    /// Counts the nodes of a tree.
    fn nodes(node: &TreeForTableResponse) -> usize {
        1 + node
            .child_information
            .as_ref()
            .map_or(0, |children| children.children.iter().map(nodes).sum())
    }
    assert!(nodes(&response) <= 2048, "{} nodes", nodes(&response));
    assert_eq!(tree_facts(&response, &[]), ["t(a, a)"]);
}

#[test]
fn a_rule_that_can_never_fire_explains_nothing() {
    let mut engine = run("
        r(a) .
        p(?x) :- r(?x), 1 > 2 .
        p(?x) :- r(?x) .
        never(?x) :- r(?x), 1 > 2 .
    ");
    let response = engine
        .tree_for_table(tree("p", vec![EntryQuery::Terms("a".into())]))
        .expect("the query answers");
    let expansion = response
        .child_information
        .as_ref()
        .expect("the server chose a rule");
    assert_eq!(expansion.rule.id, 2, "the rule that fires, statement 2");

    let response = engine
        .tree_for_table(tree("never", Vec::new()))
        .expect("the query answers");
    assert!(tree_facts(&response, &[]).is_empty(), "no fact holds");
}

#[test]
fn a_shaped_query_filters_top_down() {
    let mut engine = chain();
    // Constrain the *premise* to edge(a, b): only reach facts
    // derived from it survive at the root.
    let response = engine
        .table_entries_for_tree_nodes(TableEntriesForTreeNodesQuery {
            predicate: "reach".into(),
            table_entries: None,
            // The base rule: the fourth written statement.
            child_information: Some(QueryChildren {
                rule: 3,
                head_index: 0,
                children: vec![QueryNode {
                    table_entries: selecting(vec![EntryQuery::Terms("a, b".into())]),
                    child_information: None,
                }],
            }),
        })
        .expect("the query answers");
    assert_eq!(node_facts(&response, &[]), ["reach(a, b)"]);
    assert_eq!(node_facts(&response, &[0]), ["edge(a, b)"]);
    // Parents come before their children.
    assert_eq!(response[0].address_in_tree, Vec::<usize>::new());
}

#[test]
fn a_root_without_a_shape_stands_alone() {
    let mut engine = chain();
    let response = engine
        .table_entries_for_tree_nodes(TableEntriesForTreeNodesQuery {
            predicate: "reach".into(),
            table_entries: selecting(vec![EntryQuery::Terms("a, c".into())]),
            child_information: None,
        })
        .expect("the query answers");
    assert_eq!(response.len(), 1, "the server does not expand it");
    assert_eq!(node_facts(&response, &[]), ["reach(a, c)"]);
}

#[test]
fn negated_literals_list_their_predicate() {
    let mut engine = run("
        node(a) . node(b) . node(c) . edge(a, b) .
        isolated(?x) :- node(?x), ~edge(?x, ?y), ~edge(?z, ?x) .
    ");
    let response = engine
        .tree_for_table(tree("isolated", vec![EntryQuery::Terms("c".into())]))
        .expect("the query answers");
    let expansion = response.child_information.as_ref().expect("an expansion");
    let body: Vec<&str> = expansion
        .rule
        .body_predicates
        .iter()
        .map(|atom| atom.name.as_str())
        .collect();
    assert_eq!(body, ["node", "~edge", "~edge"]);
    // The listings follow the positive premise, each holding the
    // negated predicate's facts with their entry ids.
    assert_eq!(expansion.children.len(), 3);
    assert_eq!(tree_facts(&response, &[0]), ["node(c)"]);
    for listing in [1, 2] {
        assert_eq!(tree_facts(&response, &[listing]), ["edge(a, b)"]);
        let node = &expansion.children[listing];
        assert_eq!(node.table_entries.entries[0].entry_id, Some(0));
        assert!(node.possible_rules_above.is_empty());
    }

    // A listing pages and selects, but takes no expansion.
    let shaped = |listing: QueryNode| TableEntriesForTreeNodesQuery {
        predicate: "isolated".into(),
        table_entries: None,
        child_information: Some(QueryChildren {
            rule: 4,
            head_index: 0,
            children: vec![QueryNode::default(), listing],
        }),
    };
    let response = engine
        .table_entries_for_tree_nodes(shaped(QueryNode {
            table_entries: Some(EntriesQuery {
                queries: Vec::new(),
                pagination: Some(Pagination { start: 0, count: 0 }),
            }),
            child_information: None,
        }))
        .expect("the query answers");
    let listing = response
        .iter()
        .find(|node| node.address_in_tree == [1])
        .expect("the first listing");
    assert!(listing.table_entries.entries.is_empty());
    assert!(listing.table_entries.pagination.more_entries_exist);
    let refusal = engine
        .table_entries_for_tree_nodes(shaped(QueryNode {
            table_entries: None,
            child_information: Some(QueryChildren {
                rule: 3,
                head_index: 0,
                children: Vec::new(),
            }),
        }))
        .expect_err("a listing is not expanded");
    assert_eq!(refusal.code(), nemo::codes::NEGATED_EXPANSION);
}

#[test]
fn pagination_is_honest() {
    let mut engine = chain();
    let response = engine
        .table_entries_for_tree_nodes(TableEntriesForTreeNodesQuery {
            predicate: "edge".into(),
            table_entries: Some(EntriesQuery {
                queries: Vec::new(),
                pagination: Some(Pagination { start: 0, count: 2 }),
            }),
            child_information: None,
        })
        .expect("the query answers");
    let root = &response[0].table_entries;
    assert_eq!(root.entries.len(), 2);
    assert!(root.pagination.more_entries_exist);
    let response = engine
        .table_entries_for_tree_nodes(TableEntriesForTreeNodesQuery {
            predicate: "edge".into(),
            table_entries: Some(EntriesQuery {
                queries: Vec::new(),
                pagination: Some(Pagination { start: 2, count: 2 }),
            }),
            child_information: None,
        })
        .expect("the query answers");
    let root = &response[0].table_entries;
    assert_eq!(root.entries.len(), 1);
    assert_eq!(root.pagination.start, 2);
    assert!(!root.pagination.more_entries_exist);
}

#[test]
fn entry_ids_select_the_same_fact() {
    let mut engine = chain();
    let by_terms = engine
        .tree_for_table(tree("edge", vec![EntryQuery::Terms("b, c".into())]))
        .expect("the query answers");
    let id = by_terms.table_entries.entries[0]
        .entry_id
        .expect("stored facts carry ids");
    let by_id = engine
        .tree_for_table(tree("edge", vec![EntryQuery::Entry(id)]))
        .expect("the query answers");
    assert_eq!(
        by_id.table_entries.entries[0].term_tuple,
        by_terms.table_entries.entries[0].term_tuple
    );
}

#[test]
fn selections_that_do_not_fit_are_refused() {
    let mut engine = chain();
    let refusal = engine
        .tree_for_table(tree("edge", vec![EntryQuery::Terms("a, (".into())]))
        .expect_err("the terms do not parse");
    assert_eq!(refusal.code(), nemo::codes::NOT_TERMS);
    let refusal = engine
        .tree_for_table(tree("edge", vec![EntryQuery::Terms("a".into())]))
        .expect_err("one term for two columns");
    assert_eq!(refusal.code(), nemo::codes::SELECTOR_ARITY);
    let refusal = engine
        .tree_for_table(tree("edge", vec![EntryQuery::Entry(99)]))
        .expect_err("no such entry");
    assert_eq!(refusal.code(), nemo::codes::UNKNOWN_ENTRY);
}

#[test]
fn rules_are_found_by_their_statement_id() {
    let mut engine = chain();
    let rule = engine.rule(4).expect("statement 4 is a rule");
    assert_eq!(rule.text, "reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .");
    let source = rule.source.expect("the rule has a source");
    assert_eq!((source.file.as_str(), source.line), ("test.rls", 4));
    assert!(engine.rule(0).is_none(), "statement 0 is a fact");
    assert!(engine.rule(99).is_none());
    // The query names the same rule.
    let response = engine
        .tree_for_table(tree("reach", vec![EntryQuery::Terms("a, c".into())]))
        .expect("the query answers");
    assert_eq!(
        response
            .child_information
            .expect("an expansion")
            .rule
            .string_representation,
        rule.text
    );
}

#[test]
fn scratch_tables_do_not_accumulate() {
    let mut engine = chain();
    let before = engine.memory_bytes();
    let query = || tree("reach", vec![EntryQuery::Terms("a, c".into())]);
    for _ in 0..5 {
        engine.tree_for_table(query()).expect("the query answers");
    }
    // Scratch tables are dropped per query; repeated queries may
    // build arrangements once, but must not grow per call.
    let after_one = engine.memory_bytes();
    for _ in 0..5 {
        engine.tree_for_table(query()).expect("the query answers");
    }
    assert_eq!(
        engine.memory_bytes(),
        after_one,
        "repeated queries hold no extra memory (started at {before})"
    );
}
