//! The derivation-structure queries of the explanation viewer of nemo-web (nev):
//! a tree of nodes over the written program, each holding facts of one predicate,
//! connected by rule applications.
//!
//! The format is nev's own, defined by its maintainers
//! (their cue/OpenAPI specification, v0.1.0),
//! and these types follow it field for field:
//! camelCase on the wire, and terms in canonical term syntax.
//! Each type names the model of that specification it corresponds to.
//!
//! Two queries share these types:
//!
//! - [TreeForTableQuery] selects facts of one predicate and lets the server expand their
//!   derivation.
//!   The server keeps expanding a node as long as one written rule derives all of its facts.
//!   The children then hold the facts those derivations use.
//! - [TableEntriesForTreeNodesQuery] gives the tree's shape:
//!   at every expanded node, a rule and the head atom that derives the node's facts,
//!   and a selection on any node.
//!   The response lists, per node, the facts consistent with every constraint of the tree.
//!
//! An expanded node has one child per positive body literal of its rule, in body order,
//! followed by one child per negated literal, which lists the facts the negated predicate holds.
//! Rules are named by *statement id*, their position in the written program,
//! as [WrittenRule::rule](crate::trace::WrittenRule::rule) names them.
//! A node's facts are numbered by *entry id*, their position in the predicate's stored order,
//! the same numbering the engine's row cursors use.

mod node;
mod rule;
mod selection;
mod table_entries_for_tree_nodes;
mod tree_for_table;

pub use self::{
    node::{MetaInformation, PageInfo, TableEntries, TableEntry},
    rule::{PredicateWithParameters, QueryRule},
    selection::{EntriesQuery, EntryQuery, Pagination},
    table_entries_for_tree_nodes::{
        NodeResponse, QueryChildren, QueryNode, TableEntriesForTreeNodesQuery,
    },
    tree_for_table::{TreeChildren, TreeForTableQuery, TreeForTableResponse},
};

#[cfg(test)]
mod tests {
    use super::*;

    /// The JSON follows nev's format: camelCase names, entries as numbers or
    /// strings, and optional parts omitted.
    #[test]
    fn queries_and_responses_follow_the_viewer_format() {
        let query: TableEntriesForTreeNodesQuery = serde_json::from_str(
            r#"{"predicate": "reach", "tableEntries": {"queries": [3, "a, b"],
                "pagination": {"start": 0, "count": 10}},
                "childInformation": {"rule": 4, "headIndex": 0, "children": [{}]}}"#,
        )
        .expect("the query parses");
        let entries = query.table_entries.expect("a selection");
        assert_eq!(
            entries.queries,
            [EntryQuery::Entry(3), EntryQuery::Terms("a, b".into())]
        );
        let children = query.child_information.expect("an expansion");
        assert_eq!((children.rule, children.head_index), (4, 0));
        assert_eq!(children.children.len(), 1);

        let response = NodeResponse {
            address_in_tree: vec![0],
            predicate: "edge".into(),
            table_entries: TableEntries {
                entries: vec![TableEntry {
                    entry_id: Some(1),
                    term_tuple: vec!["<a>".into()],
                }],
                pagination: PageInfo {
                    start: 0,
                    more_entries_exist: false,
                },
            },
            meta_information: MetaInformation::default(),
            possible_rules_above: Vec::new(),
            possible_rules_below: Vec::new(),
        };
        let json = serde_json::to_string(&response).expect("the response serializes");
        assert_eq!(
            json,
            r#"{"addressInTree":[0],"predicate":"edge","tableEntries":{"entries":[{"entryId":1,"termTuple":["<a>"]}],"pagination":{"start":0,"moreEntriesExist":false}},"metaInformation":{"executionTime":0},"possibleRulesAbove":[],"possibleRulesBelow":[]}"#
        );
    }
}
