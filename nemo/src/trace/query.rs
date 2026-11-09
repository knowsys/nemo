//! The derivation-structure queries of nev, the explanation viewer of nemo-web.
//!
//! A query describes a tree of *nodes* over the written program.
//! Each node has facts of one predicate,
//! and a node is *expanded* by a rule that derives its facts.
//! An expanded node has one child per positive body literal of its rule, in body order,
//! followed by one child per negated literal, which lists facts of the negated predicate.
//! Rules are named by *statement id*, their position in the written program,
//! as [WrittenRule::rule](crate::trace::WrittenRule::rule) names them.
//! A node's facts are numbered by *entry id*, their position in the predicate's stored order,
//! which is the numbering of the engine's [cursors](crate::engine::Engine::cursor).
//!
//! Two queries share these types:
//! - A [TreeForTableQuery] selects facts of one predicate, and the server expands the tree.
//! - A [TableEntriesForTreeNodesQuery] gives the shape of the tree,
//!   and the response lists the facts of each node that fit every constraint of the tree.
//!
//! The format is nev's own, defined by its maintainers in their cue/OpenAPI specification,
//! v0.1.0, and these types follow it field for field.
//! The JSON field names are camelCase, and terms are in canonical term syntax.
//! Each type names the model of the specification that it corresponds to.

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

    /// Queries and responses follow nev's JSON format, with camelCase names,
    /// entries as numbers or strings, and optional parts left out.
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
