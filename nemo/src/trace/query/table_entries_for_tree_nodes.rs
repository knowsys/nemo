//! The `tableEntriesForTreeNodes` query, and the nodes of its response.

use serde::{Deserialize, Serialize};

use crate::trace::query::{EntriesQuery, MetaInformation, QueryRule, TableEntries};

/// The `tableEntriesForTreeNodes` query: a tree the client shaped (nev:
/// `TableEntriesForTreeNodesQuery`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableEntriesForTreeNodesQuery {
    /// The root node's predicate, by resolved name.
    pub predicate: String,
    /// The root's selection, or every fact of the predicate if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_entries: Option<EntriesQuery>,
    /// The rule application expanding the root, or the root alone if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_information: Option<QueryChildren>,
}

/// One node below the root of a [TableEntriesForTreeNodesQuery] (nev:
/// `InnerTableQuery`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryNode {
    /// The node's selection, unconstrained if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_entries: Option<EntriesQuery>,
    /// The rule application expanding the node, absent if the tree stops here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_information: Option<QueryChildren>,
}

/// The expansion of a query node (nev:
/// `InnerTableQueryChildInformation`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryChildren {
    /// The rule, by statement id.
    pub rule: usize,
    /// The head atom deriving the node's facts, by index over the
    /// rule's head.
    pub head_index: usize,
    /// The children: one per positive body literal, then one per negated literal.
    ///
    /// Missing trailing children are unconstrained.
    /// A negated literal's child lists facts of the negated predicate:
    /// its selection and page restrict what it lists,
    /// but it constrains no other node and cannot be expanded.
    #[serde(default)]
    pub children: Vec<QueryNode>,
}

/// One node of the response to a [TableEntriesForTreeNodesQuery], which
/// lists the tree's nodes with parents before children (nev:
/// `TableEntriesForTreeNodesResponseInner`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeResponse {
    /// The path of child indices from the root, which has `[]`.
    pub address_in_tree: Vec<usize>,
    /// The node's predicate, by resolved name.
    pub predicate: String,
    /// The node's facts.
    pub table_entries: TableEntries,
    /// How long deriving the node's predicate took.
    pub meta_information: MetaInformation,
    /// The rules that read the node's predicate, one per head atom.
    pub possible_rules_above: Vec<QueryRule>,
    /// The rules whose head atoms write the node's predicate.
    pub possible_rules_below: Vec<QueryRule>,
}
