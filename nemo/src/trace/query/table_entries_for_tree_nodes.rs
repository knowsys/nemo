//! The `tableEntriesForTreeNodes` query, and the nodes of its response.

use serde::{Deserialize, Serialize};

use crate::trace::query::{EntriesQuery, MetaInformation, QueryRule, TableEntries};

/// The `tableEntriesForTreeNodes` query, which gives the shape of a tree (nev:
/// `TableEntriesForTreeNodesQuery`).
///
/// The response lists the facts of each node that fit every constraint of the tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableEntriesForTreeNodesQuery {
    /// The root node's predicate, by resolved name.
    pub predicate: String,
    /// The root's selection, or every fact of the predicate if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_entries: Option<EntriesQuery>,
    /// The expansion of the root, or none if the tree is the root alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_information: Option<QueryChildren>,
}

/// One node below the root of a [TableEntriesForTreeNodesQuery] (nev:
/// `InnerTableQuery`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryNode {
    /// The node's selection, or no restriction if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_entries: Option<EntriesQuery>,
    /// The expansion of the node, or none if the tree ends here.
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
    /// The index of the head atom that derives the node's facts, among the rule's head atoms.
    pub head_index: usize,
    /// The children, one per positive body literal, then one per negated literal.
    ///
    /// Missing trailing children are unconstrained.
    /// The child of a negated literal lists facts of the negated predicate.
    /// Its selection and slice restrict what it lists,
    /// but it constrains no other node and cannot be expanded.
    #[serde(default)]
    pub children: Vec<QueryNode>,
}

/// One node of the response to a [TableEntriesForTreeNodesQuery],
/// which lists the tree's nodes with parents before children (nev:
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
    /// The rules that read the node's predicate, once with each of their head atoms.
    pub possible_rules_above: Vec<QueryRule>,
    /// The rules that write the node's predicate, once with each head atom that writes it.
    pub possible_rules_below: Vec<QueryRule>,
}
