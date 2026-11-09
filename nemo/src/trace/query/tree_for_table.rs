//! The `treeForTable` query, and its response.

use serde::{Deserialize, Serialize};

use crate::trace::query::{EntriesQuery, MetaInformation, QueryRule, TableEntries};

/// The `treeForTable` query, which selects facts of one predicate whose derivation
/// the server expands (nev: `TreeForTableQuery`).
///
/// The server keeps expanding a node as long as one written rule derives all of its facts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TreeForTableQuery {
    /// The root node's predicate, by resolved name.
    pub predicate: String,
    /// The root's selection, or every fact of the predicate if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_entries: Option<EntriesQuery>,
}

/// The response to a [TreeForTableQuery], and each node of it (nev:
/// `TreeForTableResponse`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TreeForTableResponse {
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
    /// The expansion of the node, or none if the tree ends here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_information: Option<TreeChildren>,
}

/// The expansion of a [TreeForTableResponse] node (nev:
/// `TreeForTableResponseChildInformation`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TreeChildren {
    /// The rule, with the head atom that derives the node's facts.
    pub rule: QueryRule,
    /// The children, one per positive body literal, then one per negated literal.
    pub children: Vec<TreeForTableResponse>,
}
