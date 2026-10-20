//! The `treeForTable` query, and its response.

use serde::{Deserialize, Serialize};

use crate::trace::query::{EntriesQuery, MetaInformation, QueryRule, TableEntries};

/// The `treeForTable` query: facts of one predicate whose derivation
/// the server expands (nev: `TreeForTableQuery`).
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
    /// The rules that read the node's predicate, one per head atom.
    pub possible_rules_above: Vec<QueryRule>,
    /// The rules whose head atoms write the node's predicate.
    pub possible_rules_below: Vec<QueryRule>,
    /// The rule application expanding the node, absent if the tree stops here.
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
    /// The children: the positive premises, then the negated ones.
    pub children: Vec<TreeForTableResponse>,
}
