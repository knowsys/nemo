//! Which facts of a node a query selects.

use serde::{Deserialize, Serialize};

/// Which facts of a node a query selects, and which slice of them the
/// response carries (nev: `TableQueryBaseTableEntries`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntriesQuery {
    /// The facts the node is restricted to, unconstrained if empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queries: Vec<EntryQuery>,
    /// The slice of the node's facts to return, or all of them if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<Pagination>,
}

/// One fact a query selects (nev: `TableEntryQuery`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EntryQuery {
    /// A stored fact by its entry id, the only way to select a fact holding a null.
    Entry(usize),
    /// A ground fact by its terms in term syntax, separated by commas:
    /// `a, "b", 3`.
    Terms(String),
}

/// A slice of a node's facts (nev:
/// `TableQueryBaseTableEntriesPagination`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pagination {
    /// The position of the first fact, in the node's fact set.
    pub start: usize,
    /// How many facts at most.
    pub count: usize,
}
