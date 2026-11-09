//! Which facts of a node a query selects.

use serde::{Deserialize, Serialize};

/// Which facts of a node a query selects, and which slice of them the response contains
/// (nev: `TableQueryBaseTableEntries`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntriesQuery {
    /// The facts that the node is restricted to, or no restriction if empty.
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
    /// A stored fact, by its entry id.
    ///
    /// This is the only way to select a fact that contains a null.
    Entry(usize),
    /// A ground fact, by its terms in term syntax, separated by commas, such as `a, "b", 3`.
    Terms(String),
}

/// A slice of a node's facts (nev:
/// `TableQueryBaseTableEntriesPagination`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pagination {
    /// The position of the slice's first fact among the node's facts.
    pub start: usize,
    /// The largest number of facts in the slice.
    pub count: usize,
}
