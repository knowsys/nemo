//! What a response says about one node: its facts, and the time deriving them took.

use serde::{Deserialize, Serialize};

/// A node's facts, as far as the slice goes (nev:
/// `TableResponseBaseTableEntries`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableEntries {
    /// The facts.
    pub entries: Vec<TableEntry>,
    /// Which slice the facts are.
    pub pagination: PageInfo,
}

/// One fact of a node (nev: `TableEntryResponse`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableEntry {
    /// The fact's entry id.
    ///
    /// It is absent on facts matched at the source an import reads, such as a SPARQL endpoint,
    /// which are not stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<usize>,
    /// The fact's terms, in canonical term syntax.
    ///
    /// If the run never had a value, the written term appears instead.
    pub term_tuple: Vec<String>,
}

/// Which slice of a node's facts a response carries (nev:
/// `TableResponseBaseTableEntriesPagination`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageInfo {
    /// The position of the first fact carried, in the node's fact set.
    pub start: usize,
    /// Whether facts beyond the slice exist.
    pub more_entries_exist: bool,
}

/// What a node's response says about the run (nev:
/// `TableResponseBaseMetaInformation`).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetaInformation {
    /// The wall time, in milliseconds, of the rule applications that stored facts of the node's
    /// predicate, or 0 for a predicate no rule derived.
    pub execution_time: u64,
}
