//! What a response says about one node: its facts, and the time that deriving them took.

use serde::{Deserialize, Serialize};

/// The facts of a node in the requested slice (nev: `TableResponseBaseTableEntries`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableEntries {
    /// The facts.
    pub entries: Vec<TableEntry>,
    /// The position of the slice, and whether more facts follow it.
    pub pagination: PageInfo,
}

/// One fact of a node (nev: `TableEntryResponse`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableEntry {
    /// The fact's entry id.
    ///
    /// It is absent for a fact matched at an import's endpoint, such as a SPARQL endpoint,
    /// since such facts are not stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<usize>,
    /// The fact's terms, in canonical term syntax.
    ///
    /// Where no value was computed, the written term appears instead.
    pub term_tuple: Vec<String>,
}

/// The slice of a node's facts that a response contains (nev:
/// `TableResponseBaseTableEntriesPagination`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageInfo {
    /// The position of the slice's first fact among the node's facts.
    pub start: usize,
    /// Whether facts beyond the slice exist.
    pub more_entries_exist: bool,
}

/// The time that deriving a node's facts took (nev:
/// `TableResponseBaseMetaInformation`).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetaInformation {
    /// The wall time, in milliseconds, of the rule applications that stored facts of the node's
    /// predicate, or 0 for a predicate that no rule derived.
    pub execution_time: u64,
}
