//! This module defines [TableEntriesForTreeNodesQuery] and [TableEntriesForTreeNodesResponse].

use serde::{Deserialize, Serialize};

use super::shared::{
    PaginationQuery, PaginationResponse, RuleId, TableEntryQuery, TableEntryResponse,
};

/// Defines the next layer in a [TableEntriesForTreeNodesQueryInner]
#[derive(Debug, Serialize, Deserialize)]
pub struct TableEntriesForTreeNodesQuerySuccessor {
    /// Rule deriving this facts
    pub rule: RuleId,
    /// Child nodes used in this rule application
    pub children: Vec<TableEntriesForTreeNodesQueryInner>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableEntriesForTreeNodesQueryInner {
    /// List of queries
    pub queries: Vec<TableEntryQuery>,

    /// [PaginationQuery] for restricting the amount of expected results
    pub pagination: PaginationQuery,

    /// Children of this node
    pub children: TableEntriesForTreeNodesQuerySuccessor,
}

/// Request for a trace which follows the given tree structure
///
/// The expected result has the form of [TableEntriesForTreeNodesResponse].
#[derive(Debug, Serialize, Deserialize)]
pub struct TableEntriesForTreeNodesQuery {
    /// Predicate of the root node
    pub predicate: String,

    /// Inner query
    pub inner: TableEntriesForTreeNodesQueryInner,
}

/// Represents an address in a tree structure using a vector of indices.
///
/// In this representation, each element in the vector corresponds to an index
/// at a specific depth level in the tree. The first element represents the
/// index at the root level, the second element represents the index at the
/// first child level, and so on.
pub type TreeAddress = Vec<usize>;

#[derive(Debug, Serialize, Deserialize)]
pub struct TableEntriesForTreeNodesResponseElement {
    /// Predicate of the this table
    pub predicate: String,

    /// Entries contained in this table
    pub entries: Vec<TableEntryResponse>,
    /// [PaginationResponse] specifying whether the response is incomplete
    pub pagination: PaginationResponse,

    /// All rules that may use facts in this table
    pub possible_rules_above: Vec<RuleId>,
    /// All rules that derived facts in this table
    pub possible_rules_below: Vec<RuleId>,

    /// Identifying the query node for which
    pub address: TreeAddress,
}

/// Response for a [TableEntriesForTreeNodesQuery]
///
/// For each query node contains the set of facts
/// that are consistent with a trace in the query tree.
#[derive(Debug, Serialize, Deserialize)]
pub struct TableEntriesForTreeNodesResponse {
    /// Responses for each node
    pub elements: Vec<TableEntriesForTreeNodesResponseElement>,
}
