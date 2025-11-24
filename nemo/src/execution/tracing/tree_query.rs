//! This module defines [TreeForTableQuery] and [TreeForTableResponse].

use serde::{Deserialize, Serialize};

use crate::execution::tracing::shared::ResponseMetaInformation;

use super::shared::{
    PaginationQuery, PaginationResponse, Rule, TableEntryQuery, TableEntryResponse,
};

/// Request for a trace of a set of facts
///
/// The expected result has the form of [TreeForTableResponse].
#[derive(Debug, Serialize, Deserialize)]
pub struct TreeForTableQuery {
    /// Predicate of the root table
    pub predicate: String,
    /// List of queries
    pub queries: Vec<TableEntryQuery>,

    /// [PaginationQuery] for restricting the amount of expected results
    pub pagination: Option<PaginationQuery>,
}

/// Defines the next layer in a [TreeForTableResponse]
#[derive(Debug, Serialize, Deserialize)]
pub struct TreeForTableResponseSuccessor {
    /// Rule deriving this facts
    pub rule: Rule,
    /// Child nodes used in this rule application
    pub children: Vec<TreeForTableResponse>,
}

/// Response for a [TreeForTableQuery]
///
/// Starting from a root node containing a set of facts,
/// this response encodes a tree with fact sets as nodes
/// and rules as edges, providing an explanation for the root facts.
///
/// If facts from one node are derived from different rules,
/// then this not is not expanded further.
#[derive(Debug, Serialize, Deserialize)]
pub struct TreeForTableResponse {
    /// Predicate of this table
    pub predicate: String,

    /// Meta information about the rule execution
    pub meta_information: ResponseMetaInformation,

    /// Entries contained in this table
    pub entries: Vec<TableEntryResponse>,
    /// [PaginationResponse] specifying whether the response is incomplete
    pub pagination: PaginationResponse,

    /// All rules that may use facts in this table
    pub possible_rules_above: Vec<Rule>,
    /// All rules that derived facts in this table
    pub possible_rules_below: Vec<Rule>,

    /// Children of this tree node
    pub next: Option<TreeForTableResponseSuccessor>,
}
