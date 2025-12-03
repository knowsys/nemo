//! This module defines [TableEntriesForTreeNodesQuery] and [TableEntriesForTreeNodesResponse].

use std::hash::Hash;

use serde::{Deserialize, Serialize};

use crate::execution::tracing::shared::ResponseMetaInformation;

use super::shared::{
    PaginationQuery, PaginationResponse, Rule, RuleId, TableEntryQuery, TableEntryResponse,
};

/// Defines the next layer in a [TableEntriesForTreeNodesQueryInner]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableEntriesForTreeNodesQuerySuccessor {
    /// Rule deriving this facts
    pub rule: RuleId,
    /// The head index in the rule deriving the facts
    pub head_index: usize,
    /// Child nodes used in this rule application
    pub children: Vec<TableEntriesForTreeNodesQueryInner>,
}

/// Inner query in [TableEntriesForTreeNodesQuery]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableEntriesForTreeNodesQueryInner {
    /// List of queries
    pub queries: Vec<TableEntryQuery>,

    /// [PaginationQuery] for restricting the amount of expected results
    pub pagination: Option<PaginationQuery>,

    /// Children of this node
    pub next: Option<TableEntriesForTreeNodesQuerySuccessor>,
}

impl TableEntriesForTreeNodesQueryInner {
    /// Return the number of nodes contained in this query
    pub fn num_nodes(&self) -> usize {
        if let Some(successor) = &self.next {
            1 + successor
                .children
                .iter()
                .map(|child| child.num_nodes())
                .sum::<usize>()
        } else {
            1
        }
    }

    /// Return the maximal depth of this tree
    pub fn max_depth(&self) -> usize {
        if let Some(successor) = &self.next {
            1 + successor
                .children
                .iter()
                .map(|child| child.num_nodes())
                .max()
                .unwrap_or_default()
        } else {
            1
        }
    }

    /// Return whether there is no restriction expressed in this node.
    pub fn is_simple(&self) -> bool {
        self.queries.is_empty() && self.next.is_none()
    }
}

/// Request for a trace which follows the given tree structure
///
/// The expected result has the form of [TableEntriesForTreeNodesResponse].
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableEntriesForTreeNodesQuery {
    /// Predicate of the root node
    pub predicate: String,

    /// Inner query
    pub inner: TableEntriesForTreeNodesQueryInner,
}

impl TableEntriesForTreeNodesQuery {
    /// Return the number of nodes contained in this query
    pub fn num_nodes(&self) -> usize {
        self.inner.num_nodes()
    }

    /// Return the maximal depth of this query
    pub fn max_depth(&self) -> usize {
        self.inner.max_depth()
    }
}

/// Represents an address in a tree structure using a vector of indices.
///
/// In this representation, each element in the vector corresponds to an index
/// at a specific depth level in the tree. The first element represents the
/// index at the root level, the second element represents the index at the
/// first child level, and so on.
pub type TreeAddress = Vec<usize>;

/// Single entry in [TableEntriesForTreeNodesResponse]
#[derive(Debug, Serialize, Deserialize)]
pub struct TableEntriesForTreeNodesResponseElement {
    /// Predicate of the this table
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

    /// Identifying the query node for which
    pub address: TreeAddress,
}

/// Response for a [TableEntriesForTreeNodesQuery]
///
/// For each query node contains the set of facts
/// that are consistent with a trace in the query tree.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TableEntriesForTreeNodesResponse {
    /// Responses for each node
    pub elements: Vec<TableEntriesForTreeNodesResponseElement>,
}
