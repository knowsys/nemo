//! A part of a trace response that arrives in parts.

use serde::{Deserialize, Serialize};

use crate::trace::{TraceNode, WrittenRule};

/// A part of a [TraceResponse](crate::trace::TraceResponse) that is handed out in parts,
/// with the nodes and rules that follow those of the earlier parts.
///
/// Ids continue from part to part.
/// A part's first node has the id after the last node of the earlier parts,
/// and so does its first rule.
/// A node names premises and a rule of its own part or of an earlier one.
///
/// [trace_predicates](crate::engine::Engine::trace_predicates) shows an example.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TracePart {
    /// The nodes, each after the premises it was derived from.
    pub nodes: Vec<TraceNode>,
    /// The rules that the nodes apply and that no earlier part listed.
    pub rules: Vec<WrittenRule>,
}
