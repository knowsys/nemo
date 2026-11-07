//! A part of a trace response that arrives in parts.

use serde::{Deserialize, Serialize};

use crate::trace::{TraceNode, WrittenRule};

/// A part of a [TraceResponse](crate::trace::TraceResponse) handed out in parts,
/// as [trace_predicates](crate::engine::Engine::trace_predicates) does:
/// the nodes and rules that follow those of the parts before.
///
/// Ids continue from part to part: a part's first node has the id after the last node of the
/// parts before, and so do its rules.
/// A node names premises and a rule of its own part or of an earlier one.
///
/// [trace_predicates](crate::engine::Engine::trace_predicates) shows an example.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TracePart {
    /// The nodes, each after the premises it was derived from.
    pub nodes: Vec<TraceNode>,
    /// The rules the nodes apply that no earlier part listed.
    pub rules: Vec<WrittenRule>,
}
