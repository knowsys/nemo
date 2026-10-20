//! The response to a trace request: one traced fact per requested fact.

use serde::{Deserialize, Serialize};

use crate::trace::{FactText, TraceNode};

/// The response to a [trace](crate::engine::Engine::trace) request:
/// one entry per requested fact, in request order.
///
/// # Example
///
/// ```
/// use nemo::{
///     engine::Engine,
///     fact,
///     model::{FileId, Program},
///     trace::TraceNode,
/// };
///
/// let (program, _) = Program::parse(
///     "edge(1, 2) . edge(2, 3) .
///      reach(?x, ?y) :- edge(?x, ?y) .
///      reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .",
///     FileId::new(0),
/// );
/// let mut engine = Engine::new();
/// engine.load(program)?;
/// engine.reason()?;
///
/// let response = engine.trace(&[fact!(reach(1, 3))])?;
/// let traced = &response.facts[0];
/// assert!(traced.holds);
/// let Some(TraceNode::Derived { premises, .. }) = &traced.tree else {
///     panic!("reach(1, 3) is derived by a rule");
/// };
/// assert_eq!(premises.len(), 2);
/// # Ok::<(), nemo::diagnostics::Diagnostic>(())
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceResponse {
    /// The protocol version.
    pub version: u32,
    /// The traced facts.
    pub facts: Vec<TracedFact>,
}

/// One requested fact and its outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TracedFact {
    /// The fact as requested (values in canonical rendering).
    pub fact: FactText,
    /// Whether the fact holds after reasoning, as a loaded or a derived fact.
    pub holds: bool,
    /// The proof tree, for a fact that holds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tree: Option<TraceNode>,
}
