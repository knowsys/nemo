//! The response to a trace request: the derivation graph of the requested facts.

use serde::{Deserialize, Serialize};

use crate::trace::{Proof, TraceNode, WrittenRule};

/// The response to a [trace](crate::engine::Engine::trace) request:
/// the requested facts, and the derivations of those that hold, as one graph.
///
/// Each node is a fact, and its id is its position in [nodes](Self::nodes).
/// A derived node names its premises by id, and every premise comes before the nodes it
/// supports,
/// so a fact several derivations use is a single node.
/// Rules are listed once, and a derivation names its rule by its position in
/// [rules](Self::rules).
/// [proof](Self::proof) walks a requested fact's derivation like a tree.
///
/// # Example
///
/// ```
/// use nemo::{
///     engine::Engine,
///     fact,
///     model::{FileId, Program},
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
/// let proof = response.proof(0).expect("reach(1, 3) holds");
/// assert_eq!(proof.fact(), "reach(1, 3)");
/// let premises: Vec<&str> = proof.premises().map(|premise| premise.fact()).collect();
/// assert_eq!(premises, ["reach(1, 2)", "edge(2, 3)"]);
/// # Ok::<(), nemo::diagnostics::Diagnostic>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceResponse {
    /// The protocol version.
    pub version: u32,
    /// The requested facts, in request order.
    pub facts: Vec<TracedFact>,
    /// The facts of the derivations, each after the premises it was derived from.
    pub nodes: Vec<TraceNode>,
    /// The rules the derivations apply, each once.
    pub rules: Vec<WrittenRule>,
}

/// One requested fact and its outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TracedFact {
    /// The fact as requested, in term syntax.
    pub fact: String,
    /// The fact's node, or `None` if the fact does not hold.
    pub node: Option<usize>,
}

impl TraceResponse {
    /// Returns the proof of the requested fact at the given position,
    /// or `None` if the fact does not hold.
    ///
    /// # Panics
    ///
    /// Panics if no fact was requested at that position.
    pub fn proof(&self, fact: usize) -> Option<Proof<'_>> {
        self.facts[fact].node.map(|id| self.node_proof(id))
    }

    /// Returns the proof of the node with the given id, to walk its derivation.
    ///
    /// # Panics
    ///
    /// Panics if the response holds no node with that id.
    pub fn node_proof(&self, id: usize) -> Proof<'_> {
        assert!(id < self.nodes.len(), "the response holds no node {id}");
        Proof::new(self, id)
    }
}
