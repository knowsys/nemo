//! Walking a derivation of a trace response like a tree.

use crate::trace::{Derivation, TraceNode, TraceResponse, WrittenRule};

/// A node of a [TraceResponse], for walking its derivation like a tree.
///
/// The premises of a proof are the proofs of the nodes that its derivation names,
/// so a fact that several derivations use is reached along each of them.
#[derive(Debug, Clone, Copy)]
pub struct Proof<'a> {
    /// The response the node belongs to.
    response: &'a TraceResponse,
    /// The node's id.
    id: usize,
}

impl<'a> Proof<'a> {
    /// Creates the proof of the node with id `id`, which the response must have.
    pub(super) fn new(response: &'a TraceResponse, id: usize) -> Self {
        Self { response, id }
    }

    /// Returns the node's id, its position in the response's nodes.
    pub fn id(&self) -> usize {
        self.id
    }

    /// Returns the node.
    pub fn node(&self) -> &'a TraceNode {
        &self.response.nodes[self.id]
    }

    /// Returns the fact, in term syntax.
    pub fn fact(&self) -> &'a str {
        &self.node().fact
    }

    /// Returns how a rule derived the fact,
    /// or `None` for a fact that was loaded or that an import's endpoint returned.
    pub fn derivation(&self) -> Option<&'a Derivation> {
        self.node().derivation.as_ref()
    }

    /// Returns the rule that derived the fact, or `None` if no rule did.
    pub fn rule(&self) -> Option<&'a WrittenRule> {
        self.derivation()
            .map(|derivation| &self.response.rules[derivation.rule])
    }

    /// Returns the premises that the fact was derived from, one per positive body literal.
    pub fn premises(&self) -> impl Iterator<Item = Proof<'a>> + 'a {
        let response = self.response;
        self.derivation()
            .into_iter()
            .flat_map(|derivation| &derivation.premises)
            .map(move |&id| Proof::new(response, id))
    }
}
