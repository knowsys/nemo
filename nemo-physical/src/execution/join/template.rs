//! Join templates: what the joins of one shape over different inputs share.

use crate::{
    dictionary::Dictionary,
    execution::{JoinArena, JoinShape, TrieJoin},
    function::EvaluationContext,
    trie::Trie,
};

/// What the joins of one shape share across the inputs they join:
/// everything but the joined blocks.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JoinTemplate<'a> {
    /// The shape of the joins, taken to be valid; see [TrieJoin::with_trusted_shape].
    pub(crate) shape: JoinShape<'a>,
    /// The subtracted tries, one per subtracted input of the shape.
    pub(crate) subtracted: &'a [&'a Trie],
    /// The dictionary the joins' programs read.
    pub(crate) dictionary: &'a Dictionary,
    /// The context the joins' programs evaluate in.
    pub(crate) evaluation: EvaluationContext,
}

impl<'a> JoinTemplate<'a> {
    /// Builds the join over the blocks of the given inputs in an arena.
    pub(crate) fn build(&self, inputs: &[&[&'a Trie]], arena: &'a JoinArena) -> TrieJoin<'a> {
        TrieJoin::with_trusted_shape(
            self.shape,
            inputs,
            self.subtracted,
            Some(self.dictionary),
            self.evaluation,
            arena,
        )
    }
}
