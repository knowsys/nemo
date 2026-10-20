//! Join templates: what the joins of one shape over different tries share.

use crate::{
    dictionary::Dictionary,
    execution::{JoinArena, JoinShape, TrieJoin},
    function::EvaluationContext,
    trie::Trie,
};

/// What the joins of one shape share across the tries they join:
/// everything but the joined tries.
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
    /// Builds the join over the given tries in an arena.
    pub(crate) fn build(&self, tries: &[&'a Trie], arena: &'a JoinArena) -> TrieJoin<'a> {
        TrieJoin::with_trusted_shape(
            self.shape,
            tries,
            self.subtracted,
            Some(self.dictionary),
            self.evaluation,
            arena,
        )
    }
}
