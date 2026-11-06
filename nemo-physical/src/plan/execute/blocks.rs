//! The blocks a member's join reads.

use crate::{execution::SubtractedShape, trie::Trie};

/// The blocks a member's join reads, gathered in the join arena.
#[derive(Debug, Clone, Copy)]
pub(super) struct JoinBlocks<'a> {
    /// Per joined input, its blocks.
    pub(super) inputs: &'a [&'a [&'a Trie]],
    /// The subtracted blocks, each subtracted on its own.
    pub(super) subtracted: &'a [&'a Trie],
    /// Per subtracted block, its shape.
    pub(super) subtracted_shapes: &'a [SubtractedShape<'a>],
}

impl JoinBlocks<'_> {
    /// Returns the number of blocks, joined and subtracted.
    pub(super) fn count(&self) -> usize {
        self.inputs.iter().map(|blocks| blocks.len()).sum::<usize>() + self.subtracted.len()
    }
}
