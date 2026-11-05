//! The blocks a member's join reads.

use crate::{execution::SubtractedShape, trie::Trie};

/// The blocks a member's join reads.
#[derive(Debug)]
pub(super) struct JoinBlocks<'a> {
    /// Per joined input, its blocks.
    pub(super) inputs: Vec<Vec<&'a Trie>>,
    /// The subtracted blocks, each subtracted on its own.
    pub(super) subtracted: Vec<&'a Trie>,
    /// Per subtracted block, its shape.
    pub(super) subtracted_shapes: Vec<SubtractedShape>,
}

impl<'a> JoinBlocks<'a> {
    /// Returns the number of blocks, joined and subtracted.
    pub(super) fn count(&self) -> usize {
        self.inputs.iter().map(Vec::len).sum::<usize>() + self.subtracted.len()
    }

    /// Returns the blocks of each joined input, as a join takes them.
    pub(super) fn input_slices(&self) -> Vec<&[&'a Trie]> {
        self.inputs.iter().map(Vec::as_slice).collect()
    }
}
