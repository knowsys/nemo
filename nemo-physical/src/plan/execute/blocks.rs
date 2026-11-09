//! The blocks a member's join reads.

use bumpalo::{Bump, collections::Vec as BumpVec};

use crate::{
    execution::SubtractedShape,
    plan::{execute::Execution, layout::MemberLayout},
    trie::Trie,
};

/// The blocks a member's join reads, gathered in the join arena.
#[derive(Debug, Clone, Copy)]
pub(super) struct JoinBlocks<'a> {
    /// Per joined input, its blocks.
    pub(super) inputs: &'a [&'a [&'a Trie]],
    /// The subtracted blocks, each subtracted on its own.
    pub(super) subtracted: &'a [&'a Trie],
    /// Per subtracted block, its shape.
    pub(super) subtracted_shapes: &'a [SubtractedShape<'a>],
    /// The blocks of the subtracted relations that share no layer with the join.
    pub(super) uncompared: &'a [&'a Trie],
}

impl JoinBlocks<'_> {
    /// Returns the number of blocks, joined and subtracted.
    pub(super) fn count(&self) -> usize {
        self.inputs.iter().map(|blocks| blocks.len()).sum::<usize>()
            + self.subtracted.len()
            + self.uncompared.len()
    }
}

impl Execution<'_> {
    /// Returns the blocks a member's join reads, gathered in the given arena:
    /// those of its inputs and of its subtracted relations.
    pub(super) fn member_blocks<'j>(
        &'j self,
        member: &'j MemberLayout,
        bump: &'j Bump,
    ) -> JoinBlocks<'j> {
        let inputs = BumpVec::from_iter_in(
            member.inputs.iter().map(|&required| {
                let mut blocks = BumpVec::new_in(bump);
                self.blocks(required, &mut blocks);
                blocks.into_bump_slice()
            }),
            bump,
        );

        // The blocks of every relation first, then their shapes in one allocation,
        // so that neither list moves when the other grows.
        let mut ends = BumpVec::with_capacity_in(member.subtracted.len(), bump);
        let mut subtracted = BumpVec::new_in(bump);
        for relation in &member.subtracted {
            self.blocks(relation.order, &mut subtracted);
            ends.push(subtracted.len());
        }
        let mut subtracted_shapes = BumpVec::with_capacity_in(subtracted.len(), bump);
        let mut first = 0;
        for (relation, &end) in member.subtracted.iter().zip(&ends) {
            let shape = SubtractedShape {
                layers: &relation.layers,
            };
            subtracted_shapes.extend(std::iter::repeat_n(shape, end - first));
            first = end;
        }

        let mut uncompared = BumpVec::new_in(bump);
        for &required in &member.uncompared {
            self.blocks(required, &mut uncompared);
        }

        JoinBlocks {
            inputs: inputs.into_bump_slice(),
            subtracted: subtracted.into_bump_slice(),
            subtracted_shapes: subtracted_shapes.into_bump_slice(),
            uncompared: uncompared.into_bump_slice(),
        }
    }
}
