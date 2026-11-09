//! The scratch memory that joins build their working state in.

use bumpalo::Bump;

/// The scratch memory that joins build their working state in.
///
/// The working state of a join is a few dozen small collections,
/// such as the cursors, links, hook tables and evaluation buffers,
/// which live exactly as long as the join.
/// The arena allocates them by bumping a pointer,
/// and [reset](Self::reset) releases them all at once.
///
/// Allocating them one by one would cost more than a small join itself,
/// and many joins are small.
/// So one arena serves any number of joins in sequence,
/// and their working state accumulates in it until it is reset.
/// Joins that run at the same time need an arena each.
#[derive(Debug, Default)]
pub struct JoinArena {
    /// The bump allocator.
    bump: Bump,
}

impl JoinArena {
    /// Creates an empty arena, which allocates on first use.
    pub fn new() -> Self {
        Self::default()
    }

    /// Releases everything allocated so far, keeping the memory for the next joins.
    ///
    /// The arena is borrowed mutably, so no join built in it survives the reset.
    pub fn reset(&mut self) {
        self.bump.reset();
    }

    /// Returns the allocator the joins build their collections in.
    pub(crate) fn bump(&self) -> &Bump {
        &self.bump
    }
}
