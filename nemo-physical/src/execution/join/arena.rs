//! The scratch memory joins build their working state in.

use bumpalo::Bump;

/// The scratch memory joins build their working state in.
///
/// The working state of a join is a few dozen small collections,
/// the cursors, links, hook tables and evaluation buffers,
/// which live exactly as long as the join.
/// The arena allocates them by bumping a pointer,
/// and [reset](Self::reset) releases them all at once.
///
/// Allocating them one by one would cost more than a small join itself,
/// and many joins are small.
/// So the caller owns the arena and reuses it for any number of joins in sequence.
/// A parallel run gives each worker an arena of its own.
#[derive(Debug, Default)]
pub struct JoinArena {
    /// The bump allocator behind the scratch.
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
