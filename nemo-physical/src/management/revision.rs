//! Revisions: the logical times at which rows are added to a table.

/// A logical time at which rows are added to a table.
///
/// The caller adds each batch of rows at a revision of its choosing,
/// every batch at a later revision than the one before, and may skip revisions.
/// A read asks for the rows added within a range of revisions,
/// such as everything added since a revision it read before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(pub u64);
