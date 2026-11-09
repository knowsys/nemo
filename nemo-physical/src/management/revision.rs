//! Revisions: the logical times at which rows are added to a table.

/// A logical time at which rows are added to a table.
///
/// Each batch of rows is added at a later revision than the batch before,
/// and revisions may be skipped.
/// A read, such as [tries](crate::management::TableManager::tries),
/// asks for the rows added within a range of revisions,
/// for example all rows added since a revision that was read before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(pub u64);
