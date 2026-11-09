//! The sink that projects rows to some of their columns.

use crate::{
    execution::{
        ProjectionShape,
        sink::{RowSet, RowSink},
    },
    storage::StorageValue,
    trie::RowBuffer,
};

/// The scratch memory of a projection, kept across the joins drained through it.
#[derive(Debug)]
pub(crate) struct ProjectionScratch {
    /// The projected rows of the current group, without their group prefix.
    seen: RowSet,
    /// The buffer for the projected row.
    row: Vec<StorageValue>,
}

impl ProjectionScratch {
    /// Creates the scratch memory of a projection.
    pub(crate) fn new(shape: &ProjectionShape) -> Self {
        Self {
            seen: RowSet::new(shape.columns.len() - shape.group_prefix),
            row: Vec::new(),
        }
    }
}

/// A sink that projects each row to some of its columns, as its [shape](ProjectionShape) says,
/// and keeps the projected rows that are new in the current group.
///
/// The projection starts with the row's leading columns, unchanged,
/// which are its [group prefix](ProjectionShape::group_prefix).
/// While the group prefix stays the same,
/// a projected row can repeat only through the layers projected away,
/// and the sink drops those repeats.
/// Repeats across drains are dropped when the drained runs are merged into a trie.
#[derive(Debug)]
pub(crate) struct ProjectingSink<'a> {
    /// Per projected column, the column of the drained row it reads.
    columns: &'a [usize],
    /// The number of leading projected columns that copy the row's leading columns.
    group_prefix: usize,
    /// The projected rows of the current group, without their group prefix.
    seen: &'a mut RowSet,
    /// The buffer for the projected row.
    row: &'a mut Vec<StorageValue>,
    /// The buffer receiving the projected rows not seen before.
    out: &'a mut RowBuffer,
}

impl<'a> ProjectingSink<'a> {
    /// Creates a sink that projects into `out` as the shape says, and starts a new group.
    ///
    /// `scratch` must have been created for the same shape.
    pub(crate) fn new(
        shape: &'a ProjectionShape,
        scratch: &'a mut ProjectionScratch,
        out: &'a mut RowBuffer,
    ) -> Self {
        debug_assert_eq!(
            scratch.seen.width(),
            shape.columns.len() - shape.group_prefix
        );
        scratch.seen.begin_group();

        Self {
            columns: &shape.columns,
            group_prefix: shape.group_prefix,
            seen: &mut scratch.seen,
            row: &mut scratch.row,
            out,
        }
    }
}

impl RowSink for ProjectingSink<'_> {
    #[inline]
    fn accept(&mut self, row: &[StorageValue], changed: usize) {
        if changed < self.group_prefix {
            self.seen.begin_group();
        }

        self.row.clear();
        self.row
            .extend(self.columns.iter().map(|&column| row[column]));

        if self.seen.insert(&self.row[self.group_prefix..]) {
            self.out.push_row(self.row);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::int;

    /// A projecting sink drops repeats within a group,
    /// and starts a new group when a layer of the group prefix changes.
    #[test]
    fn projecting_sinks_group_on_the_changed_layer() {
        // Project (a, c) from rows (a, b, c), with a as the group prefix.
        let shape = ProjectionShape::new(vec![0, 2]);
        let mut scratch = ProjectionScratch::new(&shape);
        let mut out = RowBuffer::new(2);
        let mut sink = ProjectingSink::new(&shape, &mut scratch, &mut out);

        // (1, 7) arrives twice, with different b, and is kept once.
        sink.accept(&[int(1), int(2), int(7)], 0);
        sink.accept(&[int(1), int(3), int(7)], 1);
        sink.accept(&[int(1), int(3), int(9)], 2);

        // A new group may repeat the suffix.
        sink.accept(&[int(2), int(1), int(7)], 0);
        assert_eq!(out.len(), 3);
        assert_eq!(out.row(0), &[int(1), int(7)]);
        assert_eq!(out.row(1), &[int(1), int(9)]);
        assert_eq!(out.row(2), &[int(2), int(7)]);
    }
}
