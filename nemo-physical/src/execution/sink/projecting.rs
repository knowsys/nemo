//! The sink projecting rows to some of their columns.

use crate::{
    execution::{
        ProjectionShape,
        sink::{RowSet, RowSink},
    },
    storage::StorageValue,
    trie::RowBuffer,
};

/// The scratch of a projection, kept across the joins drained through it:
/// the projected rows of the current group, and the projected row.
#[derive(Debug)]
pub(crate) struct ProjectionScratch {
    /// The projected rows of the current group, without their group prefix.
    seen: RowSet,
    /// The projected row.
    row: Vec<StorageValue>,
}

impl ProjectionScratch {
    /// Creates the scratch of a projection.
    pub(crate) fn new(shape: &ProjectionShape) -> Self {
        Self {
            seen: RowSet::new(shape.columns.len() - shape.group_prefix),
            row: Vec::new(),
        }
    }
}

/// A sink that projects each row to some of its columns, as its [shape](ProjectionShape) says,
/// and keeps the projected rows it has not seen in the current group.
///
/// The projection starts with the row's leading columns, unchanged:
/// the group prefix.
/// While the prefix stands, a projected row repeats
/// only through the layers projected away, and the set drops those repeats.
/// The repeats across drains are dropped when the drained runs are merged into a trie.
#[derive(Debug)]
pub(crate) struct ProjectingSink<'a> {
    /// Per projected column, the column of the drained row it reads.
    columns: &'a [usize],
    /// The number of leading projected columns that copy the row's leading columns.
    group_prefix: usize,
    /// The projected rows of the current group, without their group prefix.
    seen: &'a mut RowSet,
    /// The scratch for the projected row.
    row: &'a mut Vec<StorageValue>,
    /// The buffer receiving the projected rows not seen before.
    out: &'a mut RowBuffer,
}

impl<'a> ProjectingSink<'a> {
    /// Creates a sink projecting as the shape says into `out`,
    /// and starts a new group in the scratch, which must be the shape's.
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

    #[test]
    fn projecting_sinks_group_on_the_changed_layer() {
        // Project (a, c) from rows (a, b, c); a is the group prefix.
        let shape = ProjectionShape::new(vec![0, 2]);
        let mut scratch = ProjectionScratch::new(&shape);
        let mut out = RowBuffer::new(2);
        let mut sink = ProjectingSink::new(&shape, &mut scratch, &mut out);

        // (1, *, 7) twice through different b's: one survivor.
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
