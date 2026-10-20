//! Runs: the rows of a materializing node, collected as sorted runs before its tries are built.

use crate::trie::{RowBuffer, Run, SegmentedRowBuffer, SortedRowBuffer};

/// The rows of a materializing node before its tries are built: sorted runs.
///
/// The sequential joins write their rows into one buffer, segment by segment.
/// The buffers of cells stay apart instead of being copied.
/// A row may repeat across runs; the build drops the repeats.
#[derive(Debug)]
pub(super) struct Runs {
    /// The rows of the sequential joins, segment by segment.
    buffer: SegmentedRowBuffer,
    /// The buffers of the cells.
    cells: Vec<SortedRowBuffer>,
    /// The runs; their order does not matter to the build, which merges them.
    runs: Vec<RunSource>,
}

/// Where one run of a node's rows lies.
#[derive(Debug, Clone, Copy)]
enum RunSource {
    /// The buffer's segment of the given index.
    Segment(usize),
    /// The buffer of the cell of the given index.
    Cell(usize),
}

impl Runs {
    /// Creates runs without rows, of the given arity.
    pub(super) fn new(arity: usize) -> Self {
        Self {
            buffer: SegmentedRowBuffer::new(arity),
            cells: Vec::new(),
            runs: Vec::new(),
        }
    }

    /// Creates the single run of a sorted buffer.
    pub(super) fn of(buffer: SortedRowBuffer) -> Self {
        Self {
            buffer: SegmentedRowBuffer::from_sorted(buffer),
            cells: Vec::new(),
            runs: vec![RunSource::Segment(0)],
        }
    }

    /// Returns the number of values per row.
    pub(super) fn arity(&self) -> usize {
        self.buffer.arity()
    }

    /// Returns the number of rows across the runs, repeats included.
    pub(super) fn len(&self) -> usize {
        self.buffer.len() + self.cells.iter().map(SortedRowBuffer::len).sum::<usize>()
    }

    /// Starts a segment at the end of the buffer, and returns the buffer to push its rows into.
    ///
    /// The rows pushed until the next segment starts must form one sorted run.
    pub(super) fn begin_segment(&mut self) -> &mut RowBuffer {
        self.runs
            .push(RunSource::Segment(self.buffer.segment_count()));
        self.buffer.begin_segment()
    }

    /// Sorts the rows of the last segment within the groups of rows
    /// that agree on the first `prefix` columns.
    ///
    /// A segment whose rows arrived in order by that prefix becomes a sorted run.
    pub(super) fn sort_last_segment(&mut self, prefix: usize) {
        self.buffer.sort_last_segment(prefix);
    }

    /// Adds the sorted buffer of a cell as a run of its own.
    pub(super) fn push_cell(&mut self, cell: SortedRowBuffer) {
        self.runs.push(RunSource::Cell(self.cells.len()));
        self.cells.push(cell);
    }

    /// Sorts and deduplicates the buffer into one run, for segments that are not runs.
    ///
    /// The buffers of the cells stay the runs they are.
    pub(super) fn sort_buffer(&mut self) {
        self.buffer.sort_all();
        self.runs.retain(|run| matches!(run, RunSource::Cell(_)));
        self.runs.insert(0, RunSource::Segment(0));
    }

    /// Returns the run at the given source.
    fn run(&self, source: RunSource) -> Run<'_> {
        match source {
            RunSource::Segment(segment) => self.buffer.segment(segment),
            RunSource::Cell(cell) => self.cells[cell].run(),
        }
    }

    /// Passes the runs to the given function.
    ///
    /// A single run, the common case, needs no allocation.
    pub(super) fn with_runs<T>(&self, read: impl FnOnce(&[Run<'_>]) -> T) -> T {
        match self.runs.as_slice() {
            [] => read(&[]),
            [single] => read(&[self.run(*single)]),
            sources => {
                let runs: Vec<Run<'_>> = sources.iter().map(|&source| self.run(source)).collect();
                read(&runs)
            }
        }
    }

    /// Returns the rows in another column order, sorted in that order;
    /// see [reordered_sorted](RowBuffer::reordered_sorted).
    ///
    /// Repeats across runs stay, for the build to drop.
    pub(super) fn reordered_sorted(&self, order: &[usize]) -> SortedRowBuffer {
        if self.cells.is_empty() {
            return self.buffer.rows().reordered_sorted(order);
        }

        let mut all = RowBuffer::new(self.arity());
        all.append(self.buffer.rows());
        for cell in &self.cells {
            all.append(cell.rows());
        }
        all.reordered_sorted(order)
    }

    /// Returns the allocated bytes of the largest buffer among the runs.
    pub(super) fn largest_buffer_bytes(&self) -> usize {
        self.cells
            .iter()
            .map(SortedRowBuffer::memory_bytes)
            .fold(self.buffer.memory_bytes(), usize::max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::StorageValue;

    #[test]
    fn runs_keep_segments_and_cells_apart() {
        let mut runs = Runs::new(1);
        let segment = runs.begin_segment();
        segment.push_row(&[StorageValue::integer(1)]);
        segment.push_row(&[StorageValue::integer(2)]);
        let mut cell = RowBuffer::new(1);
        cell.push_row(&[StorageValue::integer(3)]);
        runs.push_cell(cell.into_sorted());
        runs.begin_segment().push_row(&[StorageValue::integer(2)]);
        assert_eq!(runs.len(), 4);

        runs.with_runs(|sorted| {
            assert_eq!(sorted.len(), 3);
            assert_eq!(sorted[0].len(), 2);
            assert_eq!(sorted[1].row(0), &[StorageValue::integer(3)]);
            assert_eq!(sorted[2].row(0), &[StorageValue::integer(2)]);
        });

        // Sorting the buffer folds its segments into one run
        // and leaves the cell a run of its own.
        runs.sort_buffer();
        runs.with_runs(|sorted| {
            assert_eq!(sorted.len(), 2);
            assert_eq!(sorted[0].len(), 2);
        });
        assert_eq!(runs.reordered_sorted(&[0]).len(), 3);
    }
}
