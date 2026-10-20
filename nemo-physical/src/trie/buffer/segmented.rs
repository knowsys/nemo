//! The segmented row buffer: rows appended segment by segment, each segment a run.

use std::mem;

use crate::{
    parallel::{self, Tasks},
    trie::{RowBuffer, Run, SortedRowBuffer},
};

/// A [RowBuffer] whose rows form segments, each a run:
/// the rows appended between one [begin_segment](Self::begin_segment) and the next.
///
/// A segment's rows arrive in storage order,
/// or in order by a prefix of their columns,
/// which [sort_last_segment](Self::sort_last_segment) completes.
/// Rows may repeat within a segment and across segments.
#[derive(Debug)]
pub(crate) struct SegmentedRowBuffer {
    /// The segments' rows, one segment after the other.
    buffer: RowBuffer,
    /// The index of each segment's first row, ascending.
    starts: Vec<usize>,
}

impl SegmentedRowBuffer {
    /// Creates a buffer without segments for rows of the given arity.
    pub(crate) fn new(arity: usize) -> Self {
        Self {
            buffer: RowBuffer::new(arity),
            starts: Vec::new(),
        }
    }

    /// Creates a buffer whose one segment holds the given rows.
    pub(crate) fn from_sorted(rows: SortedRowBuffer) -> Self {
        Self {
            buffer: rows.into_rows(),
            starts: vec![0],
        }
    }

    /// Returns the number of values per row.
    pub(crate) fn arity(&self) -> usize {
        self.buffer.arity()
    }

    /// Returns the number of rows across the segments.
    pub(crate) fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the rows of every segment as a plain buffer.
    pub(crate) fn rows(&self) -> &RowBuffer {
        &self.buffer
    }

    /// Returns the number of segments.
    pub(crate) fn segment_count(&self) -> usize {
        self.starts.len()
    }

    /// Begins a segment at the end and returns the buffer its rows are appended to,
    /// in storage order unless [sort_last_segment](Self::sort_last_segment) follows.
    pub(crate) fn begin_segment(&mut self) -> &mut RowBuffer {
        self.starts.push(self.buffer.len());
        &mut self.buffer
    }

    /// Sorts the segment begun last,
    /// whose rows stand in order by their leading `prefix` columns,
    /// into one run: the rows within each group of equal prefix are sorted.
    ///
    /// The groups sort independently, on the pool if they are many and large enough.
    /// A prefix of zero makes the segment one group, which is a plain sort of it.
    pub(crate) fn sort_last_segment(&mut self, prefix: usize) {
        let start = *self.starts.last().expect("a segment was begun");
        let arity = self.buffer.arity();
        let rows = self.buffer.len();
        if arity == 0 || rows.saturating_sub(start) <= 1 {
            return;
        }

        let mut ends = Vec::new();
        for row in start + 1..rows {
            let (before, now) = (self.buffer.row(row - 1), self.buffer.row(row));
            if before[..prefix] != now[..prefix] {
                ends.push((row - start) * arity);
            }
        }
        ends.push((rows - start) * arity);

        let segment = &mut self.buffer.values_mut()[start * arity..];
        let tasks = Tasks::cut(segment.len(), ends.len());

        parallel::for_each_piece(segment, &ends, tasks, |group| {
            RowBuffer::sort_rows(group, arity)
        });
    }

    /// Returns the segment of the given index as a run.
    pub(crate) fn segment(&self, index: usize) -> Run<'_> {
        let arity = self.buffer.arity();
        let start = self.starts[index];
        let end = self
            .starts
            .get(index + 1)
            .copied()
            .unwrap_or(self.buffer.len());
        let values = &self.buffer.values()[start * arity..end * arity];
        debug_assert!(
            arity == 0 || values.chunks_exact(arity).is_sorted(),
            "a segment is a run"
        );

        Run::new(values, arity, end - start)
    }

    /// Sorts and deduplicates all rows, folding the segments into one.
    pub(crate) fn sort_all(&mut self) {
        let arity = self.buffer.arity();
        let mut sorted = mem::replace(&mut self.buffer, RowBuffer::new(arity)).into_sorted();
        sorted.dedup();
        self.buffer = sorted.into_rows();
        self.starts.clear();
        self.starts.push(0);
    }

    /// Returns the allocated bytes of the buffer.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.buffer.memory_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::{Rng, SeedableRng, rngs::StdRng};

    use crate::{storage::StorageValue, testing::int};

    /// The rows of a run, for comparison.
    fn rows_of(run: Run<'_>) -> Vec<Vec<StorageValue>> {
        run.iter().map(<[StorageValue]>::to_vec).collect()
    }

    /// Appends integer rows to a buffer.
    fn push_rows(buffer: &mut RowBuffer, rows: &[Vec<i64>]) {
        for row in rows {
            let row: Vec<StorageValue> = row.iter().map(|&value| int(value)).collect();
            buffer.push_row(&row);
        }
    }

    #[test]
    fn sorting_the_last_segment_matches_a_full_sort_of_it() {
        // Rows in order by their first column, unordered within each
        // group, with repeats, behind a segment that must stay put.
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..40 {
            let arity = rng.gen_range(2..=4);
            let head: Vec<Vec<i64>> = (0..rng.gen_range(0..4))
                .map(|_| (0..arity).map(|_| rng.gen_range(0..3)).collect())
                .collect();
            let mut tail: Vec<Vec<i64>> = Vec::new();
            let mut group = 0;
            for _ in 0..rng.gen_range(0..60) {
                if rng.gen_bool(0.3) {
                    group += 1;
                }
                let mut row = vec![group];
                row.extend((1..arity).map(|_| rng.gen_range(0..5)));
                tail.push(row);
            }

            let mut actual = SegmentedRowBuffer::new(arity);
            push_rows(actual.begin_segment(), &head);
            push_rows(actual.begin_segment(), &tail);
            actual.sort_last_segment(1);

            let mut expected = RowBuffer::new(arity);
            push_rows(&mut expected, &head);
            let mut sorted_tail = RowBuffer::new(arity);
            push_rows(&mut sorted_tail, &tail);
            expected.append(sorted_tail.into_sorted().rows());

            assert_eq!(actual.rows().values(), expected.values());
            assert_eq!(actual.segment(1).len(), tail.len());
        }
    }

    #[test]
    fn large_groups_sort_like_the_whole_segment() {
        let mut rng = StdRng::seed_from_u64(11);
        let rows: Vec<Vec<i64>> = (0..200)
            .map(|_| vec![0, rng.gen_range(0..1000), rng.gen_range(0..2)])
            .collect();
        let mut actual = SegmentedRowBuffer::new(3);
        push_rows(actual.begin_segment(), &rows);
        actual.sort_last_segment(1);

        let mut expected = RowBuffer::new(3);
        push_rows(&mut expected, &rows);
        assert_eq!(
            actual.rows().values(),
            expected.into_sorted().rows().values()
        );
    }

    #[test]
    fn segments_are_runs_cut_at_their_starts() {
        let mut buffer = SegmentedRowBuffer::new(1);
        let first = buffer.begin_segment();
        first.push_row(&[int(1)]);
        first.push_row(&[int(3)]);
        buffer.begin_segment().push_row(&[int(2)]);
        buffer.begin_segment();

        assert_eq!(buffer.segment_count(), 3);
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.segment(0).len(), 2);
        assert_eq!(buffer.segment(1).row(0), &[int(2)]);
        assert!(buffer.segment(2).is_empty());
        assert_eq!(buffer.rows().len(), 3);
    }

    #[test]
    fn the_last_segment_sorts_within_its_groups() {
        let mut buffer = SegmentedRowBuffer::new(2);
        buffer.begin_segment().push_row(&[int(9), int(9)]);
        let tail = buffer.begin_segment();
        for row in [[1, 2], [1, 1], [1, 2], [2, 0]] {
            tail.push_row(&row.map(int));
        }
        buffer.sort_last_segment(1);

        assert_eq!(rows_of(buffer.segment(0)), vec![vec![int(9), int(9)]]);
        assert_eq!(
            rows_of(buffer.segment(1)),
            vec![
                vec![int(1), int(1)],
                vec![int(1), int(2)],
                vec![int(1), int(2)],
                vec![int(2), int(0)]
            ]
        );
    }

    #[test]
    fn sorting_all_folds_the_segments_into_one() {
        let mut buffer = SegmentedRowBuffer::new(1);
        buffer.begin_segment().push_row(&[int(2)]);
        let second = buffer.begin_segment();
        second.push_row(&[int(1)]);
        second.push_row(&[int(2)]);
        buffer.sort_all();

        assert_eq!(buffer.segment_count(), 1);
        assert_eq!(rows_of(buffer.segment(0)), vec![vec![int(1)], vec![int(2)]]);
    }
}
