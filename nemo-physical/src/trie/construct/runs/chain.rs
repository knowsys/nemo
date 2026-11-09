//! The chain, a sequence of run pieces that together are one sorted sequence.

use crate::{
    storage::StorageValue,
    trie::{Run, construct::merge::RowSource},
};

/// A chain of run pieces, each of which follows the one before,
/// so that the rows, read piece after piece, are in storage order.
///
/// A chain is also the [source](RowSource) of its rows for a merge.
#[derive(Debug)]
pub(super) struct RunChain<'r> {
    /// The pieces, in order.
    pieces: Vec<Run<'r>>,
    /// The index of the piece that the source is at.
    piece: usize,
    /// The index of the row within the piece that the source is at.
    row: usize,
}

impl<'r> RunChain<'r> {
    /// Creates a chain of pieces that follow one another.
    fn new(pieces: Vec<Run<'r>>) -> Self {
        debug_assert!(
            pieces.windows(2).all(|pair| follows(pair[0], pair[1])),
            "the pieces of a chain follow one another"
        );
        Self {
            pieces,
            piece: 0,
            row: 0,
        }
    }

    /// Groups nonempty pieces into as few chains as their overlaps allow.
    ///
    /// Pieces that already follow one another, as the runs of one sorted stream do,
    /// form one chain as they are.
    /// Otherwise, a first-fit assignment of the pieces in the order of their first rows
    /// finds the fewest chains, so that a merge has chains rather than pieces as its sources.
    pub(super) fn partition(pieces: &[Run<'r>]) -> Vec<Self> {
        if pieces.windows(2).all(|pair| follows(pair[0], pair[1])) {
            return vec![Self::new(pieces.to_vec())];
        }

        let mut sorted = pieces.to_vec();
        sorted.sort_by(|left, right| left.row(0).cmp(right.row(0)));

        let mut chains: Vec<Vec<Run<'r>>> = Vec::new();
        for piece in sorted {
            match chains
                .iter_mut()
                .find(|chain| follows(chain[chain.len() - 1], piece))
            {
                Some(chain) => chain.push(piece),
                None => chains.push(vec![piece]),
            }
        }
        chains.into_iter().map(Self::new).collect()
    }

    /// Returns the number of pieces.
    #[cfg(test)]
    pub(super) fn pieces(&self) -> usize {
        self.pieces.len()
    }

    /// Returns the number of rows, repeats included.
    pub(super) fn len(&self) -> usize {
        self.pieces.iter().map(Run::len).sum()
    }

    /// Returns all rows in order, whatever position the source is at.
    pub(super) fn rows(&self) -> impl Iterator<Item = &'r [StorageValue]> + '_ {
        self.pieces.iter().flat_map(Run::iter)
    }
}

/// Returns whether every row of `before` comes at or before every row of `after`.
fn follows(before: Run<'_>, after: Run<'_>) -> bool {
    before.row(before.len() - 1) <= after.row(0)
}

impl RowSource for RunChain<'_> {
    fn row(&self) -> &[StorageValue] {
        self.pieces[self.piece].row(self.row)
    }

    fn has_row(&self) -> bool {
        self.piece < self.pieces.len()
    }

    fn advance(&mut self) {
        self.row += 1;
        if self.row == self.pieces[self.piece].len() {
            self.piece += 1;
            self.row = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::trie::{RowBuffer, SortedRowBuffer};

    /// Returns a sorted buffer of unary rows with the given integers.
    fn buffer(values: &[i64]) -> SortedRowBuffer {
        let mut buffer = RowBuffer::new(1);
        for &value in values {
            buffer.push_row(&[StorageValue::integer(value)]);
        }
        buffer.into_sorted()
    }

    /// Interleaved pieces of two overlapping sorted streams are grouped into two chains.
    #[test]
    fn pieces_group_into_the_fewest_chains() {
        // The pieces of two sorted streams, interleaved as they were produced.
        // The pieces of each stream follow one another, and the two streams overlap.
        let buffers = [
            buffer(&[1, 2]),
            buffer(&[3, 4]),
            buffer(&[1, 3]),
            buffer(&[5, 6]),
            buffer(&[4, 9]),
        ];
        let pieces: Vec<Run<'_>> = buffers.iter().map(SortedRowBuffer::run).collect();
        let chains = RunChain::partition(&pieces);
        assert_eq!(chains.len(), 2);
        assert_eq!(chains[0].pieces(), 3);
        assert_eq!(chains[1].pieces(), 2);
        assert_eq!(chains[0].row()[0], StorageValue::integer(1));
        assert_eq!(chains[1].row()[0], StorageValue::integer(1));
        assert_eq!(chains[0].len(), 6);
    }

    /// A chain returns the rows of its pieces in order,
    /// both as a source and through [rows](RunChain::rows).
    #[test]
    fn a_chain_reads_its_pieces_in_order() {
        let buffers = [buffer(&[1, 2]), buffer(&[2, 3])];
        let pieces: Vec<Run<'_>> = buffers.iter().map(SortedRowBuffer::run).collect();
        let mut chains = RunChain::partition(&pieces);
        assert_eq!(chains.len(), 1);

        let chain = &mut chains[0];
        let mut read = Vec::new();
        while chain.has_row() {
            read.push(chain.row()[0]);
            chain.advance();
        }
        assert_eq!(read, [1, 2, 2, 3].map(StorageValue::integer));
        assert!(chain.rows().eq(pieces.iter().flat_map(Run::iter)));
    }
}
