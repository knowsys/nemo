//! The chain: pieces of runs that follow one another, so that together they are one sorted sequence.

use crate::{
    storage::StorageValue,
    trie::{Run, construct::merge::RowSource},
};

/// A chain of run pieces, each following the one before,
/// so that its rows read piece after piece are in storage order.
///
/// A chain is also the [source](RowSource) of its rows for a merge.
#[derive(Debug)]
pub(super) struct RunChain<'r> {
    /// The pieces, in order.
    pieces: Vec<Run<'r>>,
    /// The piece the source is at.
    piece: usize,
    /// The row within the piece the source is at.
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
    /// Pieces that already follow one another, as the rows of one sorted stream
    /// cut into runs do, form one chain as they are.
    /// Otherwise first fit over the pieces in first-row order finds the fewest chains,
    /// so that a merge ranges over chains rather than pieces.
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

    /// Returns the rows in order, whatever the source's position.
    pub(super) fn rows(&self) -> impl Iterator<Item = &'r [StorageValue]> + '_ {
        self.pieces.iter().flat_map(Run::iter)
    }
}

/// Returns whether one piece's rows all come at or before another's.
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

    /// A sorted buffer of the given unary rows.
    fn buffer(values: &[i64]) -> SortedRowBuffer {
        let mut buffer = RowBuffer::new(1);
        for &value in values {
            buffer.push_row(&[StorageValue::integer(value)]);
        }
        buffer.into_sorted()
    }

    #[test]
    fn pieces_group_into_the_fewest_chains() {
        // The pieces of two combinations, interleaved as emitted: each
        // combination's pieces chain, the combinations overlap.
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
