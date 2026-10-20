//! The concatenation: the column pieces built for consecutive shards of one trie,
//! joined into the trie's columns.

use crate::{
    parallel::{self, Tasks},
    trie::{ColumnView, Intervals, column::Column, construct::builder::ColumnKind},
};

impl Column {
    /// Concatenates the columns built for consecutive shards of one trie,
    /// each shard a column vector of the arity, into the trie's columns,
    /// one column per task; see [concat](Self::concat).
    pub(crate) fn concat_shards(arity: usize, shards: Vec<Vec<Column>>) -> Vec<Column> {
        let work: usize = shards.iter().flatten().map(Column::len).sum();
        let mut per_column: Vec<Vec<Column>> = (0..arity)
            .map(|_| Vec::with_capacity(shards.len()))
            .collect();
        for columns in shards {
            for (column, piece) in columns.into_iter().enumerate() {
                per_column[column].push(piece);
            }
        }

        parallel::map_tasks(Tasks::fixed(work, arity), |column| {
            Column::concat(&per_column[column], column == 0)
        })
    }

    /// Concatenates the pieces of one column built for consecutive shards of its rows:
    /// the values in order, each piece's blocks shifted behind the values before it.
    ///
    /// With `continued`, the pieces' first blocks are one block cut across them,
    /// as in the first column, whose single block spans every row.
    /// The pieces may differ in layout; the result takes the most compact layout that holds them all.
    fn concat(pieces: &[Column], continued: bool) -> Column {
        let total: usize = pieces.iter().map(Column::len).sum();
        let blocks: usize = pieces.iter().map(|piece| piece.intervals().blocks()).sum();
        let mut intervals = Intervals::with_capacity(total, blocks);
        let mut offset = 0;
        for piece in pieces {
            let starts = piece.intervals();
            for block in 0..starts.blocks() {
                if continued && block == 0 && offset > 0 {
                    continue;
                }
                intervals.push(offset + starts.get(block));
            }
            offset += piece.len();
        }
        intervals.push(total);

        match ColumnKind::merged(pieces.iter().map(Column::view)) {
            ColumnKind::U32(tag) => {
                let mut values = Vec::with_capacity(total);
                for piece in pieces {
                    match piece.view() {
                        ColumnView::U32 { values: part, .. } => values.extend_from_slice(part),
                        other => debug_assert!(other.is_empty(), "the merged layout holds all"),
                    }
                }
                Column::U32 {
                    values,
                    tag,
                    intervals,
                }
            }
            ColumnKind::U64(tag) => {
                let mut values = Vec::with_capacity(total);
                for piece in pieces {
                    match piece.view() {
                        ColumnView::U32 { values: part, .. } => {
                            values.extend(part.iter().map(|&value| u64::from(value)));
                        }
                        ColumnView::U64 { values: part, .. } => values.extend_from_slice(part),
                        mixed => debug_assert!(mixed.is_empty(), "the merged layout holds all"),
                    }
                }
                Column::U64 {
                    values,
                    tag,
                    intervals,
                }
            }
            ColumnKind::Mixed => {
                let mut values = Vec::with_capacity(total);
                let mut tags = Vec::with_capacity(total);
                for piece in pieces {
                    match piece.view() {
                        ColumnView::U32 { values: part, tag } => {
                            values.extend(part.iter().map(|&value| u64::from(value)));
                            tags.resize(values.len(), tag.tag());
                        }
                        ColumnView::U64 { values: part, tag } => {
                            values.extend_from_slice(part);
                            tags.resize(values.len(), tag);
                        }
                        ColumnView::Mixed {
                            values: part,
                            tags: part_tags,
                        } => {
                            values.extend_from_slice(part);
                            tags.extend_from_slice(part_tags);
                        }
                    }
                }
                Column::Mixed {
                    values,
                    tags,
                    intervals,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        storage::{StorageValue, Tag},
        trie::construct::builder::ColumnBuilder,
    };

    #[test]
    fn concatenated_pieces_widen_to_the_layout_holding_them_all() {
        let column32 = ColumnBuilder::single_block(&[StorageValue::term(1), StorageValue::term(2)]);
        let column64 = ColumnBuilder::single_block(&[StorageValue::term(u64::from(u32::MAX) + 1)]);
        let integers = ColumnBuilder::single_block(&[StorageValue::integer(7)]);
        let empty = ColumnBuilder::new(ColumnKind::Mixed, 0, 0).finish();

        let widened = Column::concat(&[column32, empty, column64], true);
        assert!(matches!(widened, Column::U64 { tag: Tag::Term, .. }));
        assert_eq!(widened.len(), 3);
        assert_eq!(widened.intervals().blocks(), 1);
        assert_eq!(
            widened.view().get(2),
            StorageValue::term(u64::from(u32::MAX) + 1)
        );

        let column32 = ColumnBuilder::single_block(&[StorageValue::term(1), StorageValue::term(2)]);
        let mixed = Column::concat(&[column32, integers], false);
        assert!(matches!(mixed, Column::Mixed { .. }));
        assert_eq!(mixed.intervals().blocks(), 2);
        assert_eq!(mixed.view().get(0), StorageValue::term(1));
        assert_eq!(mixed.view().get(2), StorageValue::integer(7));
    }
}
