//! The column view: a column's values, borrowed in their physical layout.

use std::ops::Range;

use crate::storage::{StorageValue, Tag, Tag32};

/// A borrowed view of a trie column's values, dispatched over the physical layout;
/// the block boundaries come separately from [Trie::intervals](crate::trie::Trie::intervals).
///
/// An executor matches the layout once and runs its loops on the raw payload slices;
/// [get](Self::get) is the per-value accessor for everything that is not a hot loop.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ColumnView<'a> {
    /// A uniformly tagged column whose payloads fit 32 bits.
    U32 {
        /// The order-preserving 32-bit payloads.
        values: &'a [u32],
        /// The tag shared by every value.
        tag: Tag32,
    },
    /// A uniformly tagged column with 64-bit payloads.
    U64 {
        /// The order-preserving 64-bit payloads.
        values: &'a [u64],
        /// The tag shared by every value.
        tag: Tag,
    },
    /// A column holding values of more than one tag.
    Mixed {
        /// The order-preserving 64-bit payloads.
        values: &'a [u64],
        /// The tags, parallel to `values`.
        tags: &'a [Tag],
    },
}

impl ColumnView<'_> {
    /// Returns the number of values.
    pub(crate) fn len(&self) -> usize {
        match self {
            ColumnView::U32 { values, .. } => values.len(),
            ColumnView::U64 { values, .. } | ColumnView::Mixed { values, .. } => values.len(),
        }
    }

    /// Returns whether the column has no values.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the tag of a uniformly tagged column,
    /// or `None` for a mixed column, which an empty column is too.
    pub(crate) fn uniform_tag(&self) -> Option<Tag> {
        match self {
            ColumnView::U32 { tag, .. } => Some(tag.tag()),
            ColumnView::U64 { tag, .. } => Some(*tag),
            ColumnView::Mixed { .. } => None,
        }
    }

    /// Returns the value at the given index.
    pub(crate) fn get(&self, index: usize) -> StorageValue {
        match self {
            ColumnView::U32 { values, tag } => StorageValue {
                tag: tag.tag(),
                payload: u64::from(values[index]),
            },
            ColumnView::U64 { values, tag } => StorageValue {
                tag: *tag,
                payload: values[index],
            },
            ColumnView::Mixed { values, tags } => StorageValue {
                tag: tags[index],
                payload: values[index],
            },
        }
    }

    /// Returns the first index whose value is at or above the given one in storage order,
    /// or the number of values when there is none.
    pub(crate) fn lower_bound(&self, value: StorageValue) -> usize {
        self.lower_bound_in(0..self.len(), value)
    }

    /// Returns the first index in the range whose value is at or above the given one,
    /// or the range's end when there is none.
    ///
    /// This is [lower_bound](Self::lower_bound) within one block.
    pub(crate) fn lower_bound_in(&self, range: Range<usize>, value: StorageValue) -> usize {
        match self {
            ColumnView::U32 { values, tag } => {
                let tag = tag.tag();

                if value.tag < tag {
                    range.start
                } else if value.tag > tag {
                    range.end
                } else {
                    match u32::try_from(value.payload) {
                        Ok(payload) => {
                            range.start + values[range].partition_point(|&stored| stored < payload)
                        }
                        Err(_) => range.end,
                    }
                }
            }
            ColumnView::U64 { values, tag } => {
                if value.tag < *tag {
                    range.start
                } else if value.tag > *tag {
                    range.end
                } else {
                    range.start + values[range].partition_point(|&stored| stored < value.payload)
                }
            }
            ColumnView::Mixed { values, tags } => {
                let mut low = range.start;
                let mut high = range.end;

                while low < high {
                    let middle = low + (high - low) / 2;
                    let stored = StorageValue {
                        tag: tags[middle],
                        payload: values[middle],
                    };
                    if stored < value {
                        low = middle + 1;
                    } else {
                        high = middle;
                    }
                }

                low
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::trie::{RowBuffer, Trie};

    /// A unary trie of the given values, whose only column is a single block.
    fn unary(values: &[StorageValue]) -> Trie {
        let mut buffer = RowBuffer::new(1);
        for &value in values {
            buffer.push_row(&[value]);
        }
        Trie::from_buffer(buffer)
    }

    #[test]
    fn lower_bounds_respect_tags_in_every_layout() {
        let terms = unary(&[StorageValue::term(2), StorageValue::term(5)]);
        let integers = unary(&[StorageValue::integer(2), StorageValue::integer(5)]);
        let both = unary(&[StorageValue::term(2), StorageValue::integer(5)]);
        let (column32, column64, mixed) = (terms.column(0), integers.column(0), both.column(0));
        assert!(matches!(column32, ColumnView::U32 { .. }));
        assert!(matches!(column64, ColumnView::U64 { .. }));
        assert!(matches!(mixed, ColumnView::Mixed { .. }));

        // A term sorts before an integer, and a payload beyond 32 bits
        // lies past every stored value of the tag.
        assert_eq!(column32.lower_bound(StorageValue::term(3)), 1);
        assert_eq!(column32.lower_bound(StorageValue::integer(0)), 2);
        assert_eq!(
            column32.lower_bound(StorageValue::term(u64::from(u32::MAX) + 1)),
            2
        );
        assert_eq!(column64.lower_bound(StorageValue::term(9)), 0);
        assert_eq!(column64.lower_bound(StorageValue::integer(5)), 1);
        assert_eq!(mixed.lower_bound(StorageValue::term(3)), 1);
        assert_eq!(mixed.lower_bound(StorageValue::integer(6)), 2);
        assert_eq!(mixed.lower_bound_in(1..2, StorageValue::term(0)), 1);
    }
}
