//! This module implements [IntervalLookupColumnSingle]
//! and the associated builder [IntervalLookupColumnSingleBuilder].

use std::ops::Range;

use bytesize::ByteSize;

use crate::{
    columnar::{
        column::{Column, ColumnEnum},
        columnbuilder::{adaptive::ColumnBuilderAdaptive, ColumnBuilder},
        columnscan::ColumnScan,
    },
    management::ByteSized,
};

use super::{IntervalLookup, IntervalLookupBuilder};

/// Implementation of [IntervalLookup],
/// which internally uses a single [ColumnEnum]
/// to store the indices of the successor intervals
#[derive(Debug, Clone)]
pub(crate) struct IntervalLookupColumnSingle {
    /// [Column][crate::columnar::column::Column] that stores
    /// the start indices of intervals in the associated data column,
    /// which contain the successor nodes of the values from the previous layer
    ///
    /// This column contains one entry for every entry in the data column
    /// of the previous layer.
    ///
    /// An entry in this column might be `Self::EMPTY`
    /// to indicate that the corresponding node from the previous layer
    /// has no successor.
    ///
    /// The length of an interval at index `i` can be obtained
    /// by subtracting the value of this column at index `i`
    /// from the next value in this column that is not `Self::EMPTY`.
    interval_starts: ColumnEnum<usize>,
}

impl IntervalLookupColumnSingle {
    /// Value encoding that there is no successor for a particular trie node
    const EMPTY: usize = usize::MAX;

    /// Search `self.interval_starts` for the next entry that is not `Self::EMPTY`.
    ///
    /// Returns `None` if no such entry exists.
    fn find_next_interval_start(&self, start: usize) -> usize {
        let mut interval_scan = self.interval_starts.iter();
        interval_scan.narrow(start..self.interval_starts.len());

        for value in interval_scan {
            if value != Self::EMPTY {
                return value;
            }
        }

        unreachable!(
            "The builder for this column makes sure that the last entry is not Self::EMPTY"
        )
    }
}

impl IntervalLookup for IntervalLookupColumnSingle {
    type Builder = IntervalLookupColumnSingleBuilder;

    fn interval_bounds(&self, index: usize) -> Option<Range<usize>> {
        let interval_start = self.interval_starts.get(index);
        if interval_start == Self::EMPTY {
            return None;
        }

        let interval_end = self.find_next_interval_start(index + 1);

        Some(interval_start..interval_end)
    }
}

impl ByteSized for IntervalLookupColumnSingle {
    fn size_bytes(&self) -> ByteSize {
        self.interval_starts.size_bytes()
    }
}

#[derive(Debug, Default)]
pub(crate) struct IntervalLookupColumnSingleBuilder {
    /// [ColumnBuilderAdaptive] for building `interval_starts`
    builder: ColumnBuilderAdaptive<usize>,
    /// The end point of the last added interval
    last_end: usize,
}

impl IntervalLookupBuilder for IntervalLookupColumnSingleBuilder {
    type Lookup = IntervalLookupColumnSingle;

    fn add(&mut self, interval: Range<usize>) {
        if interval.is_empty() {
            self.builder.add(Self::Lookup::EMPTY);
        } else {
            self.builder.add(interval.start);
            self.last_end = interval.end;
        }
    }

    fn finalize(mut self) -> Self::Lookup {
        if self.builder.count() > 0 {
            self.builder.add(self.last_end)
        }

        Self::Lookup {
            interval_starts: self.builder.finalize(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::columnar::{
        column::Column,
        intervalcolumn::interval_lookup::{
            lookup_column_single::IntervalLookupColumnSingle, IntervalLookup, IntervalLookupBuilder,
        },
    };

    use super::IntervalLookupColumnSingleBuilder;

    #[test]
    fn interval_lookup_column_single() {
        let empty = IntervalLookupColumnSingle::EMPTY;

        let mut builder = IntervalLookupColumnSingleBuilder::default();
        builder.add(0..0);
        builder.add(0..0);
        builder.add(0..5);
        builder.add(5..7);
        builder.add(7..7);
        builder.add(7..7);
        builder.add(7..10);
        builder.add(10..10);

        let lookup_column = builder.finalize();
        let interval_starts = lookup_column.interval_starts.iter().collect::<Vec<usize>>();

        assert_eq!(
            interval_starts,
            vec![empty, empty, 0, 5, empty, empty, 7, empty, 10]
        );

        assert_eq!(lookup_column.interval_bounds(0), None);
        assert_eq!(lookup_column.interval_bounds(1), None);
        assert_eq!(lookup_column.interval_bounds(2), Some(0..5));
        assert_eq!(lookup_column.interval_bounds(3), Some(5..7));
        assert_eq!(lookup_column.interval_bounds(4), None);
        assert_eq!(lookup_column.interval_bounds(5), None);
        assert_eq!(lookup_column.interval_bounds(6), Some(7..10));
        assert_eq!(lookup_column.interval_bounds(7), None);
    }
}
