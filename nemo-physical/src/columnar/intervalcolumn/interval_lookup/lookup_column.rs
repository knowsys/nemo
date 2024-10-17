//! This module implements [IntervalLookupColumn]
//! and the associated builder [IntervalLookupColumnBuilder].

use crate::{
    columnar::{
        column::{Column, ColumnEnum},
        columnbuilder::{adaptive::ColumnBuilderAdaptive, ColumnBuilder},
    },
    management::bytesized::ByteSized,
};

use super::{IntervalLookup, IntervalLookupBuilder};

/// Implementation of [IntervalLookup],
/// which internally uses a [ColumnEnum] to associate
/// data nodes from the previous layer with interval indices of
/// the successor nodes of the current layer
#[derive(Debug, Clone)]
pub(crate) struct IntervalLookupColumn {
    /// [Column] that associates each node of the previous layer
    /// with an interval index for `interval_starts`
    ///
    /// An entry in this column might be `Self::Empty`
    /// to indicate that the corresponding node from the previous layer
    /// has no successor.
    lookup: ColumnEnum<usize>,

    // Whether the lookup column is just the identity
    simple: bool,
}

impl IntervalLookupColumn {
    /// Value encoding that there is no successor for a particular trie node
    const EMPTY: usize = usize::MAX;
}

impl IntervalLookup for IntervalLookupColumn {
    type Builder = IntervalLookupColumnBuilder;

    fn interval_index(&self, index: usize) -> Option<usize> {
        if self.simple {
            debug_assert!(index == self.lookup.get(index));

            return Some(index);
        }

        let interval_index = self.lookup.get(index);

        if interval_index == Self::EMPTY {
            None
        } else {
            Some(interval_index)
        }
    }
}

impl ByteSized for IntervalLookupColumn {
    fn size_bytes(&self) -> u64 {
        self.lookup.size_bytes()
    }
}

#[derive(Debug, Default)]
pub(crate) struct IntervalLookupColumnBuilder {
    /// [ColumnBuilderAdaptive] for building `predecessors`
    builder_lookup: ColumnBuilderAdaptive<usize>,

    /// Whether `builder_lookup` contains no [IntervalLookupColumn::EMPTY]
    /// and can therefore be ignored
    simple: bool,
}

impl IntervalLookupBuilder for IntervalLookupColumnBuilder {
    type Lookup = IntervalLookupColumn;

    fn add_interval(&mut self, interval_count: usize) {
        if self.builder_lookup.count() == 0 && interval_count == 0 {
            self.simple = true;
        }

        if self.simple && self.builder_lookup.count() != interval_count {
            self.simple = false;
        }

        self.builder_lookup.add(interval_count);
    }

    fn add_empty(&mut self) {
        self.builder_lookup.add(Self::Lookup::EMPTY);
        self.simple = false;
    }

    fn finalize(self) -> Self::Lookup {
        Self::Lookup {
            lookup: self.builder_lookup.finalize(),
            simple: self.simple,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::columnar::{
        column::Column,
        intervalcolumn::interval_lookup::{IntervalLookup, IntervalLookupBuilder},
    };

    use super::{IntervalLookupColumn, IntervalLookupColumnBuilder};

    #[test]
    fn interval_lookup_column() {
        let empty = IntervalLookupColumn::EMPTY;

        let mut builder = IntervalLookupColumnBuilder::default();
        builder.add_empty();
        builder.add_empty();
        builder.add_interval(0);
        builder.add_interval(1);
        builder.add_empty();
        builder.add_interval(2);
        builder.add_interval(3);
        builder.add_empty();

        let lookup_column = builder.finalize();
        let lookup = lookup_column.lookup.iter().collect::<Vec<usize>>();

        assert_eq!(lookup, vec![empty, empty, 0, 1, empty, 2, 3, empty]);

        assert_eq!(lookup_column.interval_index(0), None);
        assert_eq!(lookup_column.interval_index(1), None);
        assert_eq!(lookup_column.interval_index(2), Some(0));
        assert_eq!(lookup_column.interval_index(3), Some(1));
        assert_eq!(lookup_column.interval_index(4), None);
        assert_eq!(lookup_column.interval_index(5), Some(2));
        assert_eq!(lookup_column.interval_index(6), Some(3));
        assert_eq!(lookup_column.interval_index(7), None);
    }
}
