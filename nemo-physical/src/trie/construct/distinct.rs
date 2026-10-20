//! The distinct filter: the distinct rows of one sorted stream, each with its change.
//!
//! Every trie build reads rows in storage order and needs
//! the distinct rows, and for each the [column it changed at](RowChange),
//! which tells the columns below where their blocks start.

use crate::{storage::StorageValue, trie::construct::builder::RowChange};

/// Where a [Distinct] filter keeps the last row it passed:
/// by reference if the rows live on behind the filter, as in a run,
/// and by value if they do not, as in the rows a cursor overwrites.
pub(super) trait Keep<'r> {
    /// Returns the last row kept, or `None` before the first.
    fn last(&self) -> Option<&[StorageValue]>;

    /// Keeps a row, which differs from the last one from `changed` on.
    fn keep(&mut self, row: &'r [StorageValue], changed: usize);
}

/// The last row, by reference.
#[derive(Debug, Default)]
pub(super) struct Borrowed<'r>(Option<&'r [StorageValue]>);

impl<'r> Keep<'r> for Borrowed<'r> {
    #[inline]
    fn last(&self) -> Option<&[StorageValue]> {
        self.0
    }

    #[inline]
    fn keep(&mut self, row: &'r [StorageValue], _changed: usize) {
        self.0 = Some(row);
    }
}

/// The last row, by value; only its changed suffix is copied.
#[derive(Debug, Default)]
pub(super) struct Copied(Vec<StorageValue>);

impl<'r> Keep<'r> for Copied {
    #[inline]
    fn last(&self) -> Option<&[StorageValue]> {
        (!self.0.is_empty()).then_some(self.0.as_slice())
    }

    #[inline]
    fn keep(&mut self, row: &'r [StorageValue], changed: usize) {
        if self.0.is_empty() {
            self.0.extend_from_slice(row);
        } else {
            self.0[changed..].copy_from_slice(&row[changed..]);
        }
    }
}

/// A filter passing the distinct rows of a sorted stream, each with its [RowChange];
/// a repeat of the last row passed is dropped.
#[derive(Debug)]
pub(super) struct Distinct<K> {
    /// The last row passed.
    kept: K,
}

impl Distinct<Borrowed<'_>> {
    /// Creates a filter over rows that live on behind it.
    pub(super) fn borrowed() -> Self {
        Self {
            kept: Borrowed::default(),
        }
    }
}

impl Distinct<Copied> {
    /// Creates a filter over rows of the given arity that must be copied to be kept.
    pub(super) fn copied(arity: usize) -> Self {
        Self {
            kept: Copied(Vec::with_capacity(arity)),
        }
    }
}

impl<'r, K: Keep<'r>> Distinct<K> {
    /// Passes a row, which must not precede the last one:
    /// how it differs from that row, or `None` for a repeat.
    #[inline]
    pub(super) fn pass(&mut self, row: &'r [StorageValue]) -> Option<RowChange> {
        let Some(last) = self.kept.last() else {
            self.kept.keep(row, 0);
            return Some(RowChange::First);
        };
        debug_assert!(last <= row, "rows arrive in storage order");

        let changed = row
            .iter()
            .zip(last)
            .position(|(now, before)| now != before)?;
        self.kept.keep(row, changed);
        Some(RowChange::At(changed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_rows_report_where_they_changed() {
        let run =
            [[1, 1], [1, 1], [1, 2], [2, 2], [2, 2]].map(|row| row.map(StorageValue::integer));

        let mut distinct = Distinct::borrowed();
        let changes: Vec<Option<RowChange>> = run.iter().map(|row| distinct.pass(row)).collect();

        assert_eq!(
            changes,
            [
                Some(RowChange::First),
                None,
                Some(RowChange::At(1)),
                Some(RowChange::At(0)),
                None,
            ]
        );
    }

    #[test]
    fn copied_rows_survive_the_source_moving_on() {
        let mut distinct = Distinct::copied(2);
        let mut row = [1, 1].map(StorageValue::integer);

        assert_eq!(distinct.pass(&row), Some(RowChange::First));

        row[1] = StorageValue::integer(2);

        assert_eq!(distinct.pass(&row), Some(RowChange::At(1)));
        assert_eq!(distinct.pass(&row), None);
    }
}
