//! The column kind: the storage a column takes, decided before its values arrive.

use crate::{
    storage::{StorageValue, Tag, Tag32},
    trie::ColumnView,
};

/// The kind of storage a column takes, decided before its values arrive,
/// from a scan of the values it will hold or from the kinds of the columns it merges,
/// so that every value is stored in its final form and the storage is sized once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColumnKind {
    /// One tag with a 32-bit encoding, every payload within 32 bits.
    U32(Tag32),
    /// One tag, with the tag or some payload needing 64 bits.
    U64(Tag),
    /// Several tags, or no values at all.
    Mixed,
}

impl ColumnKind {
    /// Returns the kind of a column holding the given values.
    #[cfg(test)]
    pub(super) fn of(values: impl Iterator<Item = StorageValue>) -> Self {
        let mut scan = KindScan::default();
        for value in values {
            scan.add(value);
        }
        scan.kind()
    }

    /// Returns the kind of a column merging the given columns:
    /// uniform if every nonempty column shares one tag,
    /// 32-bit if all of them are, mixed otherwise.
    pub(crate) fn merged<'a>(columns: impl Iterator<Item = ColumnView<'a>>) -> Self {
        // The tag seen so far and, while every column was 32-bit, its 32-bit form.
        let mut seen: Option<(Tag, Option<Tag32>)> = None;
        for column in columns.filter(|column| !column.is_empty()) {
            let (tag, tag32) = match column {
                ColumnView::U32 { tag, .. } => (tag.tag(), Some(tag)),
                ColumnView::U64 { tag, .. } => (tag, None),
                ColumnView::Mixed { .. } => return ColumnKind::Mixed,
            };
            seen = Some(match seen {
                None => (tag, tag32),
                Some((before, _)) if before != tag => return ColumnKind::Mixed,
                Some((before, before32)) => (before, before32.and(tag32)),
            });
        }

        match seen {
            None => ColumnKind::Mixed,
            Some((_, Some(tag32))) => ColumnKind::U32(tag32),
            Some((tag, None)) => ColumnKind::U64(tag),
        }
    }
}

/// The running statistics a [ColumnKind] is decided from:
/// whether the values share one tag, and the largest payload.
#[derive(Debug, Default, Clone)]
pub(crate) struct KindScan {
    /// The tags seen so far.
    tags: TagState,
    /// The largest payload so far.
    max_payload: u64,
}

/// Whether the values seen so far share a single tag.
#[derive(Debug, Default, Clone, Copy)]
enum TagState {
    /// No values seen yet.
    #[default]
    Empty,
    /// Every value so far has this tag.
    Uniform(Tag),
    /// More than one tag has been seen.
    Mixed,
}

impl KindScan {
    /// Accounts one value.
    pub(crate) fn add(&mut self, value: StorageValue) {
        self.max_payload = self.max_payload.max(value.payload);
        self.tags = match self.tags {
            TagState::Empty => TagState::Uniform(value.tag),
            TagState::Uniform(tag) if tag == value.tag => TagState::Uniform(tag),
            TagState::Uniform(_) | TagState::Mixed => TagState::Mixed,
        };
    }

    /// Returns the kind the values seen so far take.
    pub(crate) fn kind(&self) -> ColumnKind {
        match self.tags {
            TagState::Empty | TagState::Mixed => ColumnKind::Mixed,
            TagState::Uniform(tag) => {
                match Tag32::from_tag(tag).filter(|_| u32::try_from(self.max_payload).is_ok()) {
                    Some(tag32) => ColumnKind::U32(tag32),
                    None => ColumnKind::U64(tag),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::trie::{column::Column, construct::builder::ColumnBuilder};

    #[test]
    fn merged_layouts_widen_as_needed() {
        let column32 = ColumnBuilder::single_block(&[StorageValue::term(1)]);
        let column64 = ColumnBuilder::single_block(&[StorageValue::term(u64::from(u32::MAX) + 1)]);
        let integers = ColumnBuilder::single_block(&[StorageValue::integer(1)]);
        let empty = ColumnBuilder::new(ColumnKind::Mixed, 0, 0).finish();

        let merged =
            |columns: &[&Column]| ColumnKind::merged(columns.iter().map(|column| column.view()));
        assert_eq!(
            merged(&[&column32, &column32]),
            ColumnKind::U32(Tag32::Term)
        );
        assert_eq!(merged(&[&column32, &column64]), ColumnKind::U64(Tag::Term));
        assert_eq!(merged(&[&column32, &integers]), ColumnKind::Mixed);
        assert_eq!(merged(&[&empty, &column32]), ColumnKind::U32(Tag32::Term));
        assert_eq!(merged(&[&empty]), ColumnKind::Mixed);
    }
}
