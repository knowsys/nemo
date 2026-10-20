//! A set of rows emptied per group.

use std::hash::BuildHasher;

use hashbrown::DefaultHashBuilder;

use crate::storage::StorageValue;

/// A set of rows of one width, emptied at the start of every group.
///
/// The rows lie back to back in one buffer,
/// and an open-addressed table holds their offsets.
/// Each slot also carries the number of the group that wrote it,
/// so a new group empties the set in constant time.
#[derive(Debug)]
pub(crate) struct RowSet {
    /// The number of values per row;
    /// a set of empty rows holds the empty row at most once per group.
    width: usize,
    /// The open-addressed slots: per slot,
    /// the number of the group that wrote it above the row's offset in `rows`.
    slots: Vec<u64>,
    /// The rows of the current group, back to back.
    rows: Vec<StorageValue>,
    /// The number of the current group;
    /// a slot of another group is free, and 0 marks a slot never written.
    group: u64,
    /// The number of rows in the current group.
    len: usize,
    /// The hasher of the rows.
    build: DefaultHashBuilder,
}

impl RowSet {
    /// The number of low bits of a slot holding the row's offset in `rows`.
    ///
    /// A group reaches the limit only with 2^40 values in its rows, 16 TiB.
    const OFFSET_BITS: u32 = 40;

    /// The mask selecting the offset of a slot.
    const OFFSET_MASK: u64 = (1 << Self::OFFSET_BITS) - 1;

    /// The number of groups a slot can tell apart before the group number wraps.
    const GROUPS: u64 = 1 << (u64::BITS - Self::OFFSET_BITS);

    /// Creates an empty set of rows of the given width.
    pub(crate) fn new(width: usize) -> Self {
        Self {
            width,
            slots: vec![0; 64],
            rows: Vec::new(),
            group: 1,
            len: 0,
            build: DefaultHashBuilder::default(),
        }
    }

    /// Returns the number of values per row.
    pub(crate) fn width(&self) -> usize {
        self.width
    }

    /// Starts a new group, emptying the set in constant time.
    pub(crate) fn begin_group(&mut self) {
        self.rows.clear();
        self.len = 0;
        self.group += 1;

        if self.group == Self::GROUPS {
            self.slots.fill(0);
            self.group = 1;
        }
    }

    /// Inserts a row, and returns whether the current group did not hold it yet.
    ///
    /// Panics if the rows of one group reach 2^40 values.
    pub(crate) fn insert(&mut self, row: &[StorageValue]) -> bool {
        debug_assert_eq!(row.len(), self.width);

        if self.len * 10 >= self.slots.len() * 7 {
            self.grow();
        }

        let mask = self.slots.len() - 1;

        // Masked into the table: truncation cannot reach past it.
        #[allow(clippy::cast_possible_truncation)]
        let mut slot = self.build.hash_one(row) as usize & mask;

        loop {
            let entry = self.slots[slot];

            if Self::group_of(entry) != self.group {
                let offset = self.rows.len() as u64;
                assert!(
                    offset <= Self::OFFSET_MASK,
                    "a group of a row set holds fewer than 2^40 values"
                );

                self.rows.extend_from_slice(row);
                self.slots[slot] = self.group << Self::OFFSET_BITS | offset;
                self.len += 1;

                return true;
            }

            let offset = Self::offset(entry);
            if &self.rows[offset..offset + self.width] == row {
                return false;
            }

            slot = (slot + 1) & mask;
        }
    }

    /// Doubles the table, re-slotting the rows of the current group.
    fn grow(&mut self) {
        let capacity = self.slots.len() * 2;
        let mut slots = vec![0; capacity];
        let mask = capacity - 1;

        for &entry in &self.slots {
            if Self::group_of(entry) != self.group {
                continue;
            }

            let offset = Self::offset(entry);
            let row = &self.rows[offset..offset + self.width];

            // Masked into the table: truncation cannot reach past it.
            #[allow(clippy::cast_possible_truncation)]
            let mut slot = self.build.hash_one(row) as usize & mask;
            while Self::group_of(slots[slot]) == self.group {
                slot = (slot + 1) & mask;
            }
            slots[slot] = entry;
        }

        self.slots = slots;
    }

    /// Returns the number of the group that wrote a slot.
    #[inline]
    fn group_of(entry: u64) -> u64 {
        entry >> Self::OFFSET_BITS
    }

    /// Returns the offset in `rows` a slot holds.
    #[inline]
    fn offset(entry: u64) -> usize {
        // An offset indexes `rows`, so it fits a `usize`.
        #[allow(clippy::cast_possible_truncation)]
        let offset = (entry & Self::OFFSET_MASK) as usize;
        offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::int;

    #[test]
    fn row_sets_dedup_within_a_group_and_reset_between() {
        let mut set = RowSet::new(2);
        assert!(set.insert(&[int(1), int(2)]));
        assert!(!set.insert(&[int(1), int(2)]));
        assert!(set.insert(&[int(1), int(3)]));
        set.begin_group();
        assert!(set.insert(&[int(1), int(2)]));
    }

    #[test]
    fn row_sets_survive_growth() {
        let mut set = RowSet::new(1);
        for value in 0..1000 {
            assert!(set.insert(&[int(value)]));
        }
        for value in 0..1000 {
            assert!(!set.insert(&[int(value)]));
        }
    }

    #[test]
    fn row_sets_wipe_their_slots_when_the_group_number_wraps() {
        let mut set = RowSet::new(1);
        assert!(set.insert(&[int(1)]));
        // Skip to the last group number, as after 2^24 - 2 groups
        // that never touched the slot written above.
        set.group = RowSet::GROUPS - 1;
        set.begin_group();
        assert_eq!(set.group, 1);
        // The slot of the first group was wiped, not taken as current.
        assert!(set.insert(&[int(1)]));
        assert!(!set.insert(&[int(1)]));
    }

    #[test]
    fn zero_width_sets_admit_one_row_per_group() {
        let mut set = RowSet::new(0);
        assert!(set.insert(&[]));
        assert!(!set.insert(&[]));
        set.begin_group();
        assert!(set.insert(&[]));
    }
}
