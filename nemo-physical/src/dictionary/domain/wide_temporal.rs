//! The stores of temporal values beyond the compact range.

use std::hash::Hash;

use crate::{
    dictionary::store::RecordPool,
    value::{Date, DateTime, Duration, Time},
};

/// The store of wide dateTimes.
pub(crate) type WideDateTimeStore = WideTemporalStore<DateTime>;

/// The store of wide dates.
pub(crate) type WideDateStore = WideTemporalStore<Date>;

/// The store of wide times.
pub(crate) type WideTimeStore = WideTemporalStore<Time>;

/// The store of wide durations.
pub(crate) type WideDurationStore = WideTemporalStore<Duration>;

/// The store of the wide values of one temporal type:
/// dateTimes, dates, times or durations.
///
/// A temporal value has a fixed size, so the whole value is the header of its record,
/// and the record has no variable bytes.
/// Two records are equal exactly if their values are equal.
pub(crate) struct WideTemporalStore<T> {
    /// The records, whose headers are the values.
    records: RecordPool<T>,
}

impl<T: Copy + Eq + Hash> WideTemporalStore<T> {
    /// Creates an empty store.
    pub(crate) fn new() -> Self {
        Self {
            records: RecordPool::new(),
        }
    }

    /// Returns the number of entries.
    pub(crate) fn len(&self) -> u64 {
        self.records.len()
    }

    /// Interns a value and returns its index.
    ///
    /// The result is never `None`, since the record of a temporal value has no bytes to store.
    pub(crate) fn intern(&mut self, value: T) -> Option<u64> {
        self.records.intern(value, &[])
    }

    /// Returns the index of a value, or `None` if it was never interned.
    pub(crate) fn find(&self, value: T) -> Option<u64> {
        self.records.find(value, &[])
    }

    /// Returns the value with the given index, or `None` for an unknown index.
    pub(crate) fn get(&self, index: u64) -> Option<T> {
        self.records.get(index).map(|(value, _)| value)
    }

    /// Returns the allocated bytes of the store.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.records.memory_bytes()
    }
}
