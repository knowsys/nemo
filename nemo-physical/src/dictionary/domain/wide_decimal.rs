//! The stores of exact numbers beyond the compact ranges.

use crate::{dictionary::store::RecordPool, value::number::decimal};

/// The fixed part of a wide decimal's record: its sign and exponent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Header {
    /// Whether the value is negative.
    negative: bool,
    /// The base-10 exponent.
    exponent: i64,
}

/// A store of wide decimals,
/// kept as records of their sign, exponent and magnitude bytes.
///
/// Identity is the value: the parts are normalized,
/// so equal records are equal values,
/// and a read borrows them straight into the [decimal::WideRef] view.
/// The dictionary keeps one store per tag,
/// the wide integers apart from the other wide decimals.
pub(crate) struct WideDecimalStore {
    /// The (header, magnitude) records.
    records: RecordPool<Header>,
}

impl WideDecimalStore {
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

    /// Interns a wide decimal and returns its index,
    /// or `None` if the store cannot hold it.
    pub(crate) fn intern(&mut self, value: decimal::WideRef<'_>) -> Option<u64> {
        self.records.intern(Self::header(value), value.magnitude())
    }

    /// Returns the index of a wide decimal, or `None` if it was never interned.
    pub(crate) fn find(&self, value: decimal::WideRef<'_>) -> Option<u64> {
        self.records.find(Self::header(value), value.magnitude())
    }

    /// Returns the wide decimal with the given index, borrowed from the store,
    /// or `None` for an unknown index.
    pub(crate) fn get(&self, index: u64) -> Option<decimal::WideRef<'_>> {
        self.records.get(index).map(|(header, magnitude)| {
            decimal::WideRef::new(header.negative, header.exponent, magnitude)
        })
    }

    /// Returns the fixed part of a value's record.
    fn header(value: decimal::WideRef<'_>) -> Header {
        Header {
            negative: value.is_negative(),
            exponent: value.exponent(),
        }
    }

    /// Returns the allocated bytes of the store.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.records.memory_bytes()
    }
}
