//! Structured values read in place from a dictionary.

use crate::{
    dictionary::{Dictionary, store::content::BlobsRef},
    value::ValueRef,
};

/// A tuple or map stored in a dictionary, read in place:
/// its label, and its elements, each decoded when it is read.
///
/// A map's elements are its keys and values, alternating, in key order.
#[derive(Clone, Copy)]
pub(crate) struct StoredStructure<'a> {
    /// The dictionary the elements decode through.
    dictionary: &'a Dictionary,
    /// The blobs of the entry.
    blobs: BlobsRef<'a>,
}

impl<'a> StoredStructure<'a> {
    /// Creates the reader of an entry's blobs.
    pub(super) fn new(dictionary: &'a Dictionary, blobs: BlobsRef<'a>) -> Self {
        Self { dictionary, blobs }
    }

    /// Returns the label, if the value has one.
    pub(crate) fn label(self) -> Option<&'a str> {
        self.blobs.label()
    }

    /// Returns the number of elements.
    pub(crate) fn len(self) -> usize {
        self.blobs.len()
    }

    /// Returns the element at a position, or `None` beyond the last one.
    pub(crate) fn element(self, position: usize) -> Option<ValueRef<'a>> {
        let stored = self.blobs.element(position)?;

        Some(
            self.dictionary
                .decode(stored)
                .expect("a stored structure references known entries"),
        )
    }
}
