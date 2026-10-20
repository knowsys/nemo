//! The blob encoding of structured values.
//!
//! A structured value is stored as two byte strings:
//! - a *type blob*: the element count, one tag byte per element,
//!   and the label if there is one, which all values of one shape share;
//! - a *content blob*: one payload of [ELEMENT_SIZE] bytes per element.
//!
//! The split lets the many same-shaped tuples of a dataset share one interned type blob.
//! [Blobs] builds the two byte strings, and [BlobsRef] reads an element out of them in place.

use crate::storage::{StorageValue, Tag};

/// The bytes a content blob stores per element: one storage payload.
pub(crate) const ELEMENT_SIZE: usize = size_of::<u64>();

/// The byte offset of the tag bytes within a type blob,
/// behind the element count and the label flag.
const HEADER_SIZE: usize = 5;

/// The flag marking a type blob that carries a label.
const HAS_LABEL: u8 = 1;

/// The two blobs a structured value is stored as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Blobs {
    /// The type blob: the element count, the tags, and the label.
    pub(crate) type_blob: Vec<u8>,
    /// The content blob: the payloads of the elements.
    pub(crate) content: Vec<u8>,
}

impl Blobs {
    /// Builds the blobs of a structured value from its label and its encoded elements.
    ///
    /// The element count must fit the header's 32 bits,
    /// which the dictionary's element limit ensures.
    pub(crate) fn build(label: Option<&str>, elements: &[StorageValue]) -> Self {
        let count = u32::try_from(elements.len())
            .expect("the dictionary refuses structures beyond its element limit");

        let mut type_blob =
            Vec::with_capacity(HEADER_SIZE + elements.len() + label.map_or(0, str::len));
        type_blob.extend_from_slice(&count.to_le_bytes());
        type_blob.push(if label.is_some() { HAS_LABEL } else { 0 });
        type_blob.extend(elements.iter().map(|element| element.tag as u8));
        if let Some(label) = label {
            type_blob.extend_from_slice(label.as_bytes());
        }

        let mut content = Vec::with_capacity(elements.len() * ELEMENT_SIZE);
        for element in elements {
            content.extend_from_slice(&element.payload.to_le_bytes());
        }

        Self { type_blob, content }
    }
}

/// The blobs of a structured value, read in place:
/// the label, and the tag and the payload of each element.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlobsRef<'a> {
    /// The label, if the value has one.
    label: Option<&'a str>,
    /// One tag byte per element.
    tags: &'a [u8],
    /// One payload per element.
    payloads: &'a [[u8; ELEMENT_SIZE]],
}

impl<'a> BlobsRef<'a> {
    /// Parses a type blob and a content blob,
    /// or returns `None` for malformed blobs.
    ///
    /// The blobs that [build](Blobs::build) builds parse.
    pub(crate) fn parse(type_blob: &'a [u8], content: &'a [u8]) -> Option<Self> {
        let count = u32::from_le_bytes(type_blob.get(0..4)?.try_into().ok()?) as usize;
        let flags = *type_blob.get(4)?;

        let tags = type_blob.get(HEADER_SIZE..HEADER_SIZE + count)?;
        if tags.iter().any(|&byte| Tag::from_byte(byte).is_none()) {
            return None;
        }

        let rest = &type_blob[HEADER_SIZE + count..];
        let label = if flags & HAS_LABEL != 0 {
            Some(std::str::from_utf8(rest).ok()?)
        } else if rest.is_empty() {
            None
        } else {
            return None;
        };

        if content.len() != count * ELEMENT_SIZE {
            return None;
        }

        Some(Self {
            label,
            tags,
            payloads: content.as_chunks::<ELEMENT_SIZE>().0,
        })
    }

    /// Reads blobs that [build](Blobs::build) built, without validating them,
    /// in time independent of their size.
    pub(crate) fn trusted(type_blob: &'a [u8], content: &'a [u8]) -> Self {
        debug_assert!(
            Self::parse(type_blob, content).is_some(),
            "trusted blobs are well-formed"
        );

        let count = u32::from_le_bytes(type_blob[0..4].try_into().expect("the count is four bytes"))
            as usize;
        let rest = &type_blob[HEADER_SIZE + count..];
        // SAFETY: `build` writes the label of a labeled value from a `&str`.
        let label =
            (type_blob[4] & HAS_LABEL != 0).then(|| unsafe { std::str::from_utf8_unchecked(rest) });

        Self {
            label,
            tags: &type_blob[HEADER_SIZE..HEADER_SIZE + count],
            payloads: content.as_chunks::<ELEMENT_SIZE>().0,
        }
    }

    /// Returns the label, if the value has one.
    pub(crate) fn label(self) -> Option<&'a str> {
        self.label
    }

    /// Returns the number of elements.
    pub(crate) fn len(self) -> usize {
        self.tags.len()
    }

    /// Returns the element at a position, or `None` beyond the last one.
    pub(crate) fn element(self, position: usize) -> Option<StorageValue> {
        let tag = Tag::from_byte(*self.tags.get(position)?).expect("parsed tags are valid");

        Some(StorageValue::new(
            tag,
            u64::from_le_bytes(self.payloads[position]),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds blobs and reads them back, asserting a round trip.
    fn round_trip(label: Option<&str>, elements: &[StorageValue]) {
        let Blobs { type_blob, content } = Blobs::build(label, elements);

        for read in [
            BlobsRef::parse(&type_blob, &content).expect("built blobs must parse"),
            BlobsRef::trusted(&type_blob, &content),
        ] {
            assert_eq!(read.label(), label);
            assert_eq!(read.len(), elements.len());
            let read: Vec<_> = (0..read.len())
                .map(|position| read.element(position).expect("within the length"))
                .collect();
            assert_eq!(read, elements);
        }
    }

    #[test]
    fn blobs_round_trip() {
        round_trip(None, &[]);
        round_trip(
            Some("point"),
            &[StorageValue::integer(1), StorageValue::double(2.5)],
        );
        round_trip(None, &[StorageValue::term(42), StorageValue::boolean(true)]);
        round_trip(Some(""), &[StorageValue::integer(-1)]);
    }

    #[test]
    fn same_shape_yields_the_same_type_blob() {
        let Blobs {
            type_blob: first_type,
            content: first_content,
        } = Blobs::build(
            Some("point"),
            &[StorageValue::integer(1), StorageValue::integer(2)],
        );
        let Blobs {
            type_blob: second_type,
            content: second_content,
        } = Blobs::build(
            Some("point"),
            &[StorageValue::integer(3), StorageValue::integer(4)],
        );

        assert_eq!(first_type, second_type);
        assert_ne!(first_content, second_content);
    }

    #[test]
    fn malformed_blobs_are_rejected() {
        assert!(BlobsRef::parse(&[], &[]).is_none());
        // The count claims one element, but no tag byte follows.
        assert!(BlobsRef::parse(&[1, 0, 0, 0, 0], &[0; 8]).is_none());
        // An invalid tag byte.
        assert!(BlobsRef::parse(&[1, 0, 0, 0, 0, 200], &[0; 8]).is_none());
        // A content length mismatch.
        assert!(BlobsRef::parse(&[1, 0, 0, 0, 0, 2], &[0; 4]).is_none());
        // Trailing bytes without the label flag.
        assert!(BlobsRef::parse(&[0, 0, 0, 0, 0, b'x'], &[]).is_none());
    }
}
