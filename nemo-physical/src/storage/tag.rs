//! The type tags of storage values.

/// The type of a [StorageValue](super::StorageValue).
///
/// The order of the tags is the first criterion of the [storage order](crate::storage).
/// It is neither the term order nor the value order of the values:
/// for example, every integer sorts before every decimal,
/// and every wide value after every inline value of its domain.
#[repr(u8)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Tag {
    /// The id of an RDF term in the dictionary,
    /// an IRI, a plain or language-tagged string, a literal of another datatype, or a null.
    ///
    /// The name follows RDF, whose terms these are.
    /// The term identity and the term order of the [values](crate::value) apply to every value,
    /// not only to the values of this tag.
    #[default]
    Term = 0,
    /// A boolean.
    Bool = 1,
    /// A 64-bit signed integer.
    Int = 2,
    /// An index into the dictionary's store of integers beyond 64 bits.
    WideInt = 3,
    /// A decimal number that is not an integer, within the inline range.
    Decimal = 4,
    /// An index into the dictionary's store of decimals beyond the inline range.
    WideDecimal = 5,
    /// A 32-bit floating point number.
    Float = 6,
    /// A 64-bit floating point number.
    Double = 7,
    /// A dateTime, with or without a timezone offset, within the inline range.
    DateTime = 8,
    /// An index into the dictionary's store of dateTimes beyond the inline range.
    WideDateTime = 9,
    /// A date, within the inline range.
    Date = 10,
    /// An index into the dictionary's store of dates beyond the inline range.
    WideDate = 11,
    /// A time, within the inline range.
    Time = 12,
    /// An index into the dictionary's store of times beyond the inline range.
    WideTime = 13,
    /// A duration of any duration domain, within the inline range.
    Duration = 14,
    /// An index into the dictionary's store of durations beyond the inline range.
    WideDuration = 15,
    /// An index into the dictionary's store of tuples.
    Tuple = 16,
    /// An index into the dictionary's store of maps.
    Map = 17,
}

impl Tag {
    /// Returns the tag with the given discriminant,
    /// or `None` if the byte names no tag.
    pub(crate) fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Tag::Term),
            1 => Some(Tag::Bool),
            2 => Some(Tag::Int),
            3 => Some(Tag::WideInt),
            4 => Some(Tag::Decimal),
            5 => Some(Tag::WideDecimal),
            6 => Some(Tag::Float),
            7 => Some(Tag::Double),
            8 => Some(Tag::DateTime),
            9 => Some(Tag::WideDateTime),
            10 => Some(Tag::Date),
            11 => Some(Tag::WideDate),
            12 => Some(Tag::Time),
            13 => Some(Tag::WideTime),
            14 => Some(Tag::Duration),
            15 => Some(Tag::WideDuration),
            16 => Some(Tag::Tuple),
            17 => Some(Tag::Map),
            _ => None,
        }
    }
}

/// The type of a storage value whose payload fits in 32 bits.
///
/// The payload of a dictionary entry fits only while the entry's index is small enough,
/// so the layout of a column checks its payloads before it uses this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Tag32 {
    /// The id of an RDF term in the dictionary.
    Term,
    /// A boolean.
    Bool,
    /// An index into the store of wide integers.
    WideInt,
    /// An index into the store of wide decimals.
    WideDecimal,
    /// A 32-bit floating point number.
    Float,
    /// An index into the store of wide dateTimes.
    WideDateTime,
    /// An index into the store of wide dates.
    WideDate,
    /// An index into the store of wide times.
    WideTime,
    /// An index into the store of wide durations.
    WideDuration,
    /// An index into the store of tuples.
    Tuple,
    /// An index into the store of maps.
    Map,
}

impl Tag32 {
    /// Returns the [Tag] of this 32-bit tag.
    pub(crate) fn tag(self) -> Tag {
        match self {
            Tag32::Term => Tag::Term,
            Tag32::Bool => Tag::Bool,
            Tag32::WideInt => Tag::WideInt,
            Tag32::WideDecimal => Tag::WideDecimal,
            Tag32::Float => Tag::Float,
            Tag32::WideDateTime => Tag::WideDateTime,
            Tag32::WideDate => Tag::WideDate,
            Tag32::WideTime => Tag::WideTime,
            Tag32::WideDuration => Tag::WideDuration,
            Tag32::Tuple => Tag::Tuple,
            Tag32::Map => Tag::Map,
        }
    }

    /// Returns the 32-bit tag of a [Tag] whose payload fits in 32 bits,
    /// or `None` for a 64-bit type.
    pub(crate) fn from_tag(tag: Tag) -> Option<Self> {
        match tag {
            Tag::Term => Some(Tag32::Term),
            Tag::Bool => Some(Tag32::Bool),
            Tag::WideInt => Some(Tag32::WideInt),
            Tag::WideDecimal => Some(Tag32::WideDecimal),
            Tag::Float => Some(Tag32::Float),
            Tag::WideDateTime => Some(Tag32::WideDateTime),
            Tag::WideDate => Some(Tag32::WideDate),
            Tag::WideTime => Some(Tag32::WideTime),
            Tag::WideDuration => Some(Tag32::WideDuration),
            Tag::Tuple => Some(Tag32::Tuple),
            Tag::Map => Some(Tag32::Map),
            Tag::Int
            | Tag::Decimal
            | Tag::Double
            | Tag::DateTime
            | Tag::Date
            | Tag::Time
            | Tag::Duration => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every tag converts to its byte and back, the bytes follow the order of the tags,
    /// and a 32-bit tag converts back to its tag.
    #[test]
    fn bytes_round_trip_in_order() {
        let mut previous = None;

        for byte in 0..=u8::MAX {
            let Some(tag) = Tag::from_byte(byte) else {
                assert!(byte >= 18);
                continue;
            };

            assert_eq!(tag as u8, byte);

            if let Some(previous) = previous {
                assert!(previous < tag);
            }

            previous = Some(tag);

            assert!(Tag32::from_tag(tag).is_none_or(|tag32| tag32.tag() == tag));
        }
    }
}
