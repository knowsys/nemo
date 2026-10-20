//! The storage value.

use crate::{
    storage::{encoding::PayloadEncoding, tag::Tag},
    value::{
        Date, DateTime, DecimalRef, Double, Duration, Float, NumberRef, Time, ValueRef,
        number::decimal,
    },
};

/// A single value as stored in a trie: its [Tag] and the payload encoding it.
///
/// Comparing two values of the same tag compares the values they encode,
/// and the derived order on `(tag, payload)` is the storage order.
///
/// # Example
///
/// ```
/// use nemo_physical::{
///     storage::{StorageValue, Tag},
///     value::{DataValue, NumberRef, ValueRef},
/// };
///
/// // An integer is its own payload, and decodes without a dictionary.
/// let five = StorageValue::integer(5);
/// assert_eq!(five.tag, Tag::Int);
/// assert_eq!(five.to_value(), Some(ValueRef::Number(NumberRef::Decimal(5.into()))));
/// assert!(StorageValue::integer(-3) < five);
///
/// // A string has no inline payload: a dictionary stores it.
/// assert_eq!(StorageValue::from_value(DataValue::string("five").as_ref()), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageValue {
    /// The type of the value.
    pub tag: Tag,
    /// The payload encoding the value.
    pub(crate) payload: u64,
}

impl StorageValue {
    /// Creates a storage value from a tag and its payload.
    pub(crate) fn new(tag: Tag, payload: u64) -> Self {
        Self { tag, payload }
    }

    /// Creates a storage value from a dictionary term id.
    pub fn term(id: u64) -> Self {
        Self::new(Tag::Term, id)
    }

    /// Creates a storage value from a boolean.
    pub fn boolean(value: bool) -> Self {
        Self::new(Tag::Bool, value.to_payload())
    }

    /// Creates a storage value from a signed integer.
    pub fn integer(value: i64) -> Self {
        Self::new(Tag::Int, value.to_payload())
    }

    /// Creates a storage value from a 32-bit float.
    pub fn float(value: impl Into<Float>) -> Self {
        Self::new(Tag::Float, value.into().to_payload())
    }

    /// Creates a storage value from a 64-bit float.
    pub fn double(value: impl Into<Double>) -> Self {
        Self::new(Tag::Double, value.into().to_payload())
    }

    /// Creates a storage value from a compact decimal.
    pub(crate) fn decimal(value: decimal::Compact) -> Self {
        Self::new(Tag::Decimal, value.to_payload())
    }

    /// Creates a storage value from a dateTime,
    /// or `None` for a dateTime the payload cannot hold.
    pub fn datetime(value: DateTime) -> Option<Self> {
        Self::encode(value)
    }

    /// Creates a storage value through a type's [PayloadEncoding],
    /// or `None` if the encoding rejects the value.
    fn encode<T: PayloadEncoding>(value: T) -> Option<Self> {
        value.to_payload().into().map(|payload| Self {
            tag: T::TAG,
            payload,
        })
    }

    /// Encodes a value inline,
    /// or `None` if the value lives in the dictionary.
    pub fn from_value(value: ValueRef<'_>) -> Option<Self> {
        match value {
            ValueRef::Boolean(value) => Some(Self::boolean(value)),
            ValueRef::Number(NumberRef::Decimal(value)) => match value.0 {
                decimal::ReprRef::Int(value) => Some(Self::integer(value)),
                decimal::ReprRef::Compact(value) => Some(Self::decimal(value)),
                decimal::ReprRef::Wide(_) => None,
            },
            ValueRef::Number(NumberRef::Float(value)) => Some(Self::float(value)),
            ValueRef::Number(NumberRef::Double(value)) => Some(Self::double(value)),
            ValueRef::DateTime(value) => Self::encode(value),
            ValueRef::Date(value) => Self::encode(value),
            ValueRef::Time(value) => Self::encode(value),
            ValueRef::Duration(value) => Self::encode(value),
            ValueRef::Iri(_)
            | ValueRef::String(_)
            | ValueRef::LanguageString(_)
            | ValueRef::Null(_)
            | ValueRef::Tuple(_)
            | ValueRef::Map(_)
            | ValueRef::Other(_) => None,
        }
    }

    /// Decodes an inline value,
    /// or `None` for a dictionary-backed tag, which only the dictionary resolves.
    pub fn to_value(self) -> Option<ValueRef<'static>> {
        match self.tag {
            Tag::Bool => Some(ValueRef::Boolean(bool::from_payload(self.payload))),
            Tag::Int => Some(ValueRef::Number(NumberRef::Decimal(DecimalRef(
                decimal::ReprRef::Int(i64::from_payload(self.payload)),
            )))),
            Tag::Decimal => Some(ValueRef::Number(NumberRef::Decimal(DecimalRef(
                decimal::ReprRef::Compact(decimal::Compact::from_payload(self.payload)),
            )))),
            Tag::Float => Some(ValueRef::Number(NumberRef::Float(Float::from_payload(
                self.payload,
            )))),
            Tag::Double => Some(ValueRef::Number(NumberRef::Double(Double::from_payload(
                self.payload,
            )))),
            Tag::DateTime => Some(ValueRef::DateTime(DateTime::from_payload(self.payload))),
            Tag::Date => Some(ValueRef::Date(Date::from_payload(self.payload))),
            Tag::Time => Some(ValueRef::Time(Time::from_payload(self.payload))),
            Tag::Duration => Some(ValueRef::Duration(Duration::from_payload(self.payload))),
            Tag::Term
            | Tag::WideInt
            | Tag::WideDecimal
            | Tag::WideDateTime
            | Tag::WideDate
            | Tag::WideTime
            | Tag::WideDuration
            | Tag::Tuple
            | Tag::Map => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::value::{DataValue, TimezoneOffset, datatype::xsd};

    #[test]
    fn engine_order_is_tag_major() {
        // Every value of a lower tag sorts before every value of a higher one,
        // whatever the payloads.
        let ordered = [
            StorageValue::term(u64::MAX),
            StorageValue::boolean(true),
            StorageValue::integer(i64::MIN),
            StorageValue::new(Tag::WideInt, 0),
            StorageValue::decimal(decimal::Compact::new(15, -1).expect("value is representable")),
            StorageValue::new(Tag::WideDecimal, 0),
            StorageValue::float(f32::NEG_INFINITY),
            StorageValue::double(f64::NEG_INFINITY),
            StorageValue::datetime(DateTime::from_unix_millis(
                i64::MIN / 8192,
                Some(TimezoneOffset::UTC),
            ))
            .expect("instant is in range"),
            StorageValue::new(Tag::WideDateTime, 0),
            StorageValue::new(Tag::Date, 0),
            StorageValue::new(Tag::WideDate, 0),
            StorageValue::new(Tag::Time, 0),
            StorageValue::new(Tag::WideTime, 0),
            StorageValue::new(Tag::Duration, 0),
            StorageValue::new(Tag::WideDuration, 0),
            StorageValue::new(Tag::Tuple, u64::MAX),
            StorageValue::new(Tag::Map, 0),
        ];

        for window in ordered.windows(2) {
            assert!(window[0] < window[1], "{:?} < {:?}", window[0], window[1]);
        }
    }

    #[test]
    fn inline_values_round_trip() {
        let values = [
            DataValue::boolean(false),
            DataValue::integer(-42),
            DataValue::decimal(15, -1).expect("value is representable"),
            DataValue::float(1.5),
            DataValue::double(f64::NAN),
            DataValue::double(-0.0),
            DataValue::datetime(DateTime::from_unix_millis(1_755_600_000_000, None)),
            DataValue::typed_literal(xsd::DATE, "2024-03-01+05:30").expect("valid"),
            DataValue::typed_literal(xsd::TIME, "12:30:05.25Z").expect("valid"),
            DataValue::typed_literal(xsd::DURATION, "P1Y2M3DT4H").expect("valid"),
        ];

        for value in values {
            let encoded =
                StorageValue::from_value(value.as_ref()).expect("value is inline-representable");
            let decoded = encoded.to_value().expect("tag is inline");
            assert_eq!(decoded.to_owned(), value);
        }
    }

    #[test]
    fn the_wide_cases_are_not_inline() {
        let values = [
            DataValue::decimal_from_lexical("123456789012345678901").expect("well-formed"),
            DataValue::decimal_from_lexical("1.2345678901234567").expect("well-formed"),
            DataValue::decimal(15, -300).expect("value is representable"),
            DataValue::datetime_from_lexical("2024-01-01T00:00:00.1234Z").expect("valid"),
            DataValue::typed_literal(xsd::DATE, "-13798000000-01-01").expect("valid"),
            DataValue::typed_literal(xsd::TIME, "12:30:05.0000000001Z").expect("valid"),
            DataValue::typed_literal(xsd::DAY_TIME_DURATION, "PT0.0001S").expect("valid"),
        ];

        for value in values {
            assert_eq!(StorageValue::from_value(value.as_ref()), None, "{value}");
        }
    }

    #[test]
    fn dictionary_domains_are_not_inline() {
        let values = [
            DataValue::iri("http://example.org/a"),
            DataValue::string("hello"),
            DataValue::null(7),
        ];

        for value in values {
            assert_eq!(StorageValue::from_value(value.as_ref()), None);
        }

        assert_eq!(StorageValue::term(42).to_value(), None);
        assert_eq!(StorageValue::new(Tag::Tuple, 0).to_value(), None);
    }
}
