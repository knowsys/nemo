//! The datatypes of literals.
//!
//! This module contains the IRIs of the datatypes this crate supports,
//! and the parsing of typed literals into the values they denote.

use std::str::FromStr;

use crate::value::{DataValue, Date, DateTime, Double, Duration, Float, NumberRef, Time, ValueRef};

/// Datatype IRIs in the [XML Schema](https://www.w3.org/TR/xmlschema11-2/) namespace.
pub(crate) mod xsd {
    /// The full IRI of a datatype in the XSD namespace.
    macro_rules! iri {
        ($local:literal) => {
            concat!("http://www.w3.org/2001/XMLSchema#", $local)
        };
    }

    /// The namespace itself.
    pub(crate) const NAMESPACE: &str = iri!("");
    /// [`xsd:string`](https://www.w3.org/TR/xmlschema11-2/#string).
    pub(crate) const STRING: &str = iri!("string");
    /// [`xsd:boolean`](https://www.w3.org/TR/xmlschema11-2/#boolean).
    pub(crate) const BOOLEAN: &str = iri!("boolean");
    /// [`xsd:integer`](https://www.w3.org/TR/xmlschema11-2/#integer).
    pub(crate) const INTEGER: &str = iri!("integer");
    /// [`xsd:decimal`](https://www.w3.org/TR/xmlschema11-2/#decimal).
    pub(crate) const DECIMAL: &str = iri!("decimal");
    /// [`xsd:float`](https://www.w3.org/TR/xmlschema11-2/#float).
    pub(crate) const FLOAT: &str = iri!("float");
    /// [`xsd:double`](https://www.w3.org/TR/xmlschema11-2/#double).
    pub(crate) const DOUBLE: &str = iri!("double");
    /// [`xsd:dateTime`](https://www.w3.org/TR/xmlschema11-2/#dateTime).
    pub(crate) const DATE_TIME: &str = iri!("dateTime");
    /// [`xsd:date`](https://www.w3.org/TR/xmlschema11-2/#date).
    pub(crate) const DATE: &str = iri!("date");
    /// [`xsd:time`](https://www.w3.org/TR/xmlschema11-2/#time).
    pub(crate) const TIME: &str = iri!("time");
    /// [`xsd:duration`](https://www.w3.org/TR/xmlschema11-2/#duration).
    pub(crate) const DURATION: &str = iri!("duration");
    /// [`xsd:dayTimeDuration`](https://www.w3.org/TR/xmlschema11-2/#dayTimeDuration).
    pub(crate) const DAY_TIME_DURATION: &str = iri!("dayTimeDuration");
    /// [`xsd:yearMonthDuration`](https://www.w3.org/TR/xmlschema11-2/#yearMonthDuration).
    pub(crate) const YEAR_MONTH_DURATION: &str = iri!("yearMonthDuration");
}

/// Datatype IRIs in the [RDF](https://www.w3.org/TR/rdf11-concepts/) namespace.
pub(crate) mod rdf {
    /// [`rdf:langString`](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-langstring).
    pub(crate) const LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";
}

/// A datatype IRI, classified by how this crate represents its literals.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Datatype {
    /// An XSD datatype whose lexical forms parse into native values.
    Native(NativeDatatype),
    /// `rdf:langString`, whose literals are language-tagged strings.
    LangString,
    /// A datatype this crate has no special support for;
    /// its literals keep their lexical form and IRI.
    Other,
}

impl Datatype {
    /// Classifies a datatype IRI.
    pub(crate) fn of(datatype: &str) -> Self {
        if datatype == rdf::LANG_STRING {
            Self::LangString
        } else {
            NativeDatatype::of(datatype).map_or(Self::Other, Self::Native)
        }
    }

    /// Returns whether this crate has no dedicated value for literals of the datatype.
    pub(crate) fn is_other(self) -> bool {
        matches!(self, Self::Other)
    }
}

/// The XSD datatypes whose literals parse into native values.
#[derive(Debug, Clone, Copy)]
pub(crate) enum NativeDatatype {
    /// `xsd:string`.
    String,
    /// `xsd:boolean`.
    Boolean,
    /// `xsd:decimal`.
    Decimal,
    /// `xsd:integer`.
    Integer,
    /// `xsd:float`.
    Float,
    /// `xsd:double`.
    Double,
    /// A type derived from `xsd:integer` by a bound on its values:
    /// the fixed-width types and the sign-constrained ones.
    Bounded(IntegerBound),
    /// `xsd:dateTime`.
    DateTime,
    /// [`xsd:dateTimeStamp`](https://www.w3.org/TR/xmlschema11-2/#dateTimeStamp),
    /// a dateTime that must carry an offset.
    DateTimeStamp,
    /// `xsd:date`.
    Date,
    /// `xsd:time`.
    Time,
    /// `xsd:duration`.
    Duration,
    /// `xsd:dayTimeDuration`, a duration whose lexical has no years or months.
    DayTimeDuration,
    /// `xsd:yearMonthDuration`, a duration whose lexical has no days or time.
    YearMonthDuration,
}

/// A numeric type
/// [derived from `xsd:integer`](https://www.w3.org/TR/xmlschema11-2/#built-in-derived).
#[derive(Debug, Clone, Copy)]
pub(crate) enum IntegerBound {
    /// [`xsd:long`](https://www.w3.org/TR/xmlschema11-2/#long), the 64-bit range.
    Long,
    /// [`xsd:int`](https://www.w3.org/TR/xmlschema11-2/#int), the 32-bit range.
    Int,
    /// [`xsd:short`](https://www.w3.org/TR/xmlschema11-2/#short), the 16-bit range.
    Short,
    /// [`xsd:byte`](https://www.w3.org/TR/xmlschema11-2/#byte), the 8-bit range.
    Byte,
    /// [`xsd:unsignedLong`](https://www.w3.org/TR/xmlschema11-2/#unsignedLong), the unsigned 64-bit range.
    UnsignedLong,
    /// [`xsd:unsignedInt`](https://www.w3.org/TR/xmlschema11-2/#unsignedInt), the unsigned 32-bit range.
    UnsignedInt,
    /// [`xsd:unsignedShort`](https://www.w3.org/TR/xmlschema11-2/#unsignedShort), the unsigned 16-bit range.
    UnsignedShort,
    /// [`xsd:unsignedByte`](https://www.w3.org/TR/xmlschema11-2/#unsignedByte), the unsigned 8-bit range.
    UnsignedByte,
    /// [`xsd:nonNegativeInteger`](https://www.w3.org/TR/xmlschema11-2/#nonNegativeInteger).
    NonNegative,
    /// [`xsd:positiveInteger`](https://www.w3.org/TR/xmlschema11-2/#positiveInteger).
    Positive,
    /// [`xsd:nonPositiveInteger`](https://www.w3.org/TR/xmlschema11-2/#nonPositiveInteger).
    NonPositive,
    /// [`xsd:negativeInteger`](https://www.w3.org/TR/xmlschema11-2/#negativeInteger).
    Negative,
}

impl NativeDatatype {
    /// Returns the native datatype with the given IRI, if it is one.
    fn of(datatype: &str) -> Option<Self> {
        Some(match datatype.strip_prefix(xsd::NAMESPACE)? {
            "string" => Self::String,
            "boolean" => Self::Boolean,
            "decimal" => Self::Decimal,
            "integer" => Self::Integer,
            "float" => Self::Float,
            "double" => Self::Double,
            "long" => Self::Bounded(IntegerBound::Long),
            "int" => Self::Bounded(IntegerBound::Int),
            "short" => Self::Bounded(IntegerBound::Short),
            "byte" => Self::Bounded(IntegerBound::Byte),
            "unsignedLong" => Self::Bounded(IntegerBound::UnsignedLong),
            "unsignedInt" => Self::Bounded(IntegerBound::UnsignedInt),
            "unsignedShort" => Self::Bounded(IntegerBound::UnsignedShort),
            "unsignedByte" => Self::Bounded(IntegerBound::UnsignedByte),
            "nonNegativeInteger" => Self::Bounded(IntegerBound::NonNegative),
            "positiveInteger" => Self::Bounded(IntegerBound::Positive),
            "nonPositiveInteger" => Self::Bounded(IntegerBound::NonPositive),
            "negativeInteger" => Self::Bounded(IntegerBound::Negative),
            "dateTime" => Self::DateTime,
            "dateTimeStamp" => Self::DateTimeStamp,
            "date" => Self::Date,
            "time" => Self::Time,
            "duration" => Self::Duration,
            "dayTimeDuration" => Self::DayTimeDuration,
            "yearMonthDuration" => Self::YearMonthDuration,
            _ => return None,
        })
    }

    /// Returns the value a lexical form of this datatype denotes,
    /// or `None` for a lexical the datatype does not accept.
    ///
    /// - An `xsd:integer` lexical carries no point.
    /// - An `xsd:decimal` lexical parses exactly.
    /// - Floats take XSD's `INF` and `NaN` spellings.
    /// - A derived integer type's lexical must lie within its bound.
    /// - `dateTimeStamp` folds into `dateTime`.
    /// - The three duration types fold into one value,
    ///   and the derived ones hold to their restricted lexical forms.
    pub(crate) fn parse(self, lexical: &str) -> Option<DataValue> {
        match self {
            Self::String => Some(DataValue::string(lexical)),
            Self::Boolean => match lexical {
                "true" | "1" => Some(DataValue::boolean(true)),
                "false" | "0" => Some(DataValue::boolean(false)),
                _ => None,
            },
            Self::Decimal => DataValue::decimal_from_lexical(lexical),
            Self::Integer => DataValue::integer_from_lexical(lexical),
            Self::Float => Float::from_lexical(lexical).map(DataValue::float),
            Self::Double => Double::from_lexical(lexical).map(DataValue::double),
            Self::Bounded(bound) => bound.parse(lexical),
            Self::DateTime => DataValue::datetime_from_lexical(lexical),
            Self::DateTimeStamp => DateTime::from_lexical(lexical)
                .filter(|value| value.offset().is_some())
                .map(DataValue::datetime),
            Self::Date => Date::from_lexical(lexical).map(DataValue::date),
            Self::Time => Time::from_lexical(lexical).map(DataValue::time),
            Self::Duration => Duration::from_lexical(lexical).map(DataValue::duration),
            Self::DayTimeDuration => {
                Duration::from_day_time_lexical(lexical).map(DataValue::duration)
            }
            Self::YearMonthDuration => {
                Duration::from_year_month_lexical(lexical).map(DataValue::duration)
            }
        }
    }
}

impl IntegerBound {
    /// Returns the integer a lexical form of the bounded type denotes,
    /// or `None` for malformed input and for a value outside the bound.
    fn parse(self, lexical: &str) -> Option<DataValue> {
        /// Parses the integer of a lexical within a fixed-width range.
        fn fixed<T: FromStr + Into<i64>>(lexical: &str) -> Option<DataValue> {
            lexical
                .parse::<T>()
                .ok()
                .map(|value| DataValue::integer(value.into()))
        }

        /// Parses the integer of a lexical that satisfies the bound.
        fn signed(lexical: &str, admits: impl FnOnce(NumberRef) -> bool) -> Option<DataValue> {
            let value = DataValue::integer_from_lexical(lexical)?;
            let ValueRef::Number(integer) = value.as_ref() else {
                unreachable!("an integer lexical denotes an integer");
            };
            admits(integer).then_some(value)
        }

        match self {
            Self::Long => fixed::<i64>(lexical),
            Self::Int => fixed::<i32>(lexical),
            Self::Short => fixed::<i16>(lexical),
            Self::Byte => fixed::<i8>(lexical),
            Self::UnsignedLong => {
                let value = lexical.parse::<u64>().ok()?;
                match i64::try_from(value) {
                    Ok(value) => Some(DataValue::integer(value)),
                    Err(_) => DataValue::integer_from_lexical(lexical),
                }
            }
            Self::UnsignedInt => fixed::<u32>(lexical),
            Self::UnsignedShort => fixed::<u16>(lexical),
            Self::UnsignedByte => fixed::<u8>(lexical),
            Self::NonNegative => signed(lexical, |n| !n.is_negative()),
            Self::Positive => signed(lexical, |n| !n.is_negative() && !n.is_zero()),
            Self::NonPositive => signed(lexical, |n| n.is_negative() || n.is_zero()),
            Self::Negative => signed(lexical, |n| n.is_negative()),
        }
    }
}
