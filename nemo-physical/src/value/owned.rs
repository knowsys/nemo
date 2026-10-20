//! Owned data values with canonicalizing construction.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::{Add, Div, Mul, Neg, Rem, Sub},
    sync::Arc,
};

use crate::value::{
    Date, DateTime, Decimal, Double, Duration, EvaluationError, Float, Iri, LanguageString,
    MapValue, OtherLiteral, Time, TimezoneOffset, TupleValue, ValueDomain, ValueRef,
    datatype::Datatype,
    number::{Number, decimal::workspace},
};

/// An owned data value, e.g., an IRI, a string, a number, a dateTime, a structured value,
/// or a value of any other [domain](ValueDomain).
/// Its borrowed view is [ValueRef].
#[derive(Clone)]
pub struct DataValue(Repr);

/// The representation of a [DataValue].
#[derive(Clone)]
enum Repr {
    /// An IRI.
    Iri(Iri),
    /// A plain string.
    String(Arc<str>),
    /// A language-tagged string.
    LanguageString(LanguageString),
    /// A boolean.
    Boolean(bool),
    /// A number, in the case that holds it.
    Number(Number),
    /// A dateTime at millisecond granularity:
    /// milliseconds since the Unix epoch, UTC, and the offset it was written with.
    DateTime {
        /// Milliseconds since the Unix epoch, UTC.
        millis: i64,
        /// The timezone offset the value was written with.
        offset: Option<TimezoneOffset>,
    },
    /// A dateTime finer than a millisecond or beyond the 64-bit range of milliseconds.
    WideDateTime(Arc<DateTime>),
    /// A date: the minutes since the Unix epoch, UTC, at which its day starts,
    /// and the offset it was written with.
    Date {
        /// Minutes since the Unix epoch, UTC.
        minutes: i64,
        /// The timezone offset the value was written with.
        offset: Option<TimezoneOffset>,
    },
    /// A time at nanosecond granularity:
    /// nanoseconds since the start of the reference day, UTC,
    /// and the offset it was written with.
    Time {
        /// Nanoseconds since the start of the reference day, UTC.
        nanos: i64,
        /// The timezone offset the value was written with.
        offset: Option<TimezoneOffset>,
    },
    /// A time finer than a nanosecond.
    WideTime(Arc<Time>),
    /// A duration whose seconds are whole milliseconds within the 64-bit range.
    Duration {
        /// The months.
        months: i64,
        /// The seconds, in milliseconds.
        millis: i64,
    },
    /// A duration whose seconds are finer than a millisecond or beyond the 64-bit range.
    WideDuration(Arc<Duration>),
    /// A labeled null, identified by its id.
    Null(u64),
    /// A tuple.
    Tuple(Arc<TupleValue>),
    /// A map.
    Map(Arc<MapValue>),
    /// A literal of an unsupported datatype.
    Other(OtherLiteral),
}

/// Compile-time check of the size target for owned values.
const _: () = assert!(size_of::<DataValue>() <= 24);

impl DataValue {
    /// Creates an IRI value.
    pub fn iri(iri: impl Into<Iri>) -> Self {
        Self(Repr::Iri(iri.into()))
    }

    /// Creates a plain string value.
    pub fn string(string: impl Into<Arc<str>>) -> Self {
        Self(Repr::String(string.into()))
    }

    /// Creates a language-tagged string, lowercasing the tag,
    /// or `None` for a tag that is not well-formed (see [LanguageString::new]).
    pub fn language_string(text: impl Into<Box<str>>, language: &str) -> Option<Self> {
        LanguageString::new(text, language).map(Self::from)
    }

    /// Creates a boolean value.
    pub fn boolean(value: bool) -> Self {
        Self(Repr::Boolean(value))
    }

    /// Creates an integer value.
    pub fn integer(value: i64) -> Self {
        Self::from(Decimal::from(value))
    }

    /// Creates the exact number `mantissa * 10^exponent`, an `xsd:decimal` value,
    /// or `None` for a value whose canonical form exceeds the length limit of numbers.
    ///
    /// The value takes its canonical form:
    /// an integral value is an integer, and any other a decimal.
    pub fn decimal(mantissa: i128, exponent: i64) -> Option<Self> {
        Decimal::new(mantissa, exponent).map(Self::from)
    }

    /// Creates a number from an `xsd:decimal` lexical form,
    /// which includes every `xsd:integer` lexical form,
    /// or `None` for malformed input
    /// and for a value whose canonical form exceeds the length limit of numbers.
    ///
    /// The value takes its canonical form like [decimal](Self::decimal).
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::{DataValue, ValueDomain};
    ///
    /// let number = |lexical| DataValue::decimal_from_lexical(lexical).unwrap();
    /// assert_eq!(number("-0.0"), DataValue::integer(0));
    /// assert_eq!(number("42."), DataValue::integer(42));
    /// assert_eq!(number("1.5").domain(), ValueDomain::Decimal);
    /// assert_eq!(number("123456789012345678901").domain(), ValueDomain::Integer);
    /// assert_eq!(number("123456789012345678901").to_string(), "123456789012345678901");
    /// assert_eq!(DataValue::decimal_from_lexical("1e5"), None);
    /// ```
    pub fn decimal_from_lexical(lexical: &str) -> Option<Self> {
        Decimal::from_lexical(lexical).map(Self::from)
    }

    /// Creates a number from a number literal of Nemo term syntax,
    /// or `None` for any other lexical
    /// (see [Number::from_term_syntax]).
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::DataValue;
    ///
    /// let number = |lexical| DataValue::number_from_term_syntax(lexical).unwrap();
    /// assert_eq!(number("+7"), DataValue::integer(7));
    /// assert_eq!(number("1.50"), DataValue::decimal(15, -1).unwrap());
    /// assert_eq!(number("-0x1F"), DataValue::integer(-31));
    /// assert_eq!(number("1E3"), DataValue::double(1000.0));
    /// assert_eq!(number("2.5f"), DataValue::float(2.5));
    /// assert_eq!(number("2d"), DataValue::double(2.0));
    /// assert_eq!(DataValue::number_from_term_syntax("5."), None);
    /// assert_eq!(DataValue::number_from_term_syntax("INF"), None);
    /// ```
    pub fn number_from_term_syntax(lexical: &str) -> Option<Self> {
        Number::from_term_syntax(lexical).map(Self::from)
    }

    /// Creates an integer value from an `xsd:integer` lexical form,
    /// a signed sequence of digits,
    /// or `None` for malformed input
    /// and for a value whose canonical form exceeds the length limit of numbers.
    pub(crate) fn integer_from_lexical(lexical: &str) -> Option<Self> {
        if lexical.contains('.') {
            return None;
        }
        Self::decimal_from_lexical(lexical)
    }

    /// Creates a 32-bit float value, turning every NaN into the canonical NaN.
    pub fn float(value: impl Into<Float>) -> Self {
        Self(Repr::Number(Number::Float(value.into())))
    }

    /// Creates a 64-bit float value, turning every NaN into the canonical NaN.
    pub fn double(value: impl Into<Double>) -> Self {
        Self(Repr::Number(Number::Double(value.into())))
    }

    /// Creates a dateTime value.
    pub fn datetime(value: DateTime) -> Self {
        match value.unix_millis() {
            Some(millis) => Self(Repr::DateTime {
                millis,
                offset: value.offset(),
            }),
            None => Self(Repr::WideDateTime(Arc::new(value))),
        }
    }

    /// Creates a dateTime value from an `xsd:dateTime` lexical form,
    /// or `None` for malformed input.
    pub fn datetime_from_lexical(lexical: &str) -> Option<Self> {
        DateTime::from_lexical(lexical).map(Self::datetime)
    }

    /// Creates a date value.
    pub fn date(value: Date) -> Self {
        Self(Repr::Date {
            minutes: value.unix_minutes(),
            offset: value.offset(),
        })
    }

    /// Creates a time value.
    pub fn time(value: Time) -> Self {
        match value.nanos() {
            Some(nanos) => Self(Repr::Time {
                nanos,
                offset: value.offset(),
            }),
            None => Self(Repr::WideTime(Arc::new(value))),
        }
    }

    /// Creates a duration value,
    /// of the most specific of the three duration domains (see [Duration::domain]).
    pub fn duration(value: Duration) -> Self {
        match value.millis() {
            Some(millis) => Self(Repr::Duration {
                months: value.total_months(),
                millis,
            }),
            None => Self(Repr::WideDuration(Arc::new(value))),
        }
    }

    /// Creates a labeled null with the given id.
    ///
    /// Ids identify nulls, and the dictionary mints fresh ones.
    pub fn null(id: u64) -> Self {
        Self(Repr::Null(id))
    }

    /// Creates a tuple value.
    pub fn tuple(tuple: TupleValue) -> Self {
        Self(Repr::Tuple(Arc::new(tuple)))
    }

    /// Creates a map value.
    pub fn map(map: MapValue) -> Self {
        Self(Repr::Map(Arc::new(map)))
    }

    /// Creates the value of a typed literal from its datatype IRI and lexical form.
    ///
    /// The supported XSD datatypes parse into their native representations,
    /// so that term identity never splits between a literal and its value.
    /// Any other datatype keeps its lexical form as an [other](Self::other) value.
    ///
    /// Returns `None` for a lexical the datatype does not accept,
    /// and for `rdf:langString`, which has no value without a tag.
    pub fn typed_literal(datatype: &str, lexical: &str) -> Option<Self> {
        match Datatype::of(datatype) {
            Datatype::Native(native) => native.parse(lexical),
            Datatype::LangString => None,
            Datatype::Other => Self::other(datatype, lexical),
        }
    }

    /// Creates a literal of a datatype this crate has no special support for,
    /// kept as lexical form plus datatype IRI.
    ///
    /// Returns `None` for the datatypes [typed_literal](Self::typed_literal) parses
    /// (see [OtherLiteral::new]).
    pub fn other(datatype: impl Into<Box<str>>, lexical: impl Into<Box<str>>) -> Option<Self> {
        OtherLiteral::new(datatype, lexical).map(Self::from)
    }

    /// Returns the borrowed view of this value,
    /// the surface for matching on its content and the input to builtin evaluation.
    pub fn as_ref(&self) -> ValueRef<'_> {
        match &self.0 {
            Repr::Iri(iri) => ValueRef::Iri(iri.as_ref()),
            Repr::String(string) => ValueRef::String(string),
            Repr::LanguageString(value) => ValueRef::LanguageString(value.as_ref()),
            Repr::Boolean(value) => ValueRef::Boolean(*value),
            Repr::Number(value) => ValueRef::Number(value.as_ref()),
            Repr::DateTime { millis, offset } => {
                ValueRef::DateTime(DateTime::from_unix_millis(*millis, *offset))
            }
            Repr::WideDateTime(value) => ValueRef::DateTime(**value),
            Repr::Date { minutes, offset } => {
                ValueRef::Date(Date::from_unix_minutes(*minutes, *offset))
            }
            Repr::Time { nanos, offset } => ValueRef::Time(Time::from_nanos(*nanos, *offset)),
            Repr::WideTime(value) => ValueRef::Time(**value),
            Repr::Duration { months, millis } => {
                ValueRef::Duration(Duration::from_millis(*months, *millis))
            }
            Repr::WideDuration(value) => ValueRef::Duration(**value),
            Repr::Null(id) => ValueRef::Null(*id),
            Repr::Tuple(tuple) => ValueRef::Tuple(TupleValue::as_ref(tuple)),
            Repr::Map(map) => ValueRef::Map(MapValue::as_ref(map)),
            Repr::Other(value) => ValueRef::Other(value.as_ref()),
        }
    }

    /// Returns the semantic domain of this value.
    pub fn domain(&self) -> ValueDomain {
        self.as_ref().domain()
    }

    /// Returns the datatype IRI of this value,
    /// or `None` for the non-literal domains (see [ValueDomain::datatype_iri]).
    pub fn datatype_iri(&self) -> Option<&str> {
        self.as_ref().datatype_iri()
    }

    /// Clones a borrowed view into an owned value.
    ///
    /// The view is taken to be canonical, as every view from a constructed value is.
    pub(crate) fn from_ref(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Iri(iri) => Self::from(iri.to_owned()),
            ValueRef::String(string) => Self::string(string),
            ValueRef::LanguageString(value) => Self::from(value.to_owned()),
            ValueRef::Boolean(value) => Self::boolean(value),
            ValueRef::Number(value) => Self(Repr::Number(value.to_owned())),
            ValueRef::DateTime(value) => Self::datetime(value),
            ValueRef::Date(value) => Self::date(value),
            ValueRef::Time(value) => Self::time(value),
            ValueRef::Duration(value) => Self::duration(value),
            ValueRef::Null(id) => Self::null(id),
            ValueRef::Tuple(tuple) => Self::tuple(tuple.to_owned()),
            ValueRef::Map(map) => Self::map(map.to_owned()),
            ValueRef::Other(value) => Self::from(value.to_owned()),
        }
    }
}

impl From<Iri> for DataValue {
    fn from(iri: Iri) -> Self {
        Self(Repr::Iri(iri))
    }
}

impl From<LanguageString> for DataValue {
    fn from(value: LanguageString) -> Self {
        Self(Repr::LanguageString(value))
    }
}

impl From<OtherLiteral> for DataValue {
    fn from(value: OtherLiteral) -> Self {
        Self(Repr::Other(value))
    }
}

impl From<Number> for DataValue {
    fn from(value: Number) -> Self {
        Self(Repr::Number(value))
    }
}

impl From<Decimal> for DataValue {
    fn from(value: Decimal) -> Self {
        Self(Repr::Number(Number::Decimal(value)))
    }
}

impl From<workspace::Big> for DataValue {
    /// Routes a workspace value, which is bounded already, to its canonical form.
    fn from(value: workspace::Big) -> Self {
        Self::from(Decimal::from(value))
    }
}

impl PartialEq for DataValue {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for DataValue {}

impl Ord for DataValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(&other.as_ref())
    }
}

impl PartialOrd for DataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for DataValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl fmt::Debug for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataValue({:?})", self.as_ref())
    }
}

impl fmt::Display for DataValue {
    /// Writes the value in canonical Nemo term syntax:
    /// IRIs in angle brackets,
    /// strings quoted with `\` and `"` escaped,
    /// floats and doubles with their `f` and `d` suffixes,
    /// or as typed literals when not finite,
    /// dates, times, durations and unsupported datatypes as typed literals,
    /// and structured values recursively.
    ///
    /// One value has one rendering, and distinct values render distinctly.
    /// Every rendering re-parses as a term denoting this value,
    /// except a null, which renders as `_:id` in the syntax of data, not of programs.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl DataValue {
    /// Returns whether two values are equal. See [ValueRef::value_eq].
    pub fn value_eq(&self, other: &Self) -> Result<bool, EvaluationError> {
        self.as_ref().value_eq(other.as_ref())
    }

    /// Returns the order of two values. See [ValueRef::value_cmp].
    pub fn value_cmp(&self, other: &Self) -> Result<Option<Ordering>, EvaluationError> {
        self.as_ref().value_cmp(other.as_ref())
    }
}

/// Forwards a binary operator on owned values to the [view](DataValue::as_ref).
macro_rules! forward_binary {
    ($trait:ident, $method:ident) => {
        impl $trait for &DataValue {
            type Output = Result<DataValue, EvaluationError>;

            /// Forwards to the operator on [ValueRef].
            fn $method(self, other: Self) -> Self::Output {
                self.as_ref().$method(other.as_ref())
            }
        }
    };
}

forward_binary!(Add, add);
forward_binary!(Sub, sub);
forward_binary!(Mul, mul);
forward_binary!(Div, div);
forward_binary!(Rem, rem);

impl Neg for &DataValue {
    type Output = Result<DataValue, EvaluationError>;

    /// Forwards to the [negation](ValueRef::neg) of the view.
    fn neg(self) -> Self::Output {
        -self.as_ref()
    }
}

impl Neg for DataValue {
    type Output = Result<DataValue, EvaluationError>;

    /// Forwards to the [negation](ValueRef::neg) of the view.
    fn neg(self) -> Self::Output {
        -self.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::hash::{BuildHasher, RandomState};

    use crate::value::{
        DecimalRef, NumberRef, TimezoneOffset,
        datatype::{rdf, xsd},
        number::decimal,
    };

    /// Creates the value of a typed literal in the XSD namespace.
    fn typed(datatype: &str, lexical: &str) -> Option<DataValue> {
        DataValue::typed_literal(&format!("{}{datatype}", xsd::NAMESPACE), lexical)
    }

    #[test]
    fn operators_and_comparisons_forward_to_the_view() {
        let (seven, two) = (DataValue::integer(7), DataValue::integer(2));
        let (left, right) = (seven.as_ref(), two.as_ref());

        assert_eq!(&seven + &two, left + right);
        assert_eq!(&seven - &two, left - right);
        assert_eq!(&seven * &two, left * right);
        assert_eq!(&seven / &two, left / right);
        assert_eq!(&seven % &two, left % right);
        assert_eq!(-&seven, -left);
        assert_eq!(-seven.clone(), -left);
        assert_eq!(seven.value_eq(&two), left.value_eq(right));
        assert_eq!(seven.value_cmp(&two), left.value_cmp(right));
    }

    #[test]
    fn floats_canonicalize_nans() {
        assert_eq!(DataValue::double(f64::NAN), DataValue::double(-f64::NAN));
        assert_eq!(
            DataValue::float(f32::NAN),
            DataValue::float(f32::from_bits(0x7F80_1234))
        );
        assert_ne!(DataValue::double(-0.0), DataValue::double(0.0));
    }

    #[test]
    fn float_literals_take_the_xsd_form_only() {
        assert_eq!(typed("float", "INF"), Some(DataValue::float(f32::INFINITY)));
        assert_eq!(
            typed("double", "-INF"),
            Some(DataValue::double(f64::NEG_INFINITY))
        );
        assert_eq!(typed("double", "NaN"), Some(DataValue::double(f64::NAN)));
        assert_eq!(typed("double", "-1.5E3"), Some(DataValue::double(-1500.0)));
        assert_eq!(typed("float", ".5"), Some(DataValue::float(0.5)));
        assert_eq!(typed("float", "5."), Some(DataValue::float(5.0)));

        for lexical in [
            "nan", "inf", "Infinity", "+inf", "1e", "e5", ".", "0x1", " 1", "",
        ] {
            assert_eq!(typed("float", lexical), None, "{lexical:?}");
            assert_eq!(typed("double", lexical), None, "{lexical:?}");
        }

        // The rendering of a special value re-parses as the same term.
        for value in [
            DataValue::double(f64::INFINITY),
            DataValue::double(f64::NEG_INFINITY),
            DataValue::double(f64::NAN),
            DataValue::float(f32::INFINITY),
        ] {
            let rendered = value.to_string();
            let (lexical, datatype) = rendered
                .strip_prefix('"')
                .and_then(|rest| rest.split_once("\"^^<"))
                .expect("a typed literal");
            let datatype = datatype.strip_suffix('>').expect("an IRI");
            assert_eq!(DataValue::typed_literal(datatype, lexical), Some(value));
        }
    }

    #[test]
    fn typed_literals_of_native_datatypes_take_their_values() {
        assert_eq!(typed("string", "hello"), Some(DataValue::string("hello")));
        assert_eq!(typed("boolean", "1"), Some(DataValue::boolean(true)));
        assert_eq!(typed("boolean", "yes"), None);
        assert_eq!(DataValue::typed_literal(rdf::LANG_STRING, "hello"), None);

        let other = DataValue::typed_literal("http://example.org/unit", "5cm");
        assert_eq!(other, DataValue::other("http://example.org/unit", "5cm"));
        assert_eq!(other.map(|value| value.domain()), Some(ValueDomain::Other));
        assert_eq!(DataValue::other(xsd::INTEGER, "42"), None);
    }

    #[test]
    fn derived_integer_types_fold_into_integers_within_their_bounds() {
        for datatype in [
            "long",
            "int",
            "short",
            "byte",
            "unsignedLong",
            "unsignedInt",
            "unsignedShort",
            "unsignedByte",
            "nonNegativeInteger",
            "positiveInteger",
        ] {
            assert_eq!(
                typed(datatype, "42"),
                Some(DataValue::integer(42)),
                "{datatype}"
            );
        }

        for datatype in [
            "nonPositiveInteger",
            "negativeInteger",
            "long",
            "int",
            "short",
            "byte",
        ] {
            assert_eq!(
                typed(datatype, "-42"),
                Some(DataValue::integer(-42)),
                "{datatype}"
            );
        }

        // Outside the bound: not a value of the type.
        assert_eq!(typed("byte", "128"), None);
        assert_eq!(typed("unsignedByte", "-1"), None);
        assert_eq!(typed("positiveInteger", "0"), None);
        assert_eq!(typed("negativeInteger", "0"), None);
        assert_eq!(typed("nonNegativeInteger", "-1"), None);
        assert_eq!(typed("nonPositiveInteger", "1"), None);
        assert_eq!(typed("nonNegativeInteger", "1.0"), None);
        assert_eq!(typed("integer", "1.0"), None);

        // Zero satisfies both non-strict bounds.
        assert_eq!(
            typed("nonNegativeInteger", "0"),
            Some(DataValue::integer(0))
        );
        assert_eq!(
            typed("nonPositiveInteger", "-0"),
            Some(DataValue::integer(0))
        );

        // Beyond 64 bits the value is wide, in the bound or not.
        let wide = typed("unsignedLong", "18446744073709551615").expect("within the bound");
        assert!(matches!(
            wide.as_ref(),
            ValueRef::Number(NumberRef::Decimal(DecimalRef(decimal::ReprRef::Wide(_))))
        ));
        assert_eq!(
            typed("nonNegativeInteger", "123456789012345678901"),
            DataValue::decimal_from_lexical("123456789012345678901")
        );
        assert_eq!(typed("negativeInteger", "123456789012345678901"), None);
        assert_eq!(typed("unsignedLong", "18446744073709551616"), None);
    }

    #[test]
    fn datetime_literals_fold_and_validate() {
        assert_eq!(
            typed("dateTimeStamp", "2024-01-01T00:00:00Z"),
            typed("dateTime", "2024-01-01T00:00:00Z")
        );
        assert_eq!(typed("dateTimeStamp", "2024-01-01T00:00:00"), None);
        assert_eq!(typed("dateTime", "2024-01-01"), None);

        // Sub-millisecond fractions and far years are values, not malformed input.
        assert!(typed("dateTime", "2024-01-01T00:00:00.1234Z").is_some());
        assert!(typed("dateTime", "-13798000000-01-01T00:00:00Z").is_some());
    }

    #[test]
    fn date_and_time_literals_validate() {
        assert_eq!(
            typed("date", "2024-03-01Z").map(|value| value.domain()),
            Some(ValueDomain::Date)
        );
        assert_eq!(
            typed("time", "24:00:00"),
            typed("time", "00:00:00"),
            "the end of the day is the start of the next"
        );
        assert_eq!(typed("date", "2024-02-30"), None);
        assert_eq!(typed("date", "2024-03-01T00:00:00"), None);
        assert_eq!(typed("time", "25:00:00"), None);
    }

    #[test]
    fn duration_literals_fold_into_the_most_specific_domain() {
        let domain = |datatype, lexical| typed(datatype, lexical).map(|value| value.domain());

        assert_eq!(
            domain("duration", "P1D"),
            Some(ValueDomain::DayTimeDuration)
        );
        assert_eq!(typed("duration", "P1D"), typed("dayTimeDuration", "PT24H"));
        assert_eq!(typed("duration", "P1Y"), typed("yearMonthDuration", "P12M"));
        assert_eq!(
            domain("yearMonthDuration", "P0M"),
            Some(ValueDomain::DayTimeDuration),
            "the zero duration is a dayTimeDuration"
        );
        assert_eq!(domain("duration", "P1MT1S"), Some(ValueDomain::Duration));

        // The derived types keep to their restricted lexical forms.
        assert_eq!(typed("dayTimeDuration", "P1Y"), None);
        assert_eq!(typed("dayTimeDuration", "P1M"), None);
        assert_eq!(typed("yearMonthDuration", "P1D"), None);
        assert_eq!(typed("yearMonthDuration", "PT1H"), None);
        assert_eq!(typed("duration", "P1S"), None);

        let rendered = typed("duration", "P1D").expect("valid").to_string();
        assert_eq!(rendered, format!("\"P1D\"^^<{}>", xsd::DAY_TIME_DURATION));
    }

    #[test]
    fn views_round_trip() {
        let values = [
            DataValue::iri("http://example.org/a"),
            DataValue::string("hello"),
            DataValue::language_string("hello", "en").expect("tag is well-formed"),
            DataValue::boolean(true),
            DataValue::integer(-42),
            DataValue::decimal_from_lexical("123456789012345678901")
                .expect("lexical form is well-formed"),
            DataValue::decimal(15, -1).expect("value is representable"),
            DataValue::decimal_from_lexical("123456789012345678901.5")
                .expect("lexical form is well-formed"),
            DataValue::float(1.5),
            DataValue::double(f64::NAN),
            DataValue::datetime(DateTime::from_unix_millis(0, Some(TimezoneOffset::UTC))),
            DataValue::datetime_from_lexical("2024-01-01T00:00:00.1234Z").expect("valid"),
            typed("date", "2024-03-01+05:30").expect("valid"),
            typed("date", "-13798000000-01-01").expect("valid"),
            typed("time", "12:30:05.25Z").expect("valid"),
            typed("time", "12:30:05.0000000001").expect("valid"),
            typed("duration", "-P1Y2M3DT4H").expect("valid"),
            typed("dayTimeDuration", "PT0.0001S").expect("valid"),
            DataValue::null(7),
            DataValue::tuple(TupleValue::labeled("point", [DataValue::integer(1)])),
            DataValue::map(MapValue::new([]).expect("no keys, no duplicates")),
            DataValue::other("http://example.org/unit", "5cm").expect("datatype is unsupported"),
        ];
        for value in values {
            assert_eq!(value.as_ref().to_owned(), value);
        }
    }

    #[test]
    fn owned_and_borrowed_hashing_agree() {
        let state = RandomState::new();
        let value = DataValue::iri("http://example.org/a");
        assert_eq!(state.hash_one(&value), state.hash_one(value.as_ref()));
    }
}
