//! Owned data values, which their constructors bring into canonical form.

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

/// An owned data value, such as an IRI, a string, a number, a dateTime or a tuple.
///
/// Its borrowed view is [ValueRef].
/// The constructors bring each value into canonical form, so that it has one representation.
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
    /// A number.
    Number(Number),
    /// A dateTime whose instant is a whole number of milliseconds within the 64-bit range.
    DateTime {
        /// The milliseconds since the Unix epoch, UTC.
        millis: i64,
        /// The timezone offset that the value was written with.
        offset: Option<TimezoneOffset>,
    },
    /// A dateTime whose instant has a fraction finer than a millisecond,
    /// or lies beyond the 64-bit range of milliseconds.
    WideDateTime(Arc<DateTime>),
    /// A date, stored as the instant at which its day starts.
    Date {
        /// The minutes since the Unix epoch, UTC, at which the day starts.
        minutes: i64,
        /// The timezone offset that the value was written with.
        offset: Option<TimezoneOffset>,
    },
    /// A time whose instant is a whole number of nanoseconds.
    Time {
        /// The nanoseconds since the start of the reference day, UTC.
        nanos: i64,
        /// The timezone offset that the value was written with.
        offset: Option<TimezoneOffset>,
    },
    /// A time whose instant has a fraction finer than a nanosecond.
    WideTime(Arc<Time>),
    /// A duration whose seconds are a whole number of milliseconds within the 64-bit range.
    Duration {
        /// The months.
        months: i64,
        /// The seconds, in milliseconds.
        millis: i64,
    },
    /// A duration whose seconds have a fraction finer than a millisecond,
    /// or lie beyond the 64-bit range of milliseconds.
    WideDuration(Arc<Duration>),
    /// A labeled null, identified by its id.
    Null(u64),
    /// A tuple.
    Tuple(Arc<TupleValue>),
    /// A map.
    Map(Arc<MapValue>),
    /// A literal of a datatype without native support.
    Other(OtherLiteral),
}

/// Checks at compile time that an owned value occupies at most 24 bytes.
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

    /// Creates a language-tagged string with the tag in lowercase,
    /// or `None` for a tag that is not [well-formed](LanguageString).
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

    /// Creates the exact number `mantissa * 10^exponent`,
    /// or `None` for a value beyond the [length limit](Decimal) of numbers.
    ///
    /// An integral value becomes an integer, and any other value a decimal.
    pub fn decimal(mantissa: i128, exponent: i64) -> Option<Self> {
        Decimal::new(mantissa, exponent).map(Self::from)
    }

    /// Creates a number from an `xsd:decimal` lexical form,
    /// or `None` for a malformed lexical form or a value beyond the [length limit](Decimal).
    ///
    /// Every `xsd:integer` lexical form is also an `xsd:decimal` lexical form.
    /// An integral value becomes an integer, as for [decimal](Self::decimal).
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

    /// Creates a number from a number literal of term syntax, or `None` for any other text.
    ///
    /// [Number::from_term_syntax] lists the accepted forms.
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

    /// Creates an integer from an `xsd:integer` lexical form, which is a sequence of digits
    /// with an optional sign, or `None` for a malformed lexical form or a value beyond the
    /// [length limit](Decimal).
    pub(crate) fn integer_from_lexical(lexical: &str) -> Option<Self> {
        if lexical.contains('.') {
            return None;
        }
        Self::decimal_from_lexical(lexical)
    }

    /// Creates a 32-bit floating-point value, with every NaN replaced by the one canonical NaN.
    pub fn float(value: impl Into<Float>) -> Self {
        Self(Repr::Number(Number::Float(value.into())))
    }

    /// Creates a 64-bit floating-point value, with every NaN replaced by the one canonical NaN.
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
    /// or `None` for a lexical form that [DateTime::from_lexical] rejects.
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

    /// Creates a duration value in the domain that [Duration::domain] returns for it.
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
    /// Nulls with the same id are the same value.
    /// [fresh_null](crate::dictionary::Dictionary::fresh_null) returns an id for a new null.
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
    /// A literal of a supported XSD datatype becomes the value it denotes, such as a number,
    /// so that a literal and its value are the same term.
    /// A literal of any other datatype keeps its lexical form, as an [other](Self::other) value.
    ///
    /// Returns `None` for a lexical form that the datatype does not accept,
    /// and for `rdf:langString`, which has no value without a language tag.
    pub fn typed_literal(datatype: &str, lexical: &str) -> Option<Self> {
        match Datatype::of(datatype) {
            Datatype::Native(native) => native.parse(lexical),
            Datatype::LangString => None,
            Datatype::Other => Self::other(datatype, lexical),
        }
    }

    /// Creates a literal of a datatype without native support,
    /// which keeps its lexical form and datatype IRI.
    ///
    /// Returns `None` for a datatype with dedicated support,
    /// such as the datatypes that [typed_literal](Self::typed_literal) parses.
    pub fn other(datatype: impl Into<Box<str>>, lexical: impl Into<Box<str>>) -> Option<Self> {
        OtherLiteral::new(datatype, lexical).map(Self::from)
    }

    /// Returns the borrowed view of this value, on which code matches to inspect its content.
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

    /// Returns the domain of this value.
    pub fn domain(&self) -> ValueDomain {
        self.as_ref().domain()
    }

    /// Returns the datatype IRI of this value,
    /// or `None` for a value that is not a literal, such as an IRI, a null or a tuple.
    pub fn datatype_iri(&self) -> Option<&str> {
        self.as_ref().datatype_iri()
    }

    /// Returns an owned copy of a borrowed view.
    ///
    /// The view must be canonical, as every view of a constructed value is.
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
    /// Converts a workspace value, which is within the length limit, into its canonical form.
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
    /// Writes the value in canonical term syntax, as its [view](Self::as_ref) does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl DataValue {
    /// Returns whether two values are equal by value, as [ValueRef::value_eq] does.
    pub fn value_eq(&self, other: &Self) -> Result<bool, EvaluationError> {
        self.as_ref().value_eq(other.as_ref())
    }

    /// Returns the order of two values by value, as [ValueRef::value_cmp] does.
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

    /// The operators and comparisons of owned values give the results of their views.
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

    /// Every NaN becomes the same value, while the two zeros stay distinct.
    #[test]
    fn floats_canonicalize_nans() {
        assert_eq!(DataValue::double(f64::NAN), DataValue::double(-f64::NAN));
        assert_eq!(
            DataValue::float(f32::NAN),
            DataValue::float(f32::from_bits(0x7F80_1234))
        );
        assert_ne!(DataValue::double(-0.0), DataValue::double(0.0));
    }

    /// Float and double literals accept only XSD's lexical forms.
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

        // A special value is written as a typed literal that is parsed back as the same value.
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

    /// A literal of a supported datatype becomes its value,
    /// and a literal of another datatype becomes an other value.
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

    /// A literal of a bounded integer datatype becomes an integer
    /// if its value lies within the bounds.
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

        // A value outside the bounds is not a value of the datatype.
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

        // Beyond the 64-bit range, an integer is stored wide, and the bounds still apply.
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

    /// An `xsd:dateTimeStamp` becomes a dateTime only if it has a timezone offset.
    #[test]
    fn datetime_literals_fold_and_validate() {
        assert_eq!(
            typed("dateTimeStamp", "2024-01-01T00:00:00Z"),
            typed("dateTime", "2024-01-01T00:00:00Z")
        );
        assert_eq!(typed("dateTimeStamp", "2024-01-01T00:00:00"), None);
        assert_eq!(typed("dateTime", "2024-01-01"), None);

        // Fractions finer than a millisecond and distant years are values,
        // not malformed lexical forms.
        assert!(typed("dateTime", "2024-01-01T00:00:00.1234Z").is_some());
        assert!(typed("dateTime", "-13798000000-01-01T00:00:00Z").is_some());
    }

    /// Date and time literals become values only if they are valid,
    /// and `24:00:00` is the start of the next day.
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

    /// A duration literal becomes a value of the most specific duration domain.
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

        // The derived datatypes accept only their restricted lexical forms.
        assert_eq!(typed("dayTimeDuration", "P1Y"), None);
        assert_eq!(typed("dayTimeDuration", "P1M"), None);
        assert_eq!(typed("yearMonthDuration", "P1D"), None);
        assert_eq!(typed("yearMonthDuration", "PT1H"), None);
        assert_eq!(typed("duration", "P1S"), None);

        let rendered = typed("duration", "P1D").expect("valid").to_string();
        assert_eq!(rendered, format!("\"P1D\"^^<{}>", xsd::DAY_TIME_DURATION));
    }

    /// A value of every kind of representation is unchanged by a round trip through its view.
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

    /// An owned value and its view hash alike.
    #[test]
    fn owned_and_borrowed_hashing_agree() {
        let state = RandomState::new();
        let value = DataValue::iri("http://example.org/a");
        assert_eq!(state.hash_one(&value), state.hash_one(value.as_ref()));
    }
}
