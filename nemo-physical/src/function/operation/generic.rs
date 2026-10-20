//! Term-level operations: type checks, datatypes, lexical forms,
//! language handling, and casts.
//!
//! A literal is a value of an RDF datatype:
//! any value but an IRI, a null, a tuple or a map.

use std::{fmt, num::IntErrorKind};

use crate::{
    function::StackValue,
    value::{
        DataValue, Date, DateTime, DomainSet, Double, Duration, EvaluationError, Float, Iri,
        NumberRef, Time, ValueDomain, ValueRef, datatype::xsd,
    },
};

/// Returns whether the domain of a value is in the set,
/// the test behind the type checks, for example
/// SPARQL's [`isIRI`](https://www.w3.org/TR/sparql12-query/#func-isIRI),
/// [`isNUMERIC`](https://www.w3.org/TR/sparql12-query/#func-isNumeric)
/// and [`isBLANK`](https://www.w3.org/TR/sparql12-query/#func-isBlank).
pub(crate) fn in_domains(value: ValueRef<'_>, domains: DomainSet) -> StackValue<'static> {
    domains.contains(value.domain()).into()
}

/// Returns the datatype IRI of a literal,
/// SPARQL's [`DATATYPE`](https://www.w3.org/TR/sparql12-query/#func-datatype).
///
/// Any other value has none.
pub(crate) fn datatype(value: ValueRef<'_>) -> StackValue<'static> {
    match value.datatype_iri() {
        Some(datatype) => DataValue::iri(datatype).into(),
        None => EvaluationError::TypeMismatch.into(),
    }
}

/// Returns the lexical form of a value as a plain string,
/// SPARQL's [`STR`](https://www.w3.org/TR/sparql12-query/#func-str).
///
/// A language string yields its text, every other literal its canonical lexical,
/// an IRI its text.
/// Nulls and structures have none.
pub(crate) fn lexical(value: ValueRef<'_>) -> StackValue<'static> {
    let text = match value {
        ValueRef::String(text) => text.to_owned(),
        ValueRef::LanguageString(value) => value.text().to_owned(),
        ValueRef::Iri(iri) => iri.to_string(),
        ValueRef::Boolean(value) => value.to_string(),
        ValueRef::Number(value) => value.to_string(),
        ValueRef::DateTime(value) => value.to_string(),
        ValueRef::Date(value) => value.to_string(),
        ValueRef::Time(value) => value.to_string(),
        ValueRef::Duration(value) => value.to_string(),
        ValueRef::Other(literal) => literal.lexical().to_owned(),
        _ => return EvaluationError::TypeMismatch.into(),
    };
    DataValue::string(text).into()
}

/// Returns the canonical form of a value as a plain string:
/// a string quoted and escaped, a language string with its tag,
/// an IRI and an exact number bare, every other literal with its datatype.
///
/// Not a SPARQL function; the term as a program would write it.
pub(crate) fn canonical(value: ValueRef<'_>) -> StackValue<'static> {
    let text = match value {
        ValueRef::String(text) => quote(text),
        ValueRef::LanguageString(value) => format!("{}@{}", quote(value.text()), value.language()),
        ValueRef::Iri(iri) => iri.to_string(),
        ValueRef::Boolean(value) => {
            format!("{}^^<{}>", quote(&value.to_string()), xsd::BOOLEAN)
        }
        ValueRef::Number(value) => match value.domain() {
            ValueDomain::Integer | ValueDomain::Decimal => value.to_string(),
            ValueDomain::Float => format!("{}^^<{}>", quote(&value.to_string()), xsd::FLOAT),
            _ => format!("{}^^<{}>", quote(&value.to_string()), xsd::DOUBLE),
        },
        ValueRef::Null(_) => format!("_:{}", value.null_number().unwrap_or_default()),
        ValueRef::DateTime(lexical) => typed(value, lexical),
        ValueRef::Date(lexical) => typed(value, lexical),
        ValueRef::Time(lexical) => typed(value, lexical),
        ValueRef::Duration(lexical) => typed(value, lexical),
        ValueRef::Other(literal) => {
            format!("{}^^<{}>", quote(literal.lexical()), literal.datatype())
        }
        _ => return EvaluationError::TypeMismatch.into(),
    };
    DataValue::string(text).into()
}

/// Returns the typed literal of a value of a native datatype:
/// its lexical form, quoted, and the datatype of its domain.
fn typed(value: ValueRef<'_>, lexical: impl fmt::Display) -> String {
    let datatype = value
        .datatype_iri()
        .expect("a value of a native datatype has one");
    format!("{}^^<{datatype}>", quote(&lexical.to_string()))
}

/// Returns a text in double quotes, with the quote, the backslash and the line breaks escaped.
fn quote(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for character in text.chars() {
        match character {
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            other => quoted.push(other),
        }
    }
    quoted.push('"');
    quoted
}

/// Returns the language tag of a literal,
/// SPARQL's [`LANG`](https://www.w3.org/TR/sparql12-query/#func-lang):
/// the tag of a language-tagged string, the empty string for every other literal.
///
/// Any other value has none.
pub(crate) fn language(value: ValueRef<'_>) -> StackValue<'static> {
    match value {
        ValueRef::LanguageString(value) => DataValue::string(value.language()).into(),
        _ if DomainSet::LITERAL.contains(value.domain()) => ValueRef::String("").into(),
        _ => EvaluationError::TypeMismatch.into(),
    }
}

/// Tags a plain string with a language,
/// SPARQL's [`STRLANG`](https://www.w3.org/TR/sparql12-query/#func-strlang);
/// an empty or otherwise invalid tag is malformed.
pub(crate) fn string_language(text: ValueRef<'_>, tag: ValueRef<'_>) -> StackValue<'static> {
    match (text, tag) {
        (ValueRef::String(text), ValueRef::String(tag)) => {
            match DataValue::language_string(text, tag) {
                Some(value) => value.into(),
                None => EvaluationError::Malformed.into(),
            }
        }
        _ => EvaluationError::TypeMismatch.into(),
    }
}

/// Returns whether a language tag matches a range,
/// SPARQL's [`langMatches`](https://www.w3.org/TR/sparql12-query/#func-langMatches),
/// the basic filtering of RFC 4647:
/// `*` matches any nonempty tag,
/// otherwise the range must be a case-insensitive subtag prefix;
/// an empty tag or range matches nothing.
pub(crate) fn language_matches(tag: ValueRef<'_>, range: ValueRef<'_>) -> StackValue<'static> {
    let (ValueRef::String(tag), ValueRef::String(range)) = (tag, range) else {
        return EvaluationError::TypeMismatch.into();
    };

    if tag.is_empty() || range.is_empty() {
        return false.into();
    }
    if range == "*" {
        return true.into();
    }

    let tag = tag.to_ascii_lowercase();
    let range = range.to_ascii_lowercase();
    let matches = tag == range
        || (tag.len() > range.len()
            && tag.starts_with(&range)
            && tag.as_bytes()[range.len()] == b'-');

    matches.into()
}

/// Creates a typed literal from a lexical and a datatype IRI,
/// SPARQL's [`STRDT`](https://www.w3.org/TR/sparql12-query/#func-strdt),
/// normalized like a program literal; see [DataValue::typed_literal].
///
/// A lexical that is not one of the datatype is malformed.
pub(crate) fn typed_literal(lexical: ValueRef<'_>, datatype: ValueRef<'_>) -> StackValue<'static> {
    let ValueRef::String(lexical) = lexical else {
        return EvaluationError::TypeMismatch.into();
    };
    let ValueRef::Iri(iri) = datatype else {
        return EvaluationError::TypeMismatch.into();
    };

    match DataValue::typed_literal(&iri.to_cow(), lexical) {
        Some(value) => value.into(),
        None => EvaluationError::Malformed.into(),
    }
}

/// Casts a value to an integer,
/// XPath's [cast to `xs:integer`](https://www.w3.org/TR/xpath-functions-31/#casting-to-integer).
///
/// A number drops its fraction, toward zero;
/// the NaN is malformed, and an infinity overflows.
/// A boolean counts.
/// A string, or a literal of a datatype without native support,
/// reads as the lexical form of an `xsd:integer`,
/// or as a binary, octal or hexadecimal numeral under a `0b`, `0o` or `0x` prefix.
/// An integer beyond 64 bits stays exact as a wide integer.
pub(crate) fn cast_integer(value: ValueRef<'_>) -> StackValue<'static> {
    match value {
        ValueRef::Number(number) if number.is_integral() => value.to_owned().into(),
        ValueRef::Number(NumberRef::Float(value)) => integral_double(value.to_f64()).into(),
        ValueRef::Number(NumberRef::Double(value)) => integral_double(value.get()).into(),
        // A fractional decimal: the integer on its zero side.
        ValueRef::Number(number) if number.is_negative() => number.ceil().into(),
        ValueRef::Number(number) => number.floor().into(),
        ValueRef::Boolean(value) => DataValue::integer(i64::from(value)).into(),
        _ => lexical_form(value).and_then(integer_from_text).into(),
    }
}

/// Returns the integer a text denotes:
/// the value of an `xsd:integer` lexical form,
/// or of a numeral of at most 127 bits in binary, octal or hexadecimal
/// under a `0b`, `0o` or `0x` prefix.
///
/// A numeral beyond 127 bits overflows; any other text is malformed.
fn integer_from_text(text: &str) -> Result<DataValue, EvaluationError> {
    let radix = match text.get(..2) {
        Some("0b") => 2,
        Some("0o") => 8,
        Some("0x") => 16,
        _ => return DataValue::integer_from_lexical(text).ok_or(EvaluationError::Malformed),
    };

    // A numeral has digits only, where the standard parser would also take a sign.
    let digits = &text[2..];
    if digits.starts_with(['+', '-']) {
        return Err(EvaluationError::Malformed);
    }

    let value = i128::from_str_radix(digits, radix).map_err(|error| match error.kind() {
        IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => EvaluationError::Overflow,
        _ => EvaluationError::Malformed,
    })?;
    DataValue::decimal(value, 0).ok_or(EvaluationError::Overflow)
}

/// Returns the integer term of a double without its fraction, toward zero:
/// the 64-bit integer if it fits, the wide integer beyond,
/// since an integral double prints its digits exactly.
///
/// The NaN is malformed, and an infinity overflows.
fn integral_double(value: f64) -> Result<DataValue, EvaluationError> {
    if value.is_nan() {
        return Err(EvaluationError::Malformed);
    }
    if value.is_infinite() {
        return Err(EvaluationError::Overflow);
    }

    let value = value.trunc();
    // Doubles hold the ends of the 64-bit range exactly,
    // so the check excludes every value the cast would saturate at.
    let bound = -(i64::MIN as f64);
    if (-bound..bound).contains(&value) {
        // The range check leaves the cast nothing to truncate.
        #[allow(clippy::cast_possible_truncation)]
        let integer = value as i64;
        Ok(DataValue::integer(integer))
    } else {
        DataValue::decimal_from_lexical(&format!("{value:.0}")).ok_or(EvaluationError::Overflow)
    }
}

/// Casts a value to a 64-bit float,
/// XPath's [cast to `xs:double`](https://www.w3.org/TR/xpath-functions-31/#casting-to-double):
/// a number to the nearest double, a boolean to one or zero.
///
/// A string, or a literal of a datatype without native support,
/// reads as the lexical form of an `xsd:double`.
pub(crate) fn cast_double(value: ValueRef<'_>) -> StackValue<'static> {
    let value = match value {
        ValueRef::Number(value) => Ok(Double::new(value.to_f64())),
        ValueRef::Boolean(value) => Ok(Double::new(f64::from(u8::from(value)))),
        _ => parse(value, Double::from_lexical),
    };
    value.map(DataValue::double).into()
}

/// Casts a value to a 32-bit float,
/// XPath's [cast to `xs:float`](https://www.w3.org/TR/xpath-functions-31/#casting-to-float),
/// as [cast_double] casts to a double.
pub(crate) fn cast_float(value: ValueRef<'_>) -> StackValue<'static> {
    let value = match value {
        ValueRef::Number(value) => Ok(Float::new(value.to_f32())),
        ValueRef::Boolean(value) => Ok(Float::new(f32::from(u8::from(value)))),
        _ => parse(value, Float::from_lexical),
    };
    value.map(DataValue::float).into()
}

/// Casts a string to an IRI, SPARQL's [`IRI`](https://www.w3.org/TR/sparql12-query/#func-iri);
/// an IRI passes through.
///
/// A string that is not an IRI, as one with a space, is malformed.
/// A relative IRI stays unresolved: the base IRI is not known here.
pub(crate) fn cast_iri(value: ValueRef<'_>) -> StackValue<'static> {
    match value {
        ValueRef::Iri(_) => value.to_owned().into(),
        ValueRef::String(text) if Iri::is_valid(text) => DataValue::iri(text).into(),
        ValueRef::String(_) => EvaluationError::Malformed.into(),
        _ => EvaluationError::TypeMismatch.into(),
    }
}

/// Casts a value to a dateTime,
/// XPath's [cast to `xs:dateTime`](https://www.w3.org/TR/xpath-functions-31/#casting-to-datetimes).
///
/// A date becomes the start of its day, in its own timezone.
/// A string, or a literal of a datatype without native support,
/// reads as the lexical form of an `xsd:dateTime`.
pub(crate) fn cast_datetime(value: ValueRef<'_>) -> StackValue<'static> {
    let datetime = match value {
        ValueRef::DateTime(value) => Ok(value),
        ValueRef::Date(value) => Ok(value.start_of_day()),
        _ => parse(value, DateTime::from_lexical),
    };
    datetime.map(DataValue::datetime).into()
}

/// Casts a value to a date,
/// XPath's [cast to `xs:date`](https://www.w3.org/TR/xpath-functions-31/#casting-to-datetimes).
///
/// A dateTime becomes its day, in its own timezone;
/// one whose day starts beyond the value space overflows.
/// A string, or a literal of a datatype without native support,
/// reads as the lexical form of an `xsd:date`.
pub(crate) fn cast_date(value: ValueRef<'_>) -> StackValue<'static> {
    let date = match value {
        ValueRef::Date(value) => Ok(value),
        ValueRef::DateTime(value) => value.date().ok_or(EvaluationError::Overflow),
        _ => parse(value, Date::from_lexical),
    };
    date.map(DataValue::date).into()
}

/// Casts a value to a time,
/// XPath's [cast to `xs:time`](https://www.w3.org/TR/xpath-functions-31/#casting-to-datetimes).
///
/// A dateTime becomes its time of day, in its own timezone.
/// A string, or a literal of a datatype without native support,
/// reads as the lexical form of an `xsd:time`.
pub(crate) fn cast_time(value: ValueRef<'_>) -> StackValue<'static> {
    let time = match value {
        ValueRef::Time(value) => Ok(value),
        ValueRef::DateTime(value) => Ok(value.time()),
        _ => parse(value, Time::from_lexical),
    };
    time.map(DataValue::time).into()
}

/// Casts a value to a duration,
/// XPath's [cast to `xs:duration`](https://www.w3.org/TR/xpath-functions-31/#casting-to-durations).
///
/// A duration of any of the three duration domains passes through.
/// A string, or a literal of a datatype without native support,
/// reads as the lexical form of an `xsd:duration`.
pub(crate) fn cast_duration(value: ValueRef<'_>) -> StackValue<'static> {
    let duration = match value {
        ValueRef::Duration(value) => Ok(value),
        _ => parse(value, Duration::from_lexical),
    };
    duration.map(DataValue::duration).into()
}

/// Casts a value to a dayTimeDuration,
/// XPath's [cast to `xs:dayTimeDuration`](https://www.w3.org/TR/xpath-functions-31/#casting-to-durations).
///
/// A duration drops its months.
/// A string, or a literal of a datatype without native support,
/// reads as the lexical form of an `xsd:dayTimeDuration`.
pub(crate) fn cast_day_time_duration(value: ValueRef<'_>) -> StackValue<'static> {
    let duration = match value {
        ValueRef::Duration(value) => {
            Duration::new(0, value.total_attoseconds()).ok_or(EvaluationError::Overflow)
        }
        _ => parse(value, Duration::from_day_time_lexical),
    };
    duration.map(DataValue::duration).into()
}

/// Casts a value to a yearMonthDuration,
/// XPath's [cast to `xs:yearMonthDuration`](https://www.w3.org/TR/xpath-functions-31/#casting-to-durations).
///
/// A duration drops its seconds.
/// A string, or a literal of a datatype without native support,
/// reads as the lexical form of an `xsd:yearMonthDuration`.
pub(crate) fn cast_year_month_duration(value: ValueRef<'_>) -> StackValue<'static> {
    let duration = match value {
        ValueRef::Duration(value) => Ok(Duration::from_months(value.total_months())),
        _ => parse(value, Duration::from_year_month_lexical),
    };
    duration.map(DataValue::duration).into()
}

/// Returns the text a cast reads as a lexical form:
/// a string's own, or the lexical form of a literal of a datatype without native support.
///
/// Any other value has none, a [TypeMismatch](EvaluationError::TypeMismatch).
fn lexical_form(value: ValueRef<'_>) -> Result<&str, EvaluationError> {
    match value {
        ValueRef::String(text) => Ok(text),
        ValueRef::Other(literal) => Ok(literal.lexical()),
        _ => Err(EvaluationError::TypeMismatch),
    }
}

/// Reads the lexical form of a value with the parser of a cast's target type;
/// a text the parser refuses is [Malformed](EvaluationError::Malformed).
fn parse<T>(
    value: ValueRef<'_>,
    parser: impl FnOnce(&str) -> Option<T>,
) -> Result<T, EvaluationError> {
    parser(lexical_form(value)?).ok_or(EvaluationError::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        testing::{number, value},
        value::{DecimalRef, MapValue, TupleValue, datatype::rdf, number::decimal},
    };

    #[test]
    fn temporal_values_cast_across_their_types() {
        let typed = |datatype: &str, lexical: &str| {
            DataValue::typed_literal(datatype, lexical).expect(lexical)
        };
        let datetime = typed(xsd::DATE_TIME, "2002-10-10T12:00:00-05:00");
        let duration = typed(xsd::DURATION, "P1Y2M3DT4H");

        assert_eq!(
            value(cast_date(datetime.as_ref())),
            typed(xsd::DATE, "2002-10-10-05:00")
        );
        assert_eq!(
            value(cast_time(datetime.as_ref())),
            typed(xsd::TIME, "12:00:00-05:00")
        );
        assert_eq!(
            value(cast_datetime(typed(xsd::DATE, "2002-10-10-05:00").as_ref())),
            typed(xsd::DATE_TIME, "2002-10-10T00:00:00-05:00")
        );
        assert_eq!(
            value(cast_day_time_duration(duration.as_ref())),
            typed(xsd::DAY_TIME_DURATION, "P3DT4H")
        );
        assert_eq!(
            value(cast_year_month_duration(duration.as_ref())),
            typed(xsd::YEAR_MONTH_DURATION, "P1Y2M")
        );
        assert_eq!(value(cast_duration(duration.as_ref())), duration);

        // Strings read as lexical forms of the target type.
        let string = |text| DataValue::string(text);
        assert_eq!(
            value(cast_date(string("2024-03-01").as_ref())),
            typed(xsd::DATE, "2024-03-01")
        );
        assert_eq!(
            value(cast_duration(string("PT36H").as_ref())),
            typed(xsd::DAY_TIME_DURATION, "P1DT12H")
        );
        assert!(
            cast_date(string("2024-02-30").as_ref())
                .as_result()
                .is_err()
        );
        assert!(
            cast_day_time_duration(string("P1M").as_ref())
                .as_result()
                .is_err()
        );
        assert!(
            cast_time(typed(xsd::DATE, "2024-03-01").as_ref())
                .as_result()
                .is_err()
        );
    }

    #[test]
    fn integral_floats_cast_beyond_64_bits() {
        let double = |value: f64| DataValue::double(value);
        let float = |value: f32| DataValue::float(value);
        assert_eq!(
            value(cast_integer(double(1e3).as_ref())),
            DataValue::integer(1000)
        );
        assert_eq!(
            value(cast_integer(double(1e20).as_ref())),
            number("100000000000000000000")
        );
        assert_eq!(
            value(cast_integer(double(-1e20).as_ref())),
            number("-100000000000000000000")
        );
        assert_eq!(
            value(cast_integer(float(1e20).as_ref())),
            number("100000002004087734272")
        );
        // The ends of the 64-bit range: the integer below, the wide
        // integer from there on.
        assert_eq!(
            value(cast_integer(double(-9_223_372_036_854_775_808.0).as_ref())),
            DataValue::integer(i64::MIN)
        );
        assert_eq!(
            value(cast_integer(double(9_223_372_036_854_775_808.0).as_ref())),
            number("9223372036854775808")
        );
        // A fraction drops toward zero; the NaN is malformed, and an infinity overflows.
        assert_eq!(
            value(cast_integer(double(1.5).as_ref())),
            DataValue::integer(1)
        );
        assert_eq!(
            value(cast_integer(float(-1.5).as_ref())),
            DataValue::integer(-1)
        );
        assert_eq!(
            value(cast_integer(number("-2.5").as_ref())),
            DataValue::integer(-2)
        );
        assert_eq!(
            value(cast_integer(number("123456789012345678901.5").as_ref())),
            number("123456789012345678901")
        );
        assert_eq!(
            cast_integer(double(f64::NAN).as_ref()).as_result(),
            Err(EvaluationError::Malformed)
        );
        assert_eq!(
            cast_integer(double(f64::INFINITY).as_ref()).as_result(),
            Err(EvaluationError::Overflow)
        );
    }

    #[test]
    fn a_cast_tells_an_unreadable_text_from_a_value_without_one() {
        let string = |text| DataValue::string(text);
        let failure = |result: StackValue<'_>| result.as_result().err();

        // A text that is not a lexical form of the target type is malformed.
        assert_eq!(
            failure(cast_integer(string("forty-two").as_ref())),
            Some(EvaluationError::Malformed)
        );
        assert_eq!(
            failure(cast_double(string("abc").as_ref())),
            Some(EvaluationError::Malformed)
        );
        assert_eq!(
            failure(cast_date(string("2024-02-30").as_ref())),
            Some(EvaluationError::Malformed)
        );
        assert_eq!(
            failure(cast_day_time_duration(string("P1M").as_ref())),
            Some(EvaluationError::Malformed)
        );
        // A numeral beyond 127 bits overflows.
        assert_eq!(
            failure(cast_integer(
                string(&format!("0x8{}", "0".repeat(31))).as_ref()
            )),
            Some(EvaluationError::Overflow)
        );

        // A value with no lexical form to read has the wrong type.
        let tagged = DataValue::language_string("1", "en").expect("the tag is valid");
        for value in [tagged, DataValue::iri("http://example.org/a")] {
            assert_eq!(
                failure(cast_integer(value.as_ref())),
                Some(EvaluationError::TypeMismatch),
                "{value}"
            );
        }
    }

    #[test]
    fn invalid_tags_and_lexicals_are_malformed() {
        let string = |text| DataValue::string(text);
        assert_eq!(
            string_language(string("a").as_ref(), string("en us").as_ref()).as_result(),
            Err(EvaluationError::Malformed)
        );
        assert_eq!(
            typed_literal(
                string("abc").as_ref(),
                DataValue::iri(xsd::INTEGER).as_ref()
            )
            .as_result(),
            Err(EvaluationError::Malformed)
        );
    }

    #[test]
    fn datatypes_and_tags_belong_to_literals() {
        let tagged = DataValue::language_string("a", "en").expect("the tag is nonempty");
        let other = DataValue::other("http://example.org/t", "a").expect("not native");
        assert_eq!(value(language(tagged.as_ref())), DataValue::string("en"));
        assert_eq!(
            value(datatype(tagged.as_ref())),
            DataValue::iri(rdf::LANG_STRING)
        );
        assert_eq!(
            value(datatype(other.as_ref())),
            DataValue::iri("http://example.org/t")
        );

        // A literal without a tag has the empty one.
        for untagged in [DataValue::string("a"), DataValue::integer(1), other] {
            assert_eq!(
                value(language(untagged.as_ref())),
                DataValue::string(""),
                "{untagged}"
            );
        }

        for non_literal in [
            DataValue::iri("http://example.org/a"),
            DataValue::null(1),
            DataValue::tuple(TupleValue::labeled("point", [DataValue::integer(1)])),
            DataValue::map(MapValue::new([]).expect("no keys, no duplicates")),
        ] {
            for operation in [datatype, language] {
                assert_eq!(
                    operation(non_literal.as_ref()).as_result(),
                    Err(EvaluationError::TypeMismatch),
                    "{non_literal}"
                );
            }
        }
    }

    #[test]
    fn language_ranges_match_subtag_prefixes() {
        let matches = |tag: &str, range: &str| {
            value(language_matches(
                DataValue::string(tag).as_ref(),
                DataValue::string(range).as_ref(),
            ))
        };
        assert_eq!(matches("en-US", "en"), DataValue::boolean(true));
        assert_eq!(matches("en", "*"), DataValue::boolean(true));
        assert_eq!(matches("", "*"), DataValue::boolean(false));
        assert_eq!(matches("", ""), DataValue::boolean(false));
        assert_eq!(matches("en", ""), DataValue::boolean(false));
    }

    #[test]
    fn datetimes_have_lexical_forms() {
        let datetime = DataValue::datetime_from_lexical("2024-03-01T12:30:05Z").expect("valid");
        assert_eq!(
            value(lexical(datetime.as_ref())),
            DataValue::string("2024-03-01T12:30:05Z")
        );
        assert_eq!(
            value(canonical(datetime.as_ref())),
            DataValue::string(format!("\"2024-03-01T12:30:05Z\"^^<{}>", xsd::DATE_TIME))
        );
    }

    #[test]
    fn booleans_have_canonical_forms() {
        assert_eq!(
            value(canonical(DataValue::boolean(true).as_ref())),
            DataValue::string(format!("\"true\"^^<{}>", xsd::BOOLEAN))
        );
    }

    #[test]
    fn decimals_cast_to_the_nearest_double_and_float() {
        let decimal = |mantissa, exponent| {
            ValueRef::Number(NumberRef::Decimal(DecimalRef(decimal::ReprRef::Compact(
                decimal::Compact::new(mantissa, exponent).expect("representable"),
            ))))
        };
        assert_eq!(value(cast_double(decimal(3, -1))), DataValue::double(0.3));
        assert_eq!(value(cast_double(decimal(7, -1))), DataValue::double(0.7));
        assert_eq!(value(cast_float(decimal(3, -1))), DataValue::float(0.3));

        // Just above the midpoint of two floats, where the nearest double lies:
        // through the double, the tie would go down to the even float.
        assert_eq!(
            value(cast_float(decimal(4_686_748_743_057_251, -15))),
            DataValue::float(4.686_749)
        );
    }

    #[test]
    fn strings_cast_as_literals() {
        let cast = |operation: fn(ValueRef<'_>) -> StackValue<'static>, text: &str| {
            operation(DataValue::string(text).as_ref())
                .as_result()
                .ok()
                .map(ValueRef::to_owned)
        };

        // A string reads as the lexical form of a literal of the target datatype.
        for text in [
            "5",
            "+5",
            "-007",
            "+99999999999999999999",
            "1.0",
            "1e5",
            " 5",
            "",
        ] {
            assert_eq!(
                cast(cast_integer, text),
                DataValue::typed_literal(xsd::INTEGER, text),
                "{text:?}"
            );
        }
        for text in [
            "1.5E3",
            ".5",
            "-INF",
            "NaN",
            "inf",
            "Infinity",
            "nan",
            "1.00000005960464477539062500000001",
        ] {
            assert_eq!(
                cast(cast_double, text),
                DataValue::typed_literal(xsd::DOUBLE, text),
                "{text:?}"
            );
            assert_eq!(
                cast(cast_float, text),
                DataValue::typed_literal(xsd::FLOAT, text),
                "{text:?}"
            );
        }

        // So does a literal of a datatype without native support.
        let year =
            DataValue::other(format!("{}gYear", xsd::NAMESPACE), "2023").expect("not native");
        assert_eq!(value(cast_integer(year.as_ref())), DataValue::integer(2023));

        // Beside the literals, the numerals under a radix prefix, up to 127 bits.
        assert_eq!(cast(cast_integer, "0b101001"), Some(DataValue::integer(41)));
        assert_eq!(cast(cast_integer, "0o52"), Some(DataValue::integer(42)));
        assert_eq!(cast(cast_integer, "0x2B"), Some(DataValue::integer(43)));
        assert_eq!(
            cast(cast_integer, "0x8000000000000000"),
            Some(number("9223372036854775808"))
        );
        assert_eq!(
            cast(cast_integer, &format!("0x7{}", "F".repeat(31))),
            Some(number(&i128::MAX.to_string()))
        );
        assert_eq!(cast(cast_integer, &format!("0x8{}", "0".repeat(31))), None);
        assert_eq!(cast(cast_integer, "0x-5"), None);
        assert_eq!(cast(cast_integer, "0x"), None);
    }
}
