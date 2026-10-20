//! The functions on dates, times and durations:
//! SPARQL's [`YEAR` through `TIMEZONE`](https://www.w3.org/TR/sparql12-query/#func-year),
//! which are XPath's
//! [component functions](https://www.w3.org/TR/xpath-functions-31/#func-year-from-dateTime),
//! and SPARQL 1.2's `ADJUST`.
//!
//! SPARQL 1.1 defines the components on dateTimes only.
//! SPARQL 1.2 and XPath read them from dates and times as well,
//! and XPath from durations; so does this module.
//! Components are read in the value's own timezone,
//! never normalized to UTC, following XPath's component functions.
//! The arithmetic and the comparisons of these values are [ValueRef]'s operators.

use crate::{
    function::StackValue,
    value::{
        DataValue, Date, DateTime, Duration, EvaluationError, Time, TimezoneOffset, ValueRef,
        temporal::{ATTOSECONDS_PER_SECOND, FRACTION_DIGITS},
    },
};

/// The extractable components.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Component {
    /// The year (dateTime, date), or the whole years (duration).
    Year,
    /// The month (dateTime, date), or the months beyond the whole years (duration).
    Month,
    /// The day (dateTime, date), or the whole days (duration).
    Day,
    /// The hours (dateTime, time), or the hours beyond the whole days (duration).
    Hours,
    /// The minutes (dateTime, time), or the minutes beyond the whole hours (duration).
    Minutes,
    /// The seconds, fractional if present (dateTime, time),
    /// or the seconds beyond the whole minutes (duration).
    Seconds,
}

/// A temporal value: a dateTime, a date or a time.
enum Temporal {
    /// A dateTime.
    DateTime(DateTime),
    /// A date.
    Date(Date),
    /// A time.
    Time(Time),
}

impl Temporal {
    /// Returns the temporal value of a value, if it is one.
    fn of(value: ValueRef<'_>) -> Option<Self> {
        match value {
            ValueRef::DateTime(value) => Some(Self::DateTime(value)),
            ValueRef::Date(value) => Some(Self::Date(value)),
            ValueRef::Time(value) => Some(Self::Time(value)),
            _ => None,
        }
    }

    /// Returns the timezone offset the value was written with, if present.
    fn offset(&self) -> Option<TimezoneOffset> {
        match self {
            Self::DateTime(value) => value.offset(),
            Self::Date(value) => value.offset(),
            Self::Time(value) => value.offset(),
        }
    }
}

/// Extracts one component of a dateTime, date, time or duration.
///
/// The components of a duration carry its sign,
/// as XPath's `fn:years-from-duration` and its kin do.
pub(crate) fn component(value: ValueRef<'_>, component: Component) -> StackValue<'static> {
    let result = match (value, component) {
        (ValueRef::DateTime(value), Component::Year) => integer(value.year()),
        (ValueRef::DateTime(value), Component::Month) => integer(value.month()),
        (ValueRef::DateTime(value), Component::Day) => integer(value.day()),
        (ValueRef::DateTime(value), Component::Hours) => integer(value.hour()),
        (ValueRef::DateTime(value), Component::Minutes) => integer(value.minute()),
        (ValueRef::DateTime(value), Component::Seconds) => {
            seconds(value.second(), value.attosecond())
        }
        (ValueRef::Date(value), Component::Year) => integer(value.year()),
        (ValueRef::Date(value), Component::Month) => integer(value.month()),
        (ValueRef::Date(value), Component::Day) => integer(value.day()),
        (ValueRef::Time(value), Component::Hours) => integer(value.hour()),
        (ValueRef::Time(value), Component::Minutes) => integer(value.minute()),
        (ValueRef::Time(value), Component::Seconds) => seconds(value.second(), value.attosecond()),
        (ValueRef::Duration(value), Component::Year) => integer(value.years()),
        (ValueRef::Duration(value), Component::Month) => integer(value.months()),
        (ValueRef::Duration(value), Component::Day) => integer(value.days()),
        (ValueRef::Duration(value), Component::Hours) => integer(value.hours()),
        (ValueRef::Duration(value), Component::Minutes) => integer(value.minutes()),
        (ValueRef::Duration(value), Component::Seconds) => {
            seconds(value.seconds(), value.attosecond())
        }
        _ => None,
    };

    match result {
        Some(value) => value.into(),
        None => EvaluationError::TypeMismatch.into(),
    }
}

/// Returns an integer component.
fn integer(value: impl Into<i64>) -> Option<DataValue> {
    Some(DataValue::integer(value.into()))
}

/// Returns the seconds component as an exact number: the whole seconds and their fraction.
fn seconds(second: impl Into<i128>, attosecond: impl Into<i128>) -> Option<DataValue> {
    DataValue::decimal(
        second.into() * ATTOSECONDS_PER_SECOND + attosecond.into(),
        -i64::from(FRACTION_DIGITS),
    )
}

/// Returns the timezone of a temporal value as a plain string,
/// empty if absent,
/// SPARQL's [`TZ`](https://www.w3.org/TR/sparql12-query/#func-tz).
pub(crate) fn timezone_string(value: ValueRef<'_>) -> StackValue<'static> {
    let Some(temporal) = Temporal::of(value) else {
        return EvaluationError::TypeMismatch.into();
    };
    DataValue::string(
        temporal
            .offset()
            .map(|offset| offset.to_string())
            .unwrap_or_default(),
    )
    .into()
}

/// Returns the timezone of a temporal value as an `xsd:dayTimeDuration`,
/// [Missing](EvaluationError::Missing) if the value has none,
/// SPARQL's [`TIMEZONE`](https://www.w3.org/TR/sparql12-query/#func-timezone),
/// XPath's [`fn:timezone-from-dateTime`].
///
/// [`fn:timezone-from-dateTime`]: https://www.w3.org/TR/xpath-functions-31/#func-timezone-from-dateTime
pub(crate) fn timezone_duration(value: ValueRef<'_>) -> StackValue<'static> {
    let Some(temporal) = Temporal::of(value) else {
        return EvaluationError::TypeMismatch.into();
    };
    let Some(offset) = temporal.offset() else {
        return EvaluationError::Missing.into();
    };
    DataValue::duration(Duration::from(offset)).into()
}

/// Returns a dateTime, date or time adjusted to the timezone a dayTimeDuration names,
/// SPARQL 1.2's `ADJUST`,
/// XPath's [`fn:adjust-dateTime-to-timezone`] and its kin.
///
/// A zoned value keeps its instant and reads it in the new timezone;
/// an unzoned value keeps its local fields and takes the timezone.
/// The timezone is a dayTimeDuration, any other duration a
/// [TypeMismatch](EvaluationError::TypeMismatch),
/// of whole minutes within fourteen hours either way;
/// any other dayTimeDuration is [Malformed](EvaluationError::Malformed).
///
/// [`fn:adjust-dateTime-to-timezone`]: https://www.w3.org/TR/xpath-functions-31/#func-adjust-dateTime-to-timezone
pub(crate) fn adjust(value: ValueRef<'_>, timezone: ValueRef<'_>) -> StackValue<'static> {
    let (Some(temporal), ValueRef::Duration(timezone)) = (Temporal::of(value), timezone) else {
        return EvaluationError::TypeMismatch.into();
    };
    if !timezone.is_day_time() {
        return EvaluationError::TypeMismatch.into();
    }
    let Some(offset) = TimezoneOffset::from_duration(timezone) else {
        return EvaluationError::Malformed.into();
    };

    let adjusted = match temporal {
        Temporal::DateTime(value) => value.adjust(Some(offset)).map(DataValue::datetime),
        Temporal::Date(value) => value.adjust(Some(offset)).map(DataValue::date),
        Temporal::Time(value) => value.adjust(Some(offset)).map(DataValue::time),
    };
    adjusted.ok_or(EvaluationError::Overflow).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{testing::value, value::datatype::xsd};

    /// The value of a typed literal; panics on a lexical the datatype does not accept.
    fn literal(datatype: &str, lexical: &str) -> DataValue {
        DataValue::typed_literal(datatype, lexical).expect(lexical)
    }

    #[test]
    fn durations_have_their_components() {
        // The examples of XPath's `fn:years-from-duration` and its kin.
        for (lexical, part, expected) in [
            ("P20Y15M", Component::Year, "21"),
            ("-P15M", Component::Year, "-1"),
            ("P20Y15M", Component::Month, "3"),
            ("-P20Y18M", Component::Month, "-6"),
            ("P3DT55H", Component::Day, "5"),
            ("P3DT12H32M12S", Component::Hours, "12"),
            ("PT123H", Component::Hours, "3"),
            ("-P5DT12H30M", Component::Minutes, "-30"),
            ("P3DT10H12.5S", Component::Seconds, "12.5"),
            ("-PT256S", Component::Seconds, "-16"),
            ("P1M", Component::Day, "0"),
        ] {
            assert_eq!(
                value(component(literal(xsd::DURATION, lexical).as_ref(), part)),
                DataValue::decimal_from_lexical(expected).expect("well-formed"),
                "{part:?} of {lexical}"
            );
        }
    }

    #[test]
    fn values_adjust_to_a_timezone() {
        let adjusted = |datatype, lexical, timezone| {
            adjust(
                literal(datatype, lexical).as_ref(),
                literal(xsd::DAY_TIME_DURATION, timezone).as_ref(),
            )
        };

        // The examples of XPath's `fn:adjust-dateTime-to-timezone` and its kin.
        assert_eq!(
            value(adjusted(
                xsd::DATE_TIME,
                "2002-03-07T10:00:00-07:00",
                "PT10H"
            )),
            literal(xsd::DATE_TIME, "2002-03-08T03:00:00+10:00")
        );
        assert_eq!(
            value(adjusted(xsd::DATE_TIME, "2002-03-07T10:00:00", "PT10H")),
            literal(xsd::DATE_TIME, "2002-03-07T10:00:00+10:00")
        );
        assert_eq!(
            value(adjusted(xsd::DATE, "2002-03-07-07:00", "-PT10H")),
            literal(xsd::DATE, "2002-03-06-10:00")
        );
        assert_eq!(
            value(adjusted(xsd::TIME, "10:00:00-07:00", "PT10H")),
            literal(xsd::TIME, "03:00:00+10:00")
        );

        // A timezone is whole minutes within fourteen hours either way.
        let error = |result: StackValue<'_>| result.as_result().err();
        assert_eq!(
            error(adjusted(xsd::DATE_TIME, "2002-03-07T10:00:00", "PT14H1M")),
            Some(EvaluationError::Malformed)
        );
        assert_eq!(
            error(adjust(
                literal(xsd::DATE, "2002-03-07").as_ref(),
                literal(xsd::YEAR_MONTH_DURATION, "P1M").as_ref()
            )),
            Some(EvaluationError::TypeMismatch)
        );
        assert_eq!(
            error(adjust(
                literal(xsd::DATE, "2002-03-07").as_ref(),
                DataValue::integer(60).as_ref()
            )),
            Some(EvaluationError::TypeMismatch)
        );
    }

    #[test]
    fn a_value_without_a_timezone_misses_one() {
        let unzoned = literal(xsd::DATE_TIME, "2002-03-07T10:00:00");
        assert_eq!(
            timezone_duration(unzoned.as_ref()).as_result().err(),
            Some(EvaluationError::Missing)
        );
        assert_eq!(
            timezone_duration(DataValue::integer(1).as_ref())
                .as_result()
                .err(),
            Some(EvaluationError::TypeMismatch)
        );
    }

    #[test]
    fn components_read_inline_and_wide_datetimes() {
        for (lexical, year, seconds) in [
            ("2024-03-01T12:30:05.25+05:30", 2024, "5.25"),
            ("2024-03-01T12:30:05.000250+05:30", 2024, "5.00025"),
            ("-13798000000-03-01T12:30:05+05:30", -13_798_000_000, "5"),
        ] {
            let datetime = DataValue::datetime_from_lexical(lexical).expect("valid");
            let component = |component| value(self::component(datetime.as_ref(), component));
            assert_eq!(component(Component::Year), DataValue::integer(year));
            assert_eq!(component(Component::Month), DataValue::integer(3));
            assert_eq!(component(Component::Hours), DataValue::integer(12));
            assert_eq!(
                component(Component::Seconds),
                DataValue::decimal_from_lexical(seconds).expect("well-formed")
            );
            assert_eq!(
                value(timezone_string(datetime.as_ref())),
                DataValue::string("+05:30")
            );
            assert_eq!(
                value(timezone_duration(datetime.as_ref())),
                literal(xsd::DAY_TIME_DURATION, "PT5H30M")
            );
        }
    }

    #[test]
    fn dates_and_times_have_their_components() {
        let date = literal(xsd::DATE, "2024-03-01Z");
        assert_eq!(
            value(component(date.as_ref(), Component::Year)),
            DataValue::integer(2024)
        );
        assert_eq!(
            value(component(date.as_ref(), Component::Day)),
            DataValue::integer(1)
        );
        assert_eq!(
            value(timezone_string(date.as_ref())),
            DataValue::string("Z")
        );
        assert!(
            component(date.as_ref(), Component::Hours)
                .as_result()
                .is_err()
        );
        let time = literal(xsd::TIME, "12:30:05.5");
        assert_eq!(
            value(component(time.as_ref(), Component::Seconds)),
            DataValue::decimal_from_lexical("5.5").expect("well-formed")
        );
        assert_eq!(value(timezone_string(time.as_ref())), DataValue::string(""));
        assert!(timezone_duration(time.as_ref()).as_result().is_err());
        let fraction = literal(xsd::TIME, "12:30:05.000250");
        assert_eq!(
            value(component(fraction.as_ref(), Component::Seconds)),
            DataValue::decimal_from_lexical("5.00025").expect("well-formed")
        );
        assert!(
            component(fraction.as_ref(), Component::Year)
                .as_result()
                .is_err()
        );
        assert_eq!(
            value(timezone_duration(
                literal(xsd::DATE, "2024-03-01Z").as_ref()
            )),
            literal(xsd::DAY_TIME_DURATION, "PT0S")
        );
    }
}
