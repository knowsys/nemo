//! The component and timezone functions of dates, times and durations.
//!
//! They are SPARQL's
//! [`YEAR` through `TIMEZONE`](https://www.w3.org/TR/sparql12-query/#func-year),
//! which are XPath's
//! [component functions](https://www.w3.org/TR/xpath-functions-31/#func-year-from-dateTime),
//! and SPARQL 1.2's `ADJUST`.
//! SPARQL 1.1 defines the components on dateTimes only,
//! SPARQL 1.2 and XPath also on dates and times, and XPath also on durations.
//! This module follows XPath.
//! A component is read in the value's own timezone, never normalized to UTC.
//!
//! The arithmetic and the comparisons of these values are [ValueRef]'s operators.

use crate::{
    function::StackValue,
    value::{
        DataValue, Date, DateTime, Duration, EvaluationError, Time, TimezoneOffset, ValueRef,
        temporal::{ATTOSECONDS_PER_SECOND, FRACTION_DIGITS},
    },
};

/// The components that [component] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Component {
    /// The year of a dateTime or a date, or the whole years of a duration.
    Year,
    /// The month of a dateTime or a date, or the months of a duration beyond its whole years.
    Month,
    /// The day of a dateTime or a date, or the whole days of a duration.
    Day,
    /// The hours of a dateTime or a time, or the hours of a duration beyond its whole days.
    Hours,
    /// The minutes of a dateTime or a time,
    /// or the minutes of a duration beyond its whole hours.
    Minutes,
    /// The seconds of a dateTime or a time, with their fraction,
    /// or the seconds of a duration beyond its whole minutes.
    Seconds,
}

/// A point in time: a dateTime, a date or a time.
enum Temporal {
    /// A dateTime.
    DateTime(DateTime),
    /// A date.
    Date(Date),
    /// A time.
    Time(Time),
}

impl Temporal {
    /// Returns the value as a point in time, if it is one.
    fn of(value: ValueRef<'_>) -> Option<Self> {
        match value {
            ValueRef::DateTime(value) => Some(Self::DateTime(value)),
            ValueRef::Date(value) => Some(Self::Date(value)),
            ValueRef::Time(value) => Some(Self::Time(value)),
            _ => None,
        }
    }

    /// Returns the timezone offset of the value, if it has one.
    fn offset(&self) -> Option<TimezoneOffset> {
        match self {
            Self::DateTime(value) => value.offset(),
            Self::Date(value) => value.offset(),
            Self::Time(value) => value.offset(),
        }
    }
}

/// Returns one component of a dateTime, date, time or duration.
///
/// The components of a duration have its sign,
/// as in XPath's `fn:years-from-duration` and the related functions.
/// A value without the component, such as a date for the hours,
/// is a [TypeMismatch](EvaluationError::TypeMismatch).
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

/// Returns an integer component as a value.
fn integer(value: impl Into<i64>) -> Option<DataValue> {
    Some(DataValue::integer(value.into()))
}

/// Returns the seconds component, the whole seconds and their fraction, as an exact number.
fn seconds(second: impl Into<i128>, attosecond: impl Into<i128>) -> Option<DataValue> {
    DataValue::decimal(
        second.into() * ATTOSECONDS_PER_SECOND + attosecond.into(),
        -i64::from(FRACTION_DIGITS),
    )
}

/// Returns the timezone of a dateTime, date or time as a plain string,
/// SPARQL's [`TZ`](https://www.w3.org/TR/sparql12-query/#func-tz).
///
/// A value without a timezone gives the empty string.
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

/// Returns the timezone of a dateTime, date or time as a dayTimeDuration,
/// SPARQL's [`TIMEZONE`](https://www.w3.org/TR/sparql12-query/#func-timezone)
/// and XPath's [`fn:timezone-from-dateTime`].
///
/// A value without a timezone is [Missing](EvaluationError::Missing).
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

/// Returns a dateTime, date or time adjusted to the timezone that a dayTimeDuration gives,
/// SPARQL 1.2's `ADJUST`
/// and XPath's [`fn:adjust-dateTime-to-timezone`] with the related functions.
///
/// A value with a timezone keeps its instant, and its fields change to the new timezone.
/// A value without a timezone keeps its fields and gets the new timezone.
///
/// The timezone must be a dayTimeDuration,
/// and any other operand is a [TypeMismatch](EvaluationError::TypeMismatch).
/// It must be whole minutes within fourteen hours either way,
/// and any other dayTimeDuration is [Malformed](EvaluationError::Malformed).
/// An adjusted value outside the value space is an [Overflow](EvaluationError::Overflow).
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

    /// Returns the value of a typed literal,
    /// and panics for a lexical form that the datatype does not accept.
    fn literal(datatype: &str, lexical: &str) -> DataValue {
        DataValue::typed_literal(datatype, lexical).expect(lexical)
    }

    /// The components of a duration have its sign, as in XPath's examples.
    #[test]
    fn durations_have_their_components() {
        // The examples of XPath's `fn:years-from-duration` and the related functions.
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

    /// Values adjust to a timezone as in XPath's examples,
    /// and a timezone that is no dayTimeDuration of whole minutes within fourteen hours fails.
    #[test]
    fn values_adjust_to_a_timezone() {
        let adjusted = |datatype, lexical, timezone| {
            adjust(
                literal(datatype, lexical).as_ref(),
                literal(xsd::DAY_TIME_DURATION, timezone).as_ref(),
            )
        };

        // The examples of XPath's `fn:adjust-dateTime-to-timezone` and the related functions.
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

    /// The timezone of a value without one is missing,
    /// and a value that is not a point in time has the wrong type.
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

    /// Components and timezones are read alike from inline dateTimes and from wide ones.
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

    /// Dates and times have their own components and timezones,
    /// and reading a component that they lack fails.
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
