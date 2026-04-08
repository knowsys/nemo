//! This module defines functions for extracting components from XSD date/time values.

use chrono::{Datelike, Days, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use crate::{
    datatypes::StorageTypeName,
    datavalues::{AnyDataValue, DataValue, ValueDomain},
};

use super::{FunctionTypePropagation, UnaryFunction};

const XSD_DATETIME: &str = "http://www.w3.org/2001/XMLSchema#dateTime";
const XSD_DATE: &str = "http://www.w3.org/2001/XMLSchema#date";
const XSD_TIME: &str = "http://www.w3.org/2001/XMLSchema#time";
const XSD_DAY_TIME_DURATION: &str = "http://www.w3.org/2001/XMLSchema#dayTimeDuration";

fn int_type_propagation() -> FunctionTypePropagation {
    FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
}

fn double_type_propagation() -> FunctionTypePropagation {
    FunctionTypePropagation::KnownOutput(StorageTypeName::Double.bitset())
}

fn string_type_propagation() -> FunctionTypePropagation {
    FunctionTypePropagation::KnownOutput(
        StorageTypeName::Id32
            .bitset()
            .union(StorageTypeName::Id64.bitset()),
    )
}

/// Retrieve a `ValueDomain::Other` value's lexical value and datatype IRI.
fn other_parts(value: AnyDataValue) -> Option<(String, String)> {
    if value.value_domain() != ValueDomain::Other {
        return None;
    }
    Some((value.lexical_value(), value.datatype_iri()))
}

/// Split a timezone suffix from the end of an XSD date/time lexical string.
///
/// Returns `(base, tz_str)` where `tz_str` is `None` when no timezone is present,
/// or `Some(raw)` with the raw timezone suffix (e.g. `"Z"`, `"+05:30"`, `"-05:00"`).
fn split_timezone(s: &str) -> (&str, Option<&str>) {
    if let Some(stripped) = s.strip_suffix('Z') {
        return (stripped, Some(&s[s.len() - 1..]));
    }
    if s.len() >= 6 {
        let tz_start = s.len() - 6;
        let candidate = &s[tz_start..];
        if (candidate.starts_with('+') || candidate.starts_with('-'))
            && candidate[1..3].bytes().all(|c| c.is_ascii_digit())
            && candidate.as_bytes()[3] == b':'
            && candidate[4..6].bytes().all(|c| c.is_ascii_digit())
        {
            return (&s[..tz_start], Some(candidate));
        }
    }
    (s, None)
}

/// Parse an XSD dateTime lexical value (without timezone).
///
/// Handles the special XSD/XPath case of `T24:00:00` (end-of-day midnight),
/// which is equivalent to midnight at the start of the following day.
/// See XPath functions spec §9.5.1: `fn:year-from-dateTime("1999-12-31T24:00:00")` returns 2000.
fn parse_naive_datetime(s: &str) -> Option<NaiveDateTime> {
    // Detect T24:00:00 — XSD allows this to mean start of the next day
    if let Some(t_pos) = s.find('T')
        && s[t_pos + 1..].starts_with("24:00:00")
    {
        let date = NaiveDate::parse_from_str(&s[..t_pos], "%Y-%m-%d").ok()?;
        let next_day = date.checked_add_days(Days::new(1))?;
        return next_day.and_hms_opt(0, 0, 0);
    }
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .or_else(|| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok())
}

/// Parse an XSD date lexical value (without timezone).
fn parse_naive_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

/// Parse an XSD time lexical value (without timezone).
///
/// Handles the special XSD/XPath case of `24:00:00` (end-of-day midnight),
/// which is equivalent to `00:00:00`.
/// See XPath functions spec §9.5.4: `fn:hours-from-time("24:00:00")` returns 0.
fn parse_naive_time(s: &str) -> Option<NaiveTime> {
    if s.starts_with("24:00:00") {
        return NaiveTime::from_hms_opt(0, 0, 0);
    }
    NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .ok()
        .or_else(|| NaiveTime::parse_from_str(s, "%H:%M:%S").ok())
}

/// Convert a raw timezone suffix string (e.g. `"Z"`, `"+05:30"`, `"-05:00"`)
/// to an `xsd:dayTimeDuration` lexical string.
fn tz_to_day_time_duration(tz: &str) -> String {
    if tz == "Z" {
        return "PT0S".to_string();
    }
    let negative = tz.starts_with('-');
    let h: u32 = tz[1..3].parse().unwrap_or(0);
    let m: u32 = tz[4..6].parse().unwrap_or(0);
    if h == 0 && m == 0 {
        return "PT0S".to_string();
    }
    let sign = if negative { "-" } else { "" };
    match (h, m) {
        (h, 0) => format!("{sign}PT{h}H"),
        (0, m) => format!("{sign}PT{m}M"),
        (h, m) => format!("{sign}PT{h}H{m}M"),
    }
}

// ─── Date component extractors ───────────────────────────────────────────────

/// Extract the year from an XSD dateTime or date value.
///
/// Corresponds to SPARQL `YEAR(arg)`. Returns an integer.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeYear;
impl UnaryFunction for DateTimeYear {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = other_parts(parameter)?;
        let (base, _tz) = split_timezone(&lexical);
        let year = if datatype == XSD_DATETIME {
            let dt = parse_naive_datetime(base)?;
            dt.year()
        } else if datatype == XSD_DATE {
            let d = parse_naive_date(base)?;
            d.year()
        } else {
            return None;
        };
        Some(AnyDataValue::new_integer_from_i64(year as i64))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        int_type_propagation()
    }
}

/// Extract the month from an XSD dateTime or date value.
///
/// Corresponds to SPARQL `MONTH(arg)`. Returns an integer (1–12).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeMonth;
impl UnaryFunction for DateTimeMonth {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = other_parts(parameter)?;
        let (base, _tz) = split_timezone(&lexical);
        let month = if datatype == XSD_DATETIME {
            let dt = parse_naive_datetime(base)?;
            dt.month()
        } else if datatype == XSD_DATE {
            let d = parse_naive_date(base)?;
            d.month()
        } else {
            return None;
        };
        Some(AnyDataValue::new_integer_from_i64(month as i64))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        int_type_propagation()
    }
}

/// Extract the day from an XSD dateTime or date value.
///
/// Corresponds to SPARQL `DAY(arg)`. Returns an integer (1–31).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeDay;
impl UnaryFunction for DateTimeDay {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = other_parts(parameter)?;
        let (base, _tz) = split_timezone(&lexical);
        let day = if datatype == XSD_DATETIME {
            let dt = parse_naive_datetime(base)?;
            dt.day()
        } else if datatype == XSD_DATE {
            let d = parse_naive_date(base)?;
            d.day()
        } else {
            return None;
        };
        Some(AnyDataValue::new_integer_from_i64(day as i64))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        int_type_propagation()
    }
}

// ─── Time component extractors ───────────────────────────────────────────────

/// Extract the hours from an XSD dateTime or time value.
///
/// Corresponds to SPARQL `HOURS(arg)`. Returns an integer (0–23).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeHours;
impl UnaryFunction for DateTimeHours {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = other_parts(parameter)?;
        let (base, _tz) = split_timezone(&lexical);
        let hour = if datatype == XSD_DATETIME {
            parse_naive_datetime(base)?.hour()
        } else if datatype == XSD_TIME {
            parse_naive_time(base)?.hour()
        } else {
            return None;
        };
        Some(AnyDataValue::new_integer_from_i64(hour as i64))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        int_type_propagation()
    }
}

/// Extract the minutes from an XSD dateTime or time value.
///
/// Corresponds to SPARQL `MINUTES(arg)`. Returns an integer (0–59).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeMinutes;
impl UnaryFunction for DateTimeMinutes {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = other_parts(parameter)?;
        let (base, _tz) = split_timezone(&lexical);
        let minute = if datatype == XSD_DATETIME {
            parse_naive_datetime(base)?.minute()
        } else if datatype == XSD_TIME {
            parse_naive_time(base)?.minute()
        } else {
            return None;
        };
        Some(AnyDataValue::new_integer_from_i64(minute as i64))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        int_type_propagation()
    }
}

/// Extract the seconds from an XSD dateTime or time value.
///
/// Corresponds to SPARQL `SECONDS(arg)`. Returns a double.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeSeconds;
impl UnaryFunction for DateTimeSeconds {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = other_parts(parameter)?;
        let (base, _tz) = split_timezone(&lexical);
        let t = if datatype == XSD_DATETIME {
            parse_naive_datetime(base)?.time()
        } else if datatype == XSD_TIME {
            parse_naive_time(base)?
        } else {
            return None;
        };
        let seconds = t.second() as f64 + t.nanosecond() as f64 / 1_000_000_000.0;
        AnyDataValue::new_double_from_f64(seconds).ok()
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        double_type_propagation()
    }
}

// ─── Timezone extractors ─────────────────────────────────────────────────────

/// Extract the timezone as an `xsd:dayTimeDuration` typed literal.
///
/// Corresponds to SPARQL `TIMEZONE(arg)`.
/// Returns `None` if the argument has no timezone.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeTimezone;
impl UnaryFunction for DateTimeTimezone {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = other_parts(parameter)?;
        if datatype != XSD_DATETIME && datatype != XSD_DATE && datatype != XSD_TIME {
            return None;
        }
        let (_, tz) = split_timezone(&lexical);
        let tz = tz?; // no timezone → undefined
        Some(AnyDataValue::new_other(
            tz_to_day_time_duration(tz),
            XSD_DAY_TIME_DURATION.to_string(),
        ))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        string_type_propagation()
    }
}

/// Extract the timezone as a plain string (e.g. `"Z"`, `"-05:00"`).
///
/// Corresponds to SPARQL `TZ(arg)`.
/// Returns `""` if the argument has no timezone.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeTz;
impl UnaryFunction for DateTimeTz {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = other_parts(parameter)?;
        if datatype != XSD_DATETIME && datatype != XSD_DATE && datatype != XSD_TIME {
            return None;
        }
        let (_, tz) = split_timezone(&lexical);
        Some(AnyDataValue::new_plain_string(tz.unwrap_or("").to_string()))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        string_type_propagation()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::datavalues::DataValue;

    fn dt(lex: &str) -> AnyDataValue {
        AnyDataValue::new_other(lex.to_string(), XSD_DATETIME.to_string())
    }
    fn date(lex: &str) -> AnyDataValue {
        AnyDataValue::new_other(lex.to_string(), XSD_DATE.to_string())
    }
    fn time(lex: &str) -> AnyDataValue {
        AnyDataValue::new_other(lex.to_string(), XSD_TIME.to_string())
    }

    #[test]
    fn test_year_datetime() {
        let v = DateTimeYear.evaluate(dt("2023-06-15T10:30:45")).unwrap();
        assert_eq!(v, AnyDataValue::new_integer_from_i64(2023));
    }

    #[test]
    fn test_year_date() {
        let v = DateTimeYear.evaluate(date("2023-06-15")).unwrap();
        assert_eq!(v, AnyDataValue::new_integer_from_i64(2023));
    }

    #[test]
    fn test_year_with_timezone() {
        let v = DateTimeYear
            .evaluate(dt("2023-06-15T10:30:45+05:30"))
            .unwrap();
        assert_eq!(v, AnyDataValue::new_integer_from_i64(2023));
    }

    #[test]
    fn test_month() {
        let v = DateTimeMonth.evaluate(dt("2023-06-15T10:30:45")).unwrap();
        assert_eq!(v, AnyDataValue::new_integer_from_i64(6));
    }

    #[test]
    fn test_day() {
        let v = DateTimeDay.evaluate(dt("2023-06-15T10:30:45")).unwrap();
        assert_eq!(v, AnyDataValue::new_integer_from_i64(15));
    }

    #[test]
    fn test_hours_datetime() {
        let v = DateTimeHours.evaluate(dt("2023-06-15T10:30:45")).unwrap();
        assert_eq!(v, AnyDataValue::new_integer_from_i64(10));
    }

    #[test]
    fn test_hours_time() {
        let v = DateTimeHours.evaluate(time("10:30:45")).unwrap();
        assert_eq!(v, AnyDataValue::new_integer_from_i64(10));
    }

    #[test]
    fn test_minutes() {
        let v = DateTimeMinutes.evaluate(dt("2023-06-15T10:30:45")).unwrap();
        assert_eq!(v, AnyDataValue::new_integer_from_i64(30));
    }

    #[test]
    fn test_seconds_integer() {
        let v = DateTimeSeconds.evaluate(dt("2023-06-15T10:30:45")).unwrap();
        assert_eq!(v, AnyDataValue::new_double_from_f64(45.0).unwrap());
    }

    #[test]
    fn test_seconds_fractional() {
        let v = DateTimeSeconds
            .evaluate(dt("2023-06-15T10:30:45.5"))
            .unwrap();
        assert_eq!(v, AnyDataValue::new_double_from_f64(45.5).unwrap());
    }

    #[test]
    fn test_tz_absent() {
        let v = DateTimeTz.evaluate(dt("2023-06-15T10:30:45")).unwrap();
        assert_eq!(v, AnyDataValue::new_plain_string("".to_string()));
    }

    #[test]
    fn test_tz_utc() {
        let v = DateTimeTz.evaluate(dt("2023-06-15T10:30:45Z")).unwrap();
        assert_eq!(v, AnyDataValue::new_plain_string("Z".to_string()));
    }

    #[test]
    fn test_tz_offset() {
        let v = DateTimeTz
            .evaluate(dt("2023-06-15T10:30:45-05:00"))
            .unwrap();
        assert_eq!(v, AnyDataValue::new_plain_string("-05:00".to_string()));
    }

    #[test]
    fn test_timezone_absent() {
        assert!(
            DateTimeTimezone
                .evaluate(dt("2023-06-15T10:30:45"))
                .is_none()
        );
    }

    #[test]
    fn test_timezone_utc() {
        let v = DateTimeTimezone
            .evaluate(dt("2023-06-15T10:30:45Z"))
            .unwrap();
        assert_eq!(v.lexical_value(), "PT0S");
        assert_eq!(v.datatype_iri(), XSD_DAY_TIME_DURATION);
    }

    #[test]
    fn test_timezone_negative() {
        let v = DateTimeTimezone
            .evaluate(dt("2023-06-15T10:30:45-05:00"))
            .unwrap();
        assert_eq!(v.lexical_value(), "-PT5H");
    }

    #[test]
    fn test_timezone_positive_with_minutes() {
        let v = DateTimeTimezone
            .evaluate(dt("2023-06-15T10:30:45+05:30"))
            .unwrap();
        assert_eq!(v.lexical_value(), "PT5H30M");
    }

    #[test]
    fn test_invalid_date_rejected() {
        // chrono rejects Feb 30
        let v = DateTimeYear.evaluate(date("2023-02-30"));
        assert!(v.is_none());
    }
}
