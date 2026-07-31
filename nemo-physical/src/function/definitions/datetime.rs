//! This module defines functions for extracting components from XSD date/time values.

use std::{
    str::FromStr,
    sync::{LazyLock, RwLock},
};

use oxsdatatypes::{Date, DateTime, Time};

use crate::{
    datatypes::StorageTypeName,
    datavalues::{AnyDataValue, DataValue},
};

use super::{FunctionTypePropagation, NullaryFunction, UnaryFunction};

const XSD_DATETIME: &str = "http://www.w3.org/2001/XMLSchema#dateTime";
const XSD_DATE: &str = "http://www.w3.org/2001/XMLSchema#date";
const XSD_TIME: &str = "http://www.w3.org/2001/XMLSchema#time";
const XSD_DAY_TIME_DURATION: &str = "http://www.w3.org/2001/XMLSchema#dayTimeDuration";

/// Timestamp returned by [FuncNow], captured via [set_now_timestamp].
///
/// Lazily initialized with the current time on first access
static NOW_TIMESTAMP: LazyLock<RwLock<String>> =
    LazyLock::new(|| RwLock::new(DateTime::now().to_string()));

/// Capture the current time as the timestamp returned by `NOW()`.
///
/// This should be called once at the start of each program execution.
/// All evaluations of `NOW()` during the execution will return this fixed value.
///
/// Note that the timestamp is global to the process.
pub fn set_now_timestamp() {
    *NOW_TIMESTAMP
        .write()
        .expect("no thread should panic while holding the lock") = DateTime::now().to_string();
}

/// Extract the year from an XSD dateTime or date value.
///
/// Corresponds to SPARQL `YEAR(arg)`. Returns an integer.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeYear;
impl UnaryFunction for DateTimeYear {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = parameter.to_other()?;
        let year = if datatype == XSD_DATETIME {
            DateTime::from_str(&lexical).ok()?.year()
        } else if datatype == XSD_DATE {
            Date::from_str(&lexical).ok()?.year()
        } else {
            return None;
        };
        Some(AnyDataValue::new_integer_from_i64(year))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Extract the month from an XSD dateTime or date value.
///
/// Corresponds to SPARQL `MONTH(arg)`. Returns an integer (1–12).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeMonth;
impl UnaryFunction for DateTimeMonth {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = parameter.to_other()?;
        let month = if datatype == XSD_DATETIME {
            DateTime::from_str(&lexical).ok()?.month()
        } else if datatype == XSD_DATE {
            Date::from_str(&lexical).ok()?.month()
        } else {
            return None;
        };
        Some(AnyDataValue::new_integer_from_i64(month as i64))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Extract the day from an XSD dateTime or date value.
///
/// Corresponds to SPARQL `DAY(arg)`. Returns an integer (1–31).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeDay;
impl UnaryFunction for DateTimeDay {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = parameter.to_other()?;
        let day = if datatype == XSD_DATETIME {
            DateTime::from_str(&lexical).ok()?.day()
        } else if datatype == XSD_DATE {
            Date::from_str(&lexical).ok()?.day()
        } else {
            return None;
        };
        Some(AnyDataValue::new_integer_from_i64(day as i64))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}
/// Extract the hours from an XSD dateTime or time value.
///
/// Corresponds to SPARQL `HOURS(arg)`. Returns an integer (0–23).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeHours;
impl UnaryFunction for DateTimeHours {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = parameter.to_other()?;
        let hour = if datatype == XSD_DATETIME {
            DateTime::from_str(&lexical).ok()?.hour()
        } else if datatype == XSD_TIME {
            Time::from_str(&lexical).ok()?.hour()
        } else {
            return None;
        };
        Some(AnyDataValue::new_integer_from_i64(hour as i64))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Extract the minutes from an XSD dateTime or time value.
///
/// Corresponds to SPARQL `MINUTES(arg)`. Returns an integer (0–59).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeMinutes;
impl UnaryFunction for DateTimeMinutes {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = parameter.to_other()?;
        let minute = if datatype == XSD_DATETIME {
            DateTime::from_str(&lexical).ok()?.minute()
        } else if datatype == XSD_TIME {
            Time::from_str(&lexical).ok()?.minute()
        } else {
            return None;
        };
        Some(AnyDataValue::new_integer_from_i64(minute as i64))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Extract the seconds from an XSD dateTime or time value.
///
/// Corresponds to SPARQL `SECONDS(arg)`. Returns an `xsd:decimal`,
/// which is an integer if the fractional digits are omitted or zero.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeSeconds;
impl UnaryFunction for DateTimeSeconds {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = parameter.to_other()?;
        let second = if datatype == XSD_DATETIME {
            DateTime::from_str(&lexical).ok()?.second()
        } else if datatype == XSD_TIME {
            Time::from_str(&lexical).ok()?.second()
        } else {
            return None;
        };
        AnyDataValue::new_from_decimal_literal(second.to_string()).ok()
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Int64
                .bitset()
                .union(StorageTypeName::Id32.bitset())
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Extract the timezone as an `xsd:dayTimeDuration` typed literal.
///
/// Corresponds to SPARQL `TIMEZONE(arg)`.
/// Returns `None` if the argument has no timezone.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DateTimeTimezone;
impl UnaryFunction for DateTimeTimezone {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let (lexical, datatype) = parameter.to_other()?;

        let timezone = if datatype == XSD_DATETIME {
            DateTime::from_str(&lexical).ok()?.timezone()
        } else if datatype == XSD_DATE {
            Date::from_str(&lexical).ok()?.timezone()
        } else if datatype == XSD_TIME {
            Time::from_str(&lexical).ok()?.timezone()
        } else {
            return None;
        };
        Some(AnyDataValue::new_other(
            timezone?.to_string(),
            XSD_DAY_TIME_DURATION.to_string(),
        ))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
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
        let (lexical, datatype) = parameter.to_other()?;
        let timezone_offset = if datatype == XSD_DATETIME {
            DateTime::from_str(&lexical).ok()?.timezone_offset()
        } else if datatype == XSD_DATE {
            Date::from_str(&lexical).ok()?.timezone_offset()
        } else if datatype == XSD_TIME {
            Time::from_str(&lexical).ok()?.timezone_offset()
        } else {
            return None;
        };
        let tz_str = timezone_offset.map(|tz| tz.to_string()).unwrap_or_default();
        Some(AnyDataValue::new_plain_string(tz_str))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Return the current date and time as an `xsd:dateTime` typed literal.
///
/// Corresponds to SPARQL `NOW()`.
///
/// Returns the timestamp captured by [set_now_timestamp], so all evaluations
/// during one program execution share the same value.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FuncNow;
impl NullaryFunction for FuncNow {
    fn evaluate(&self) -> Option<AnyDataValue> {
        let timestamp = NOW_TIMESTAMP
            .read()
            .expect("no thread should panic while holding the lock")
            .clone();
        Some(AnyDataValue::new_other(timestamp, XSD_DATETIME.to_string()))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}
