//! Dates, times and durations.
//!
//! The types here are the XSD datatypes
//! [`dateTime`](https://www.w3.org/TR/xmlschema11-2/#dateTime),
//! [`date`](https://www.w3.org/TR/xmlschema11-2/#date),
//! [`time`](https://www.w3.org/TR/xmlschema11-2/#time) and
//! [`duration`](https://www.w3.org/TR/xmlschema11-2/#duration),
//! with the timezone offset they share.
//!
//! A dateTime, a date and a time are each an *instant*, a point on the timeline,
//! together with the *timezone offset* the value was written with.
//! A *zoned* value carries an offset and denotes one instant.
//! An *unzoned* value carries none: it is a local time
//! whose instant is not known closer than the fourteen hours an offset can reach either way.
//! A date is the instant its day starts; a time is the instant of that time of day
//! on the reference day `1972-12-31`, the date
//! [XSD fills in](https://www.w3.org/TR/xmlschema11-2/#theSevenPropertyModel)
//! for a value without one.
//! A duration is a number of months and a number of seconds, of one sign.
//!
//! The offset as written is part of a value,
//! so `2024-01-01T00:00:00Z` and `2024-01-01T01:00:00+01:00` are distinct values.
//! XSD's value comparison, [value_eq](DateTime::value_eq) and [value_cmp](DateTime::value_cmp),
//! finds those two equal, and orders a zoned and an unzoned value
//! only when they are more than fourteen hours apart.
//! A duration's identity is its value, so durations have no separate value equality,
//! and their [value_cmp](Duration::value_cmp) is partial.
//!
//! Every type reads every XSD lexical form with `from_lexical`
//! and writes the canonical form with `Display`:
//! `24:00:00` rolls over to the next day, `+00:00` prints as `Z`,
//! and trailing zeros of a fraction drop.
//! The value space carries eighteen fraction digits and years to about ±5.4 trillion.
//!
//! # Example
//!
//! ```
//! use nemo_physical::value::{DateTime, Duration};
//!
//! let start = DateTime::from_lexical("2024-02-29T23:30:00+01:00").unwrap();
//! let later = start.checked_add(Duration::from_lexical("P1DT1H").unwrap()).unwrap();
//! assert_eq!(later.to_string(), "2024-03-02T00:30:00+01:00");
//!
//! let utc = DateTime::from_lexical("2024-02-29T22:30:00Z").unwrap();
//! assert_ne!(start, utc);
//! assert!(start.value_eq(utc));
//!
//! let unzoned = DateTime::from_lexical("2024-02-29T22:30:00").unwrap();
//! assert_eq!(start.value_cmp(unzoned), None);
//! ```

mod instant;

pub(crate) mod date;
pub(crate) mod datetime;
pub(crate) mod duration;
pub(crate) mod offset;
pub(crate) mod time;

pub use date::Date;
pub use datetime::DateTime;
pub use duration::Duration;
pub use offset::TimezoneOffset;
pub use time::Time;

pub(crate) use instant::{ATTOSECONDS_PER_SECOND, FRACTION_DIGITS};
