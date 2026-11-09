//! Dates, times and durations.
//!
//! The types here represent the XSD datatypes
//! [`dateTime`](https://www.w3.org/TR/xmlschema11-2/#dateTime),
//! [`date`](https://www.w3.org/TR/xmlschema11-2/#date),
//! [`time`](https://www.w3.org/TR/xmlschema11-2/#time) and
//! [`duration`](https://www.w3.org/TR/xmlschema11-2/#duration),
//! and the timezone offset that they share.
//!
//! A dateTime, a date and a time each consist of an *instant*, a point on the timeline,
//! and the *timezone offset* that the value was written with, if any.
//! A *zoned* value has an offset and denotes one instant.
//! An *unzoned* value has no offset.
//! It is a local time, and its instant is known only to within fourteen hours,
//! the largest offset in either direction.
//! The instant of a date is the start of its day.
//! The instant of a time is that time of day on the *reference day* `1972-12-31`,
//! the date that [XSD fills in](https://www.w3.org/TR/xmlschema11-2/#theSevenPropertyModel)
//! for a value without one.
//! A duration is a number of months and a number of seconds, which never have opposite signs.
//!
//! The offset as written is part of a value,
//! so `2024-01-01T00:00:00Z` and `2024-01-01T01:00:00+01:00` are distinct values.
//! XSD's comparison by value, [value_eq](DateTime::value_eq) and
//! [value_cmp](DateTime::value_cmp), finds those two equal.
//! It orders a zoned and an unzoned value only if they are more than fourteen hours apart.
//! A duration's identity is its value, so durations have no separate equality by value,
//! and their [value_cmp](Duration::value_cmp) is partial.
//!
//! Each type parses every XSD lexical form with its `from_lexical`,
//! and writes the canonical form with `Display`.
//! For example, `24:00:00` becomes the start of the next day, `+00:00` is written as `Z`,
//! and the trailing zeros of a fraction are dropped.
//! The value space has eighteen fraction digits of a second,
//! and years up to about 5.4 trillion in either direction.
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
