//! The payload encodings of the temporal types: dateTimes, dates, times and durations.
//!
//! The first three share the [instant] layout,
//! a count in the unit of the type above the code of the timezone offset.

mod date;
mod datetime;
mod duration;
mod instant;
mod time;
