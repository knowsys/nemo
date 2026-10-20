//! The record behind the timestamp types and its bridge to `oxsdatatypes`.
//!
//! An [Instant] is a count of attoseconds since `0001-01-01T00:00:00Z`, UTC,
//! and the code of the timezone offset the value was written with.
//! An unzoned value stores its local fields read as UTC.
//! The derived equality and order are term identity and term order.
//! A dateTime is any instant, a date the instant of a day's start in its offset,
//! and a time the instant of a time of day on the reference day `1972-12-31` in its offset.
//!
//! Parsing, printing, the calendar fields, offset adjustment and duration arithmetic
//! are delegated to `oxsdatatypes`,
//! which keeps a timestamp as the same attoseconds and an offset in minutes,
//! serialized as sixteen big-endian bytes of the count and two of the offset.
//! The unzoned mark of that serialization, `FF FF`, is also the offset `-00:01`,
//! so the offset crosses the bridge as a value in both directions, never as bytes:
//! a value goes to `oxsdatatypes` as UTC and is adjusted to its offset,
//! and comes back with the offset read through its accessor.

use std::{cmp::Ordering, fmt, str::FromStr};

use oxsdatatypes::{
    Date as LibraryDate, DateTime as LibraryDateTime, DayTimeDuration, Time as LibraryTime,
    TimezoneOffset as LibraryOffset,
};

use crate::value::temporal::{Duration, TimezoneOffset};

/// The fraction digits of a second the value space carries.
pub(crate) const FRACTION_DIGITS: u32 = 18;

/// The attoseconds of a second.
pub(crate) const ATTOSECONDS_PER_SECOND: i128 = 10i128.pow(FRACTION_DIGITS);

/// The seconds of a day.
pub(super) const SECONDS_PER_DAY: i128 = 86_400;

/// The Unix epoch, `1970-01-01T00:00:00Z`, in seconds since `0001-01-01T00:00:00Z`.
pub(super) const UNIX_EPOCH_SECONDS: i128 = 62_135_596_800;

/// The start of the reference day of times, `1972-12-31T00:00:00Z`,
/// in seconds since `0001-01-01T00:00:00Z`.
pub(super) const TIME_REFERENCE_SECONDS: i128 = 62_230_204_800;

/// The offset code marking the absence of a timezone, above every offset's code.
const UNZONED_CODE: u16 = TimezoneOffset::CODE_COUNT;

/// The number of offset codes, the unzoned mark included.
pub(super) const OFFSET_CODE_COUNT: u64 = UNZONED_CODE as u64 + 1;

/// The distance within which a zoned and an unzoned value stay incomparable, in attoseconds.
const FOURTEEN_HOURS: i128 = 14 * 3_600 * ATTOSECONDS_PER_SECOND;

/// An instant and the offset it was written with.
///
/// The record is packed to 8-byte alignment so that it takes 24 bytes rather than 32.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(Rust, packed(8))]
pub(super) struct Instant {
    /// Attoseconds since `0001-01-01T00:00:00Z`.
    attoseconds: i128,
    /// The order-preserving code of the timezone offset the value was written with:
    /// zoned offsets at minute granularity, the unzoned mark above them.
    offset_code: u16,
}

/// What the bridge needs of an `oxsdatatypes` timestamp type: its dateTime, date or time.
pub(super) trait LibraryTimestamp: Copy + FromStr + fmt::Display {
    /// Returns the serialization: sixteen big-endian bytes of attoseconds and two of the offset.
    fn to_be_bytes(self) -> [u8; 18];
    /// Returns the offset the value was written with.
    fn timezone_offset(self) -> Option<LibraryOffset>;
    /// Creates the value of an `oxsdatatypes` dateTime at the same instant and offset.
    fn from_datetime(value: LibraryDateTime) -> Self;
    /// Returns the value adjusted to another offset, or `None` beyond the value space.
    fn adjust(self, offset: Option<LibraryOffset>) -> Option<Self>;
    /// Returns the value a duration later, or `None` beyond the value space.
    fn checked_add_duration(self, duration: oxsdatatypes::Duration) -> Option<Self>;
    /// Returns the value a duration earlier, or `None` beyond the value space.
    fn checked_sub_duration(self, duration: oxsdatatypes::Duration) -> Option<Self>;
    /// Returns the time from another value to this one,
    /// or `None` if one value is zoned and the other not.
    fn checked_sub(self, earlier: Self) -> Option<DayTimeDuration>;
}

/// Implements [LibraryTimestamp] for an `oxsdatatypes` type,
/// given how it is built from that crate's dateTime.
macro_rules! library_timestamp {
    ($type:ty, $from_datetime:expr) => {
        impl LibraryTimestamp for $type {
            fn to_be_bytes(self) -> [u8; 18] {
                self.to_be_bytes()
            }

            fn timezone_offset(self) -> Option<LibraryOffset> {
                self.timezone_offset()
            }

            fn from_datetime(value: LibraryDateTime) -> Self {
                $from_datetime(value)
            }

            fn adjust(self, offset: Option<LibraryOffset>) -> Option<Self> {
                self.adjust(offset)
            }

            fn checked_add_duration(self, duration: oxsdatatypes::Duration) -> Option<Self> {
                self.checked_add_duration(duration)
            }

            fn checked_sub_duration(self, duration: oxsdatatypes::Duration) -> Option<Self> {
                self.checked_sub_duration(duration)
            }

            fn checked_sub(self, earlier: Self) -> Option<DayTimeDuration> {
                self.checked_sub(earlier)
            }
        }
    };
}

library_timestamp!(LibraryDateTime, |value| value);
library_timestamp!(LibraryDate, |value: LibraryDateTime| {
    LibraryDate::try_from(value).expect("the day's start of an instant in range is in range")
});
library_timestamp!(LibraryTime, LibraryTime::from);

impl Instant {
    /// Creates the instant of a count of attoseconds and an offset.
    pub(super) fn new(attoseconds: i128, offset: Option<TimezoneOffset>) -> Self {
        Self {
            attoseconds,
            offset_code: offset.map_or(UNZONED_CODE, TimezoneOffset::code),
        }
    }

    /// Creates the instant of a count of attoseconds and a raw offset code,
    /// or `None` if the code is not one.
    pub(super) fn from_code(attoseconds: i128, offset_code: u64) -> Option<Self> {
        let offset_code = u16::try_from(offset_code).ok()?;
        (u64::from(offset_code) < OFFSET_CODE_COUNT).then_some(Self {
            attoseconds,
            offset_code,
        })
    }

    /// Creates the instant of a local time of day on a local day,
    /// given as seconds since the epoch in the offset and the fraction of the second,
    /// or `None` beyond the range of instants.
    pub(super) fn from_local(
        local_seconds: i128,
        attosecond: u64,
        offset: Option<TimezoneOffset>,
    ) -> Option<Self> {
        let seconds = local_seconds.checked_sub(offset_seconds(offset))?;
        let attoseconds = seconds
            .checked_mul(ATTOSECONDS_PER_SECOND)?
            .checked_add(i128::from(attosecond))?;
        Some(Self::new(attoseconds, offset))
    }

    /// Parses a lexical form of the `oxsdatatypes` type `T`, or `None` if it is not one.
    pub(super) fn from_lexical<T: LibraryTimestamp>(lexical: &str) -> Option<Self> {
        lexical.parse::<T>().ok().map(Self::from_library)
    }

    /// Creates the instant of an `oxsdatatypes` value.
    pub(super) fn from_library<T: LibraryTimestamp>(value: T) -> Self {
        let bytes = value.to_be_bytes();
        Self::new(
            i128::from_be_bytes(bytes[..16].try_into().expect("sixteen bytes")),
            value.timezone_offset().map(from_library_offset),
        )
    }

    /// Converts the value into the `oxsdatatypes` value at this instant and offset.
    ///
    /// The instant is serialized as UTC and the offset then adjusted,
    /// which keeps the instant and sets the offset as written;
    /// adjusting to no offset keeps the UTC fields as the local ones,
    /// which is how an unzoned value denotes its instant.
    pub(super) fn to_library<T: LibraryTimestamp>(self) -> T {
        let mut bytes = [0; 18];
        bytes[..16].copy_from_slice(&self.attoseconds.to_be_bytes());
        let datetime = LibraryDateTime::from_be_bytes(bytes)
            .adjust(self.offset().map(to_library_offset))
            .expect("adjusting from UTC keeps the instant, which is in range");
        T::from_datetime(datetime)
    }

    /// Returns the attoseconds since `0001-01-01T00:00:00Z`.
    pub(super) fn attoseconds(self) -> i128 {
        self.attoseconds
    }

    /// Returns the timezone offset the value was written with; `None` for an unzoned value.
    pub(super) fn offset(self) -> Option<TimezoneOffset> {
        TimezoneOffset::from_code(self.offset_code)
    }

    /// Returns the raw offset code, below [OFFSET_CODE_COUNT].
    pub(super) fn offset_code(self) -> u64 {
        u64::from(self.offset_code)
    }

    /// Returns whether the value was written with an offset.
    fn is_zoned(self) -> bool {
        self.offset_code != UNZONED_CODE
    }

    /// Returns the whole seconds since the epoch in the value's own offset:
    /// the local calendar fields as one count.
    pub(super) fn local_seconds(self) -> i128 {
        self.attoseconds.div_euclid(ATTOSECONDS_PER_SECOND) + offset_seconds(self.offset())
    }

    /// Returns the whole seconds since the start of the local day.
    pub(super) fn seconds_of_day(self) -> i128 {
        self.local_seconds().rem_euclid(SECONDS_PER_DAY)
    }

    /// Returns the hour of the local day.
    pub(super) fn hour(self) -> u8 {
        u8::try_from(self.seconds_of_day() / 3_600).expect("below 24")
    }

    /// Returns the minute of the local hour.
    pub(super) fn minute(self) -> u8 {
        u8::try_from(self.seconds_of_day() % 3_600 / 60).expect("below 60")
    }

    /// Returns the whole second of the local minute.
    pub(super) fn second(self) -> u8 {
        u8::try_from(self.seconds_of_day() % 60).expect("below 60")
    }

    /// Returns the fraction of the second in attoseconds.
    pub(super) fn attosecond(self) -> u64 {
        u64::try_from(self.attoseconds.rem_euclid(ATTOSECONDS_PER_SECOND)).expect("below 10^18")
    }

    /// Returns the instant of the start of the local day, in the value's offset,
    /// or `None` beyond the range of instants.
    pub(super) fn day_start(self) -> Option<Self> {
        let day_start = self.local_seconds().div_euclid(SECONDS_PER_DAY) * SECONDS_PER_DAY;
        Self::from_local(day_start, 0, self.offset())
    }

    /// Returns the instant of the local time of day on the reference day, in the value's offset.
    pub(super) fn time_of_day(self) -> Self {
        Self::from_local(
            TIME_REFERENCE_SECONDS + self.seconds_of_day(),
            self.attosecond(),
            self.offset(),
        )
        .expect("the reference day is far from the range limits")
    }

    /// Returns the instant of a time of day on the day this instant starts,
    /// or `None` if both carry an offset and the offsets differ
    /// or the result is beyond the range of instants.
    ///
    /// The offset of the result is the one that is present.
    pub(super) fn at(self, time: Self) -> Option<Self> {
        let offset = match (self.offset(), time.offset()) {
            (Some(date), Some(time)) if date != time => return None,
            (date, time) => date.or(time),
        };
        let local = self.local_seconds().checked_add(time.seconds_of_day())?;
        Self::from_local(local, time.attosecond(), offset)
    }

    /// Returns the value adjusted to another offset, computed through `T`,
    /// or `None` beyond the value space.
    pub(super) fn adjust<T: LibraryTimestamp>(
        self,
        offset: Option<TimezoneOffset>,
    ) -> Option<Self> {
        self.to_library::<T>()
            .adjust(offset.map(to_library_offset))
            .map(Self::from_library)
    }

    /// Returns the value a duration later, computed through `T`, or `None` beyond the value space.
    pub(super) fn checked_add<T: LibraryTimestamp>(self, duration: Duration) -> Option<Self> {
        self.to_library::<T>()
            .checked_add_duration(duration.to_library())
            .map(Self::from_library)
    }

    /// Returns the value a duration earlier, computed through `T`, or `None` beyond the value space.
    pub(super) fn checked_sub<T: LibraryTimestamp>(self, duration: Duration) -> Option<Self> {
        self.to_library::<T>()
            .checked_sub_duration(duration.to_library())
            .map(Self::from_library)
    }

    /// Returns the time from an earlier value to this one, computed through `T`,
    /// or `None` if one value is zoned and the other not.
    pub(super) fn duration_since<T: LibraryTimestamp>(self, earlier: Self) -> Option<Duration> {
        let seconds = self
            .to_library::<T>()
            .checked_sub(earlier.to_library::<T>())?;
        Some(Duration::day_time(i128::from_be_bytes(
            seconds.to_be_bytes(),
        )))
    }

    /// Returns whether two values are equal in XSD's value space:
    /// both zoned or both unzoned, and at the same instant.
    pub(super) fn value_eq(self, other: Self) -> bool {
        self.is_zoned() == other.is_zoned() && self.attoseconds == other.attoseconds
    }

    /// Returns the order of two values in XSD's value space,
    /// or `None` for a zoned and an unzoned value within fourteen hours of each other.
    ///
    /// A zoned value compares with an unzoned one only when the comparison
    /// comes out the same under every offset the unzoned value could carry.
    pub(super) fn value_cmp(self, other: Self) -> Option<Ordering> {
        let (left, right) = (self.attoseconds, other.attoseconds);
        match (self.is_zoned(), other.is_zoned()) {
            (true, true) | (false, false) => Some(left.cmp(&right)),
            (true, false) => {
                let plus = left.cmp(&right.checked_add(FOURTEEN_HOURS)?);
                let minus = left.cmp(&right.checked_sub(FOURTEEN_HOURS)?);
                (plus == minus).then_some(plus)
            }
            (false, true) => {
                let plus = left.checked_add(FOURTEEN_HOURS)?.cmp(&right);
                let minus = left.checked_sub(FOURTEEN_HOURS)?.cmp(&right);
                (plus == minus).then_some(plus)
            }
        }
    }
}

/// Returns the seconds an offset adds to UTC to reach local time; zero for an unzoned value.
fn offset_seconds(offset: Option<TimezoneOffset>) -> i128 {
    offset.map_or(0, |offset| i128::from(offset.minutes()) * 60)
}

/// Converts an offset into its `oxsdatatypes` form.
fn to_library_offset(offset: TimezoneOffset) -> LibraryOffset {
    LibraryOffset::new(offset.minutes()).expect("the offset ranges agree")
}

/// Converts an `oxsdatatypes` offset into a [TimezoneOffset].
fn from_library_offset(offset: LibraryOffset) -> TimezoneOffset {
    TimezoneOffset::new(i16::from_be_bytes(offset.to_be_bytes())).expect("the offset ranges agree")
}

/// Returns the date part of a canonical lexical form.
pub(super) fn date_lexical(year: i64, month: u8, day: u8) -> String {
    let sign = if year < 0 { "-" } else { "" };
    format!("{sign}{:04}-{month:02}-{day:02}", year.unsigned_abs())
}

/// Returns the time part of a canonical lexical form,
/// or `None` for a fraction of a whole second or more.
pub(super) fn time_lexical(hour: u8, minute: u8, second: u8, attosecond: u64) -> Option<String> {
    if i128::from(attosecond) >= ATTOSECONDS_PER_SECOND {
        return None;
    }
    let fraction = if attosecond == 0 {
        String::new()
    } else {
        format!(".{attosecond:018}")
    };
    Some(format!("{hour:02}:{minute:02}:{second:02}{fraction}"))
}

/// Returns the offset part of a canonical lexical form; empty for an unzoned value.
pub(super) fn offset_lexical(offset: Option<TimezoneOffset>) -> String {
    offset.map(|offset| offset.to_string()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::value::temporal::{Date, DateTime, Time};

    #[test]
    fn instants_count_from_year_one() {
        let unix_epoch =
            Instant::from_lexical::<LibraryDateTime>("1970-01-01T00:00:00Z").expect("valid");
        assert_eq!(
            unix_epoch.attoseconds(),
            UNIX_EPOCH_SECONDS * ATTOSECONDS_PER_SECOND
        );
        let first =
            Instant::from_lexical::<LibraryDateTime>("0001-01-01T00:00:00Z").expect("valid");
        assert_eq!(first.attoseconds(), 0);
    }

    #[test]
    fn the_reference_day_is_pinned() {
        let midnight = Time::from_lexical("00:00:00Z").expect("valid");
        assert_eq!(
            midnight.to_string(),
            DateTime::from_lexical("1972-12-31T00:00:00Z")
                .expect("valid")
                .time()
                .to_string()
        );
        let instant = Instant::from_lexical::<LibraryTime>("00:00:00Z").expect("valid");
        assert_eq!(
            instant.attoseconds(),
            TIME_REFERENCE_SECONDS * ATTOSECONDS_PER_SECOND
        );
    }

    #[test]
    fn the_unzoned_mark_collides_with_an_offset() {
        // `oxsdatatypes` writes an unzoned value and the offset `-00:01` with the same tail bytes.
        let unzoned: LibraryDateTime = "1970-01-01T00:00:00".parse().expect("valid");
        let shifted: LibraryDateTime = "1970-01-01T00:00:00-00:01".parse().expect("valid");
        assert_eq!(unzoned.to_be_bytes()[16..], shifted.to_be_bytes()[16..]);
        // The bridge tells them apart in both directions.
        for lexical in ["1970-01-01T00:00:00", "1970-01-01T00:00:00-00:01"] {
            let value = DateTime::from_lexical(lexical).expect("valid");
            assert_eq!(value.to_string(), lexical);
        }
        for lexical in ["1970-01-01", "1970-01-01-00:01"] {
            let value = Date::from_lexical(lexical).expect("valid");
            assert_eq!(value.to_string(), lexical);
        }
        for lexical in ["00:00:00", "00:00:00-00:01"] {
            let value = Time::from_lexical(lexical).expect("valid");
            assert_eq!(value.to_string(), lexical);
        }
        assert_ne!(
            DateTime::from_lexical("1970-01-01T00:00:00"),
            DateTime::from_lexical("1970-01-01T00:01:00-00:01")
        );
    }

    #[test]
    fn value_comparison_agrees_with_oxsdatatypes() {
        let datetime = |lexical: &str| DateTime::from_lexical(lexical).expect(lexical);
        let library = |lexical: &str| lexical.parse::<LibraryDateTime>().expect(lexical);
        let mut values = vec![
            (
                DateTime::from_instant(Instant::from_library(LibraryDateTime::MIN)),
                LibraryDateTime::MIN,
            ),
            (
                DateTime::from_instant(Instant::from_library(LibraryDateTime::MAX)),
                LibraryDateTime::MAX,
            ),
        ];
        let mut lexicals = vec![
            "2000-01-15T00:00:00".to_string(),
            "2000-02-15T00:00:00".to_string(),
            "2000-01-15T12:00:00".to_string(),
            "2000-01-16T12:00:00Z".to_string(),
            "2000-01-01T12:00:00".to_string(),
            "1999-12-31T23:00:00Z".to_string(),
            "2000-01-16T12:00:00".to_string(),
            "2000-01-16T00:00:00".to_string(),
            "-5391559471918-10-06T00:00:00".to_string(),
            "5391559471919-03-30T00:00:00".to_string(),
        ];
        for offset in [
            "-14:00", "-13:59", "-13:01", "Z", "+13:01", "+13:59", "+14:00",
        ] {
            for time in ["09:59:59", "10:00:00", "10:00:01", "22:00:00"] {
                lexicals.push(format!("2000-01-16T{time}{offset}"));
            }
            lexicals.push(format!("-5391559471918-10-06T00:00:00{offset}"));
            lexicals.push(format!("5391559471919-03-30T00:00:00{offset}"));
        }
        values.extend(
            lexicals
                .iter()
                .map(|lexical| (datetime(lexical), library(lexical))),
        );
        for (left, left_library) in &values {
            for (right, right_library) in &values {
                assert_eq!(
                    left.value_cmp(*right),
                    left_library.partial_cmp(right_library),
                    "{left} against {right}"
                );
                assert_eq!(
                    left.value_eq(*right),
                    left_library == right_library,
                    "{left} against {right}"
                );
            }
        }
        // The XSD 1.1 examples.
        let less = |left: &str, right: &str| {
            assert_eq!(
                datetime(left).value_cmp(datetime(right)),
                Some(Ordering::Less)
            );
        };
        let indeterminate = |left: &str, right: &str| {
            assert_eq!(datetime(left).value_cmp(datetime(right)), None);
        };
        less("2000-01-15T00:00:00", "2000-02-15T00:00:00");
        less("2000-01-15T12:00:00", "2000-01-16T12:00:00Z");
        indeterminate("2000-01-01T12:00:00", "1999-12-31T23:00:00Z");
        indeterminate("2000-01-16T12:00:00", "2000-01-16T12:00:00Z");
        indeterminate("2000-01-16T00:00:00", "2000-01-16T12:00:00Z");
    }
}
