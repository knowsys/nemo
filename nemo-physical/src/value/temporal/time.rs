//! Times of day with optional timezone offsets.

use std::{cmp::Ordering, fmt};

use oxsdatatypes::Time as LibraryTime;

use crate::value::temporal::{
    Duration, TimezoneOffset,
    instant::{self, Instant},
};

/// An [`xsd:time`](https://www.w3.org/TR/xmlschema11-2/#time) value:
/// a time of day and the timezone offset it was written with, or no offset for a local time.
///
/// Equality, order and hashing are term identity:
/// the same time in different offsets are distinct values.
/// The order is by the instant of the time in its offset on the reference day `1972-12-31`,
/// a local time read as UTC, then by offset, with no offset last.
/// The reference day is the date
/// [XSD fills in](https://www.w3.org/TR/xmlschema11-2/#theSevenPropertyModel)
/// for a value without one.
/// XSD's value comparison is [value_eq](Self::value_eq) and [value_cmp](Self::value_cmp).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(Instant);

/// The value takes 24 bytes at 8-byte alignment.
const _: () = assert!(size_of::<Time>() == 24 && align_of::<Time>() == 8);

impl Time {
    /// Creates the time of an instant on the reference day.
    pub(super) fn from_instant(instant: Instant) -> Self {
        Self(instant)
    }

    /// Returns the instant of the time on the reference day.
    pub(super) fn instant(self) -> Instant {
        self.0
    }

    /// Parses an `xsd:time` lexical form,
    /// or `None` for malformed input.
    ///
    /// `24:00:00` is the time `00:00:00`.
    pub fn from_lexical(lexical: &str) -> Option<Self> {
        Instant::from_lexical::<LibraryTime>(lexical).map(Self)
    }

    /// Creates the time of clock components,
    /// or `None` for components that do not name a time of day.
    ///
    /// The hour `24` with a zero minute, second and fraction is the time `00:00:00`.
    pub fn from_components(
        hour: u8,
        minute: u8,
        second: u8,
        attosecond: u64,
        offset: Option<TimezoneOffset>,
    ) -> Option<Self> {
        let lexical = format!(
            "{}{}",
            instant::time_lexical(hour, minute, second, attosecond)?,
            instant::offset_lexical(offset)
        );
        Self::from_lexical(&lexical)
    }

    /// Returns the hour of the day, `fn:hours-from-time`.
    pub fn hour(self) -> u8 {
        self.0.hour()
    }

    /// Returns the minute of the hour, `fn:minutes-from-time`.
    pub fn minute(self) -> u8 {
        self.0.minute()
    }

    /// Returns the whole second of the minute.
    /// With [attosecond](Self::attosecond), it gives `fn:seconds-from-time`.
    pub fn second(self) -> u8 {
        self.0.second()
    }

    /// Returns the fraction of the second in attoseconds.
    pub fn attosecond(self) -> u64 {
        self.0.attosecond()
    }

    /// Returns the timezone offset the value was written with, or `None` for an unzoned value.
    pub fn offset(self) -> Option<TimezoneOffset> {
        self.0.offset()
    }

    /// Returns whether two values are equal in XSD's value space:
    /// both zoned or both unzoned, and at the same instant of the reference day.
    pub fn value_eq(self, other: Self) -> bool {
        self.0.value_eq(other.0)
    }

    /// Returns the order of two values in XSD's value space,
    /// or `None` for a zoned and an unzoned value within fourteen hours of each other.
    pub fn value_cmp(self, other: Self) -> Option<Ordering> {
        self.0.value_cmp(other.0)
    }

    /// Returns the value adjusted to another offset, `fn:adjust-time-to-timezone`.
    ///
    /// Given an offset, a zoned value keeps its instant and reads it in the new offset,
    /// wrapping around the day as needed,
    /// and an unzoned value keeps its clock fields and takes the offset.
    /// Given `None`, the value keeps its clock fields and drops its offset.
    pub fn adjust(self, offset: Option<TimezoneOffset>) -> Option<Self> {
        self.0.adjust::<LibraryTime>(offset).map(Self)
    }

    /// Returns the value a duration later, `op:add-dayTimeDuration-to-time`,
    /// wrapping around the day, or `None` for a duration with months.
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        duration
            .is_day_time()
            .then(|| self.0.checked_add::<LibraryTime>(duration))
            .flatten()
            .map(Self)
    }

    /// Returns the value a duration earlier, `op:subtract-dayTimeDuration-from-time`,
    /// wrapping around the day, or `None` for a duration with months.
    pub fn checked_sub(self, duration: Duration) -> Option<Self> {
        duration
            .is_day_time()
            .then(|| self.0.checked_sub::<LibraryTime>(duration))
            .flatten()
            .map(Self)
    }

    /// Returns the time from an earlier value to this one on the reference day, `op:subtract-times`,
    /// or `None` if one value is zoned and the other not.
    ///
    /// The result is negative when the other value is later in the day.
    pub fn duration_since(self, earlier: Self) -> Option<Duration> {
        self.0.duration_since::<LibraryTime>(earlier.0)
    }
}

/// The hooks of the compact representations.
impl Time {
    /// The number of offset codes, the unzoned mark included.
    pub(crate) const OFFSET_CODE_COUNT: u64 = instant::OFFSET_CODE_COUNT;

    /// The attoseconds of a nanosecond.
    const ATTOSECONDS_PER_NANOSECOND: i128 = 10i128.pow(9);

    /// The start of the reference day in attoseconds of the instant.
    const REFERENCE_DAY: i128 = instant::TIME_REFERENCE_SECONDS * instant::ATTOSECONDS_PER_SECOND;

    /// Creates the time a number of nanoseconds after the start of the reference day, UTC,
    /// written with a timezone offset.
    pub(crate) fn from_nanos(nanos: i64, offset: Option<TimezoneOffset>) -> Self {
        Self(Instant::new(Self::attoseconds(nanos), offset))
    }

    /// Creates the time a number of nanoseconds after the start of the reference day, UTC,
    /// written with the offset of a raw code, or `None` if the code is not one.
    pub(crate) fn from_nanos_and_code(nanos: i64, offset_code: u64) -> Option<Self> {
        Instant::from_code(Self::attoseconds(nanos), offset_code).map(Self)
    }

    /// Returns the nanoseconds from the start of the reference day, UTC, to the instant,
    /// or `None` if the instant is finer than a nanosecond.
    ///
    /// The instant lies within fourteen hours of the reference day,
    /// so the count is within a few days' nanoseconds either way.
    pub(crate) fn nanos(self) -> Option<i64> {
        let since_reference = self.0.attoseconds() - Self::REFERENCE_DAY;
        (since_reference % Self::ATTOSECONDS_PER_NANOSECOND == 0).then(|| {
            i64::try_from(since_reference / Self::ATTOSECONDS_PER_NANOSECOND)
                .expect("a time lies within fourteen hours of the reference day")
        })
    }

    /// Returns the raw offset code, below [OFFSET_CODE_COUNT](Self::OFFSET_CODE_COUNT).
    pub(crate) fn offset_code(self) -> u64 {
        self.0.offset_code()
    }

    /// Returns the attoseconds of the instant a number of nanoseconds after the reference day starts.
    fn attoseconds(nanos: i64) -> i128 {
        Self::REFERENCE_DAY + i128::from(nanos) * Self::ATTOSECONDS_PER_NANOSECOND
    }
}

impl fmt::Debug for Time {
    /// Writes the value in its canonical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for Time {
    /// Writes the canonical `xsd:time` lexical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.to_library::<LibraryTime>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        testing::zoned,
        value::temporal::instant::{
            ATTOSECONDS_PER_SECOND, SECONDS_PER_DAY, TIME_REFERENCE_SECONDS,
        },
    };

    /// Parses the time of a lexical; panics on malformed input.
    fn time(lexical: &str) -> Time {
        Time::from_lexical(lexical).expect(lexical)
    }

    /// Asserts that a time lies on the reference day in its own offset.
    fn assert_on_reference_day(value: Time) {
        let local = value.0.local_seconds();
        assert!(
            (TIME_REFERENCE_SECONDS..TIME_REFERENCE_SECONDS + SECONDS_PER_DAY).contains(&local),
            "{value} is off the reference day"
        );
    }

    #[test]
    fn canonical_lexicals_round_trip() {
        for lexical in [
            "00:00:00",
            "00:00:00Z",
            "12:30:05.25+05:30",
            "23:59:59.999999999999999999-14:00",
            "10:00:00-00:01",
        ] {
            assert_eq!(time(lexical).to_string(), lexical);
            assert_eq!(format!("{:?}", time(lexical)), lexical);
            assert_on_reference_day(time(lexical));
        }
        assert_eq!(time("24:00:00").to_string(), "00:00:00");
        assert_eq!(time("24:00:00Z"), time("00:00:00Z"));
        assert_eq!(time("12:00:00.100+00:00").to_string(), "12:00:00.1Z");
        assert_eq!(
            time("00:00:00Z").0.attoseconds(),
            TIME_REFERENCE_SECONDS * ATTOSECONDS_PER_SECOND
        );
    }

    #[test]
    fn malformed_lexicals_are_refused() {
        for lexical in [
            "",
            "10:00",
            "24:00:01",
            "25:00:00",
            "10:60:00",
            "10:00:60",
            "1:00:00",
            "10:00:00.",
            "10:00:00+14:30",
            "10:00:00 Z",
            "2024-01-01T10:00:00",
            "10:00:00.0000000000000000001",
        ] {
            assert_eq!(Time::from_lexical(lexical), None, "{lexical}");
        }
    }

    #[test]
    fn components_build_and_read_values() {
        let value =
            Time::from_components(12, 30, 5, 250_000_000_000_000, zoned(330)).expect("valid");
        assert_eq!(value, time("12:30:05.00025+05:30"));
        assert_eq!((value.hour(), value.minute(), value.second()), (12, 30, 5));
        assert_eq!(value.attosecond(), 250_000_000_000_000);
        assert_eq!(value.offset(), zoned(330));
        assert_eq!(
            Time::from_components(24, 0, 0, 0, None).expect("valid"),
            time("00:00:00")
        );
        let west = time("00:30:00-14:00");
        assert_eq!((west.hour(), west.minute()), (0, 30));
        assert_on_reference_day(west);
        let east = time("23:30:00+14:00");
        assert_eq!((east.hour(), east.minute()), (23, 30));
        assert_on_reference_day(east);
        for (hour, minute, second, attosecond) in [
            (24, 0, 1, 0),
            (24, 0, 0, 1),
            (0, 60, 0, 0),
            (0, 0, 60, 0),
            (0, 0, 0, 10u64.pow(18)),
        ] {
            assert_eq!(
                Time::from_components(hour, minute, second, attosecond, None),
                None,
                "{hour}:{minute}:{second}.{attosecond}"
            );
        }
    }

    #[test]
    fn value_comparison_follows_xsd() {
        assert!(time("10:00:00Z").value_eq(time("12:00:00+02:00")));
        assert!(!time("10:00:00Z").value_eq(time("10:00:00")));
        assert_eq!(
            time("10:00:00").value_cmp(time("10:00:01")),
            Some(Ordering::Less)
        );
        assert_eq!(
            time("12:00:00+02:00").value_cmp(time("10:00:00Z")),
            Some(Ordering::Equal)
        );
        assert_eq!(time("10:00:00").value_cmp(time("10:00:00Z")), None);
        assert_eq!(
            time("00:00:00").value_cmp(time("23:59:59-14:00")),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn order_is_by_instant_with_unzoned_last() {
        let ordered = [
            "00:00:00+14:00",
            "00:00:00",
            "10:00:00Z",
            "12:00:00+02:00",
            "10:00:00",
            "23:59:59.999-14:00",
        ]
        .map(time);
        for window in ordered.windows(2) {
            assert!(window[0] < window[1], "{} < {}", window[0], window[1]);
        }
    }

    #[test]
    fn adjusting_follows_xpath() {
        let adjusted = |lexical: &str, offset: Option<TimezoneOffset>| {
            let value = time(lexical).adjust(offset).expect("in range");
            assert_on_reference_day(value);
            value.to_string()
        };
        assert_eq!(adjusted("10:00:00-07:00", zoned(-600)), "07:00:00-10:00");
        assert_eq!(adjusted("10:00:00-07:00", zoned(600)), "03:00:00+10:00");
        assert_eq!(adjusted("10:00:00", zoned(-600)), "10:00:00-10:00");
        assert_eq!(adjusted("10:00:00-07:00", None), "10:00:00");
        assert_eq!(adjusted("10:00:00", None), "10:00:00");
    }

    #[test]
    fn arithmetic_follows_xpath() {
        let duration = |lexical: &str| Duration::from_lexical(lexical).expect(lexical);
        let sum = time("23:12:00+03:00")
            .checked_add(duration("P1DT3H15M"))
            .expect("day-time");
        assert_eq!(sum, time("02:27:00+03:00"));
        assert_on_reference_day(sum);
        let difference = time("08:20:00-05:00")
            .checked_sub(duration("P23DT10H10M"))
            .expect("day-time");
        assert_eq!(difference, time("22:10:00-05:00"));
        assert_on_reference_day(difference);
        assert_eq!(time("10:00:00").checked_add(duration("P1M")), None);
        assert_eq!(time("10:00:00").checked_sub(duration("P1Y")), None);
        assert_eq!(
            time("17:00:00-06:00").duration_since(time("08:00:00+09:00")),
            Some(duration("P1D"))
        );
        assert_eq!(
            time("24:00:00").duration_since(time("23:59:59")),
            Some(duration("-PT23H59M59S"))
        );
        assert_eq!(time("10:00:00").duration_since(time("10:00:00Z")), None);
    }
}
