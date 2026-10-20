//! Instants on the timeline with optional timezone offsets.

use std::{cmp::Ordering, fmt};

use oxsdatatypes::DateTime as LibraryDateTime;

use crate::value::temporal::{
    Date, Duration, Time, TimezoneOffset,
    instant::{self, Instant},
};

/// An [`xsd:dateTime`](https://www.w3.org/TR/xmlschema11-2/#dateTime) value:
/// an instant on the timeline and the timezone offset it was written with,
/// or no offset for a local time whose instant is known only to within fourteen hours.
///
/// Equality, order and hashing are term identity:
/// the same instant written in different offsets are distinct values.
/// The order is by instant, a local time read as UTC,
/// then by offset, with no offset last.
/// XSD's value comparison is [value_eq](Self::value_eq) and [value_cmp](Self::value_cmp).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTime(Instant);

/// The value takes 24 bytes at 8-byte alignment.
const _: () = assert!(size_of::<DateTime>() == 24 && align_of::<DateTime>() == 8);

impl DateTime {
    /// The attoseconds of a millisecond.
    const ATTOSECONDS_PER_MILLISECOND: i128 = 10i128.pow(15);

    /// The Unix epoch in attoseconds of the instant.
    const UNIX_EPOCH: i128 = instant::UNIX_EPOCH_SECONDS * instant::ATTOSECONDS_PER_SECOND;

    /// Creates the dateTime of an instant.
    pub(super) fn from_instant(instant: Instant) -> Self {
        Self(instant)
    }

    /// Parses an `xsd:dateTime` lexical form,
    /// or `None` for malformed input and for a value beyond the value space.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::{DateTime, TimezoneOffset};
    ///
    /// let value = DateTime::from_lexical("2024-03-01T12:30:00.5+05:30").unwrap();
    /// assert_eq!((value.year(), value.month(), value.day()), (2024, 3, 1));
    /// assert_eq!((value.hour(), value.minute(), value.second()), (12, 30, 0));
    /// assert_eq!(value.attosecond(), 500_000_000_000_000_000);
    /// assert_eq!(value.offset(), TimezoneOffset::new(330));
    ///
    /// let midnight = DateTime::from_lexical("2024-02-29T24:00:00").unwrap();
    /// assert_eq!(midnight.to_string(), "2024-03-01T00:00:00");
    /// assert_eq!(DateTime::from_lexical("2024-02-30T00:00:00"), None);
    /// ```
    pub fn from_lexical(lexical: &str) -> Option<Self> {
        Instant::from_lexical::<LibraryDateTime>(lexical).map(Self)
    }

    /// Creates the dateTime of calendar components,
    /// or `None` for components that do not name a day or a time of day,
    /// or for a value beyond the value space.
    ///
    /// The hour `24` with a zero minute, second and fraction is the start of the next day.
    #[allow(clippy::too_many_arguments)]
    pub fn from_components(
        year: i64,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        attosecond: u64,
        offset: Option<TimezoneOffset>,
    ) -> Option<Self> {
        let lexical = format!(
            "{}T{}{}",
            instant::date_lexical(year, month, day),
            instant::time_lexical(hour, minute, second, attosecond)?,
            instant::offset_lexical(offset)
        );
        Self::from_lexical(&lexical)
    }

    /// Creates the dateTime of milliseconds since the Unix epoch, UTC, and a timezone offset.
    pub fn from_unix_millis(millis: i64, offset: Option<TimezoneOffset>) -> Self {
        Self(Instant::new(
            Self::UNIX_EPOCH + i128::from(millis) * Self::ATTOSECONDS_PER_MILLISECOND,
            offset,
        ))
    }

    /// Milliseconds since the Unix epoch, UTC,
    /// or `None` if the instant is finer than a millisecond or beyond the 64-bit range.
    pub fn unix_millis(self) -> Option<i64> {
        let since_unix_epoch = self.0.attoseconds().checked_sub(Self::UNIX_EPOCH)?;
        (since_unix_epoch % Self::ATTOSECONDS_PER_MILLISECOND == 0)
            .then(|| i64::try_from(since_unix_epoch / Self::ATTOSECONDS_PER_MILLISECOND).ok())
            .flatten()
    }

    /// Returns the year in the value's own offset, `fn:year-from-dateTime`,
    /// which is negative before year one.
    pub fn year(self) -> i64 {
        self.0.to_library::<LibraryDateTime>().year()
    }

    /// Returns the month of the year in the value's own offset, `fn:month-from-dateTime`.
    pub fn month(self) -> u8 {
        self.0.to_library::<LibraryDateTime>().month()
    }

    /// Returns the day of the month in the value's own offset, `fn:day-from-dateTime`.
    pub fn day(self) -> u8 {
        self.0.to_library::<LibraryDateTime>().day()
    }

    /// Returns the hour of the day in the value's own offset, `fn:hours-from-dateTime`.
    pub fn hour(self) -> u8 {
        self.0.hour()
    }

    /// Returns the minute of the hour, `fn:minutes-from-dateTime`.
    pub fn minute(self) -> u8 {
        self.0.minute()
    }

    /// Returns the whole second of the minute.
    /// With [attosecond](Self::attosecond), it gives `fn:seconds-from-dateTime`.
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

    /// Returns the date of the value's day, in the value's offset,
    /// or `None` if the day's start is beyond the value space.
    pub fn date(self) -> Option<Date> {
        self.0.day_start().map(Date::from_instant)
    }

    /// Returns the time of day of the value, in the value's offset.
    pub fn time(self) -> Time {
        Time::from_instant(self.0.time_of_day())
    }

    /// Returns whether two values are equal in XSD's value space:
    /// both zoned or both unzoned, and at the same instant.
    pub fn value_eq(self, other: Self) -> bool {
        self.0.value_eq(other.0)
    }

    /// Returns the order of two values in XSD's value space,
    /// or `None` for a zoned and an unzoned value within fourteen hours of each other.
    pub fn value_cmp(self, other: Self) -> Option<Ordering> {
        self.0.value_cmp(other.0)
    }

    /// Returns the value adjusted to another offset, `fn:adjust-dateTime-to-timezone`,
    /// or `None` beyond the value space.
    ///
    /// Given an offset, a zoned value keeps its instant and reads it in the new offset,
    /// and an unzoned value keeps its local fields and takes the offset.
    /// Given `None`, the value keeps its local fields and drops its offset.
    pub fn adjust(self, offset: Option<TimezoneOffset>) -> Option<Self> {
        self.0.adjust::<LibraryDateTime>(offset).map(Self)
    }

    /// Returns the value a duration later,
    /// `op:add-yearMonthDuration-to-dateTime` and `op:add-dayTimeDuration-to-dateTime`,
    /// or `None` beyond the value space.
    ///
    /// The months are added first, clamping the day to the month's length,
    /// then the seconds.
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add::<LibraryDateTime>(duration).map(Self)
    }

    /// Returns the value a duration earlier,
    /// `op:subtract-yearMonthDuration-from-dateTime` and `op:subtract-dayTimeDuration-from-dateTime`,
    /// or `None` beyond the value space.
    pub fn checked_sub(self, duration: Duration) -> Option<Self> {
        self.0.checked_sub::<LibraryDateTime>(duration).map(Self)
    }

    /// Returns the time from an earlier value to this one, `op:subtract-dateTimes`,
    /// or `None` if one value is zoned and the other not.
    ///
    /// The result has no months, and it is negative if the other value is later.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::DateTime;
    ///
    /// let later = DateTime::from_lexical("2000-10-30T06:12:00-05:00").unwrap();
    /// let earlier = DateTime::from_lexical("1999-11-28T09:00:00Z").unwrap();
    /// assert_eq!(later.duration_since(earlier).unwrap().to_string(), "P337DT2H12M");
    /// ```
    pub fn duration_since(self, earlier: Self) -> Option<Duration> {
        self.0.duration_since::<LibraryDateTime>(earlier.0)
    }
}

/// The hooks of the storage encoding.
impl DateTime {
    /// The number of offset codes, the unzoned mark included.
    pub(crate) const OFFSET_CODE_COUNT: u64 = instant::OFFSET_CODE_COUNT;

    /// Creates the dateTime of milliseconds since the Unix epoch, UTC, and a raw offset code,
    /// or `None` if the code is not one.
    pub(crate) fn from_millis_and_code(millis: i64, offset_code: u64) -> Option<Self> {
        Instant::from_code(
            Self::UNIX_EPOCH + i128::from(millis) * Self::ATTOSECONDS_PER_MILLISECOND,
            offset_code,
        )
        .map(Self)
    }

    /// Returns the raw offset code, below [OFFSET_CODE_COUNT](Self::OFFSET_CODE_COUNT).
    pub(crate) fn offset_code(self) -> u64 {
        self.0.offset_code()
    }
}

impl fmt::Debug for DateTime {
    /// Writes the value in its canonical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for DateTime {
    /// Writes the canonical `xsd:dateTime` lexical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.to_library::<LibraryDateTime>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        testing::zoned,
        value::{DataValue, ValueRef},
    };

    /// Parses the dateTime value of a lexical.
    fn datetime(lexical: &str) -> DataValue {
        DataValue::datetime_from_lexical(lexical).expect(lexical)
    }

    /// Returns the dateTime of a value.
    fn inner(value: &DataValue) -> DateTime {
        match value.as_ref() {
            ValueRef::DateTime(value) => value,
            other => panic!("{other:?} is not a dateTime"),
        }
    }

    #[test]
    fn values_round_trip_through_oxsdatatypes() {
        let instant = |lexical: &str| DateTime::from_lexical(lexical).expect("valid");
        assert_eq!(
            instant("1970-01-01T00:00:00Z"),
            DateTime::from_unix_millis(0, zoned(0))
        );
        assert_eq!(
            instant("1970-01-01T01:00:00.5+01:00"),
            DateTime::from_unix_millis(500, zoned(60))
        );
        assert_eq!(
            instant("1970-01-01T00:00:00"),
            DateTime::from_unix_millis(0, None)
        );
        // The offset whose `oxsdatatypes` serialization coincides with unzoned stays its own.
        assert_eq!(
            instant("1970-01-01T00:00:00-00:01"),
            DateTime::from_unix_millis(60_000, zoned(-1))
        );

        for lexical in [
            "1970-01-01T00:00:00Z",
            "1970-01-01T01:00:00.5+01:00",
            "1970-01-01T00:00:00",
            "1970-01-01T00:00:00-00:01",
            "-13798000000-01-01T00:00:00.000000000000000001-14:00",
        ] {
            let value: LibraryDateTime = lexical.parse().expect("valid");
            let back = Instant::from_library(value).to_library::<LibraryDateTime>();
            assert!(back.is_identical_with(value), "{lexical}: {back}");
            assert_eq!(back.to_string(), lexical);
        }
    }

    #[test]
    fn canonical_lexicals_round_trip() {
        for lexical in [
            "2024-03-01T12:30:00Z",
            "2024-03-01T12:30:00.25+05:30",
            "2024-03-01T12:30:00.001-14:00",
            "2024-03-01T12:30:00",
            "-0044-03-15T09:00:00Z",
            "0000-01-01T00:00:00Z",
            "12345-06-07T08:09:10",
            "2024-02-29T23:59:59.999Z",
            "2024-03-01T12:30:00.1234Z",
            "2024-03-01T12:30:00.000000001+05:30",
            "2024-03-01T12:30:00.000000000000000001Z",
            "-13798000000-01-01T00:00:00Z",
        ] {
            assert_eq!(
                datetime(lexical).as_ref().to_string(),
                format!("\"{lexical}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>")
            );
            assert_eq!(inner(&datetime(lexical)).to_string(), lexical);
        }
    }

    #[test]
    fn lexicals_normalize() {
        let normalized = |lexical: &str| inner(&datetime(lexical)).to_string();

        // The end of a day is the start of the next.
        assert_eq!(normalized("1999-12-31T24:00:00"), "2000-01-01T00:00:00");

        // Trailing zeros of the fraction, and `+00:00` as `Z`.
        assert_eq!(
            normalized("2024-01-01T00:00:00.100+00:00"),
            "2024-01-01T00:00:00.1Z"
        );
        assert_eq!(normalized("2024-01-01T00:00:00.000"), "2024-01-01T00:00:00");

        // The instant is the same, the offset written as given.
        let utc = inner(&datetime("2024-01-01T00:00:00Z"));
        let shifted = inner(&datetime("2024-01-01T02:00:00+02:00"));
        assert!(utc.value_eq(shifted));
        assert_eq!(shifted.hour(), 2);
    }

    #[test]
    fn millisecond_instants_round_trip() {
        // Trailing zeros of the fraction do not make a finer instant.
        assert_eq!(
            datetime("2024-01-01T00:00:00.1230Z"),
            datetime("2024-01-01T00:00:00.123Z")
        );
        assert_ne!(
            datetime("2024-01-01T00:00:00.123Z"),
            datetime("2024-01-01T00:00:00.1230001Z")
        );
        assert_eq!(
            inner(&datetime("2024-01-01T00:00:00.1234Z")).unix_millis(),
            None
        );

        // A dateTime and its milliseconds are one value.
        let value = DateTime::from_unix_millis(1_250, zoned(0));
        assert_eq!(value, inner(&datetime("1970-01-01T00:00:01.25Z")));
        assert_eq!(value.unix_millis(), Some(1_250));
        assert_eq!(value.offset(), zoned(0));

        // The whole 64-bit range of milliseconds is within the value space.
        for millis in [i64::MIN, i64::MAX] {
            let far = DateTime::from_unix_millis(millis, None);
            assert_eq!(far.unix_millis(), Some(millis));
            assert_eq!(far, inner(&datetime(&far.to_string())));
        }
    }

    #[test]
    fn the_oxsdatatypes_extremes_are_values() {
        // The `oxsdatatypes` range reaches close to the limits of an `i128`,
        // so the representation must not shift the instant.
        let extremes = [LibraryDateTime::MIN, LibraryDateTime::MAX];
        for library in extremes {
            let value = DateTime(Instant::from_library(library));
            assert!(
                value
                    .0
                    .to_library::<LibraryDateTime>()
                    .is_identical_with(library)
            );
            let owned = DataValue::datetime(value);
            assert_eq!(inner(&owned), value);
            assert_eq!(inner(&owned).to_string(), library.to_string());
            assert_eq!(value.unix_millis(), None);
        }

        let epoch = inner(&datetime("1970-01-01T00:00:00Z"));
        assert!(DateTime(Instant::from_library(extremes[0])) < epoch);
        assert!(epoch < DateTime(Instant::from_library(extremes[1])));

        // Unzoned values near the extremes rebuild through the same path.
        for lexical in [
            "-5391559471918-10-06T00:00:00",
            "5391559471919-03-30T00:00:00",
        ] {
            let value = inner(&datetime(lexical));
            assert_eq!(value.to_string(), lexical);
        }
    }

    #[test]
    fn malformed_lexicals_are_refused() {
        for lexical in [
            "",
            "2024-01-01",
            "2024-01-01T00:00",
            "2024-1-1T00:00:00Z",
            "+2024-01-01T00:00:00Z",
            "02024-01-01T00:00:00Z",
            "024-01-01T00:00:00Z",
            "2024-13-01T00:00:00Z",
            "2024-00-01T00:00:00Z",
            "2024-04-31T00:00:00Z",
            "2023-02-29T00:00:00Z",
            "1900-02-29T00:00:00Z",
            "2024-01-01T24:00:01Z",
            "2024-01-01T25:00:00Z",
            "2024-01-01T00:60:00Z",
            "2024-01-01T00:00:60Z",
            "2024-01-01T00:00:+1Z",
            "2024-01-01T00:00:00.Z",
            "2024-01-01T00:00:00.12a4Z",
            "2024-01-01t00:00:00Z",
            "2024-01-01T00:00:00z",
            "2024-01-01T00:00:00+14:30",
            "2024-01-01T00:00:00+1:00",
            "2024-01-01T00:00:00+01:60",
            "2024-01-01T00:00:00 Z",
            // Finer than the value space's precision.
            "2024-01-01T00:00:00.0000000000000000001Z",
            // Beyond the range of instants.
            "9223372036854775807-12-31T23:59:59Z",
        ] {
            assert!(DateTime::from_lexical(lexical).is_none(), "{lexical}");
        }

        // Leap days exist where the calendar has them.
        assert!(DateTime::from_lexical("2024-02-29T00:00:00Z").is_some());
        assert!(DateTime::from_lexical("2000-02-29T00:00:00Z").is_some());
    }

    #[test]
    fn components_build_and_read_values() {
        let value =
            DateTime::from_components(2024, 3, 1, 12, 30, 5, 250_000_000_000_000, zoned(330))
                .expect("valid");
        assert_eq!(value, inner(&datetime("2024-03-01T12:30:05.00025+05:30")));
        assert_eq!((value.year(), value.month(), value.day()), (2024, 3, 1));
        assert_eq!((value.hour(), value.minute(), value.second()), (12, 30, 5));
        assert_eq!(value.attosecond(), 250_000_000_000_000);
        assert_eq!(value.offset(), zoned(330));
        // Negative years, the hour `24`, and the local fields of a westward offset.
        assert_eq!(
            DateTime::from_components(-44, 3, 15, 9, 0, 0, 0, zoned(0)).expect("valid"),
            inner(&datetime("-0044-03-15T09:00:00Z"))
        );
        assert_eq!(
            DateTime::from_components(1999, 12, 31, 24, 0, 0, 0, None).expect("valid"),
            inner(&datetime("2000-01-01T00:00:00"))
        );
        let west = inner(&datetime("2024-01-01T00:30:00-14:00"));
        assert_eq!((west.year(), west.month(), west.day()), (2024, 1, 1));
        assert_eq!((west.hour(), west.minute()), (0, 30));
        for (year, month, day, hour, minute, second, attosecond) in [
            (2023, 2, 29, 0, 0, 0, 0),
            (2024, 13, 1, 0, 0, 0, 0),
            (2024, 1, 1, 24, 0, 1, 0),
            (2024, 1, 1, 0, 60, 0, 0),
            (2024, 1, 1, 0, 0, 60, 0),
            (2024, 1, 1, 0, 0, 0, 10u64.pow(18)),
            (i64::MAX, 1, 1, 0, 0, 0, 0),
        ] {
            assert_eq!(
                DateTime::from_components(year, month, day, hour, minute, second, attosecond, None),
                None,
                "{year}-{month}-{day}T{hour}:{minute}:{second}.{attosecond}"
            );
        }
    }

    #[test]
    fn dates_and_times_split_off_and_join() {
        let value = inner(&datetime("1999-12-31T12:00:00.5+05:30"));
        assert_eq!(
            value.date(),
            Some(Date::from_lexical("1999-12-31+05:30").expect("valid"))
        );
        assert_eq!(
            value.time(),
            Time::from_lexical("12:00:00.5+05:30").expect("valid")
        );

        let unzoned = inner(&datetime("1999-12-31T24:00:00"));
        assert_eq!(unzoned.date(), Date::from_lexical("2000-01-01"));
        assert_eq!(
            unzoned.time(),
            Time::from_lexical("00:00:00").expect("valid")
        );

        // A westward offset keeps its local day.
        let west = inner(&datetime("2024-01-01T00:30:00-14:00"));
        assert_eq!(west.date(), Date::from_lexical("2024-01-01-14:00"));
        assert_eq!(
            west.time(),
            Time::from_lexical("00:30:00-14:00").expect("valid")
        );
        assert_eq!(west.date().expect("in range").at(west.time()), Some(west));
    }

    #[test]
    fn adjusting_follows_xpath() {
        let adjusted = |lexical: &str, offset: Option<TimezoneOffset>| {
            inner(&datetime(lexical))
                .adjust(offset)
                .expect("in range")
                .to_string()
        };
        assert_eq!(
            adjusted("2002-03-07T10:00:00-07:00", zoned(-600)),
            "2002-03-07T07:00:00-10:00"
        );
        assert_eq!(
            adjusted("2002-03-07T00:00:00+01:00", zoned(-480)),
            "2002-03-06T15:00:00-08:00"
        );
        assert_eq!(
            adjusted("2002-03-07T10:00:00", zoned(-600)),
            "2002-03-07T10:00:00-10:00"
        );
        assert_eq!(
            adjusted("2002-03-07T10:00:00-07:00", None),
            "2002-03-07T10:00:00"
        );
        assert_eq!(adjusted("2002-03-07T10:00:00", None), "2002-03-07T10:00:00");
    }

    #[test]
    fn arithmetic_follows_xpath() {
        let value = |lexical: &str| inner(&datetime(lexical));
        let duration = |lexical: &str| Duration::from_lexical(lexical).expect(lexical);
        let base = value("2000-10-30T11:12:00");

        assert_eq!(
            base.checked_add(duration("P1Y2M")).expect("in range"),
            value("2001-12-30T11:12:00")
        );
        assert_eq!(
            base.checked_add(duration("P3DT1H15M")).expect("in range"),
            value("2000-11-02T12:27:00")
        );
        assert_eq!(
            base.checked_sub(duration("P1Y2M")).expect("in range"),
            value("1999-08-30T11:12:00")
        );
        assert_eq!(
            base.checked_sub(duration("P3DT1H15M")).expect("in range"),
            value("2000-10-27T09:57:00")
        );

        // The day clamps to the month's length.
        assert_eq!(
            value("2024-01-31T00:00:00Z")
                .checked_add(duration("P1M"))
                .expect("in range"),
            value("2024-02-29T00:00:00Z")
        );
        assert_eq!(
            value("2000-10-30T06:12:00-05:00").duration_since(value("1999-11-28T09:00:00Z")),
            Some(duration("P337DT2H12M"))
        );
        assert_eq!(
            value("1999-11-28T09:00:00Z").duration_since(value("2000-10-30T06:12:00-05:00")),
            Some(duration("-P337DT2H12M"))
        );
        assert_eq!(
            value("2000-10-30T06:12:00").duration_since(value("1999-11-28T09:00:00Z")),
            None
        );
        let far = DateTime(Instant::from_library(LibraryDateTime::MAX));
        assert_eq!(far.checked_add(duration("P1D")), None);
        assert_eq!(far.checked_add(duration("P1Y")), None);
    }

    #[test]
    fn offset_codes_round_trip() {
        for offset in [None, zoned(0), zoned(-840), zoned(840), zoned(90)] {
            let value = DateTime::from_unix_millis(0, offset);
            assert_eq!(
                DateTime::from_millis_and_code(0, value.offset_code()),
                Some(value)
            );
            assert_eq!(value.offset(), offset);
        }
        assert_eq!(
            DateTime::from_millis_and_code(0, DateTime::OFFSET_CODE_COUNT),
            None
        );
        assert_eq!(DateTime::from_millis_and_code(0, u64::MAX), None);
    }

    #[test]
    fn order_is_by_instant_with_unzoned_last() {
        let ordered = [
            "-13798000000-01-01T00:00:00Z",
            "1969-12-31T23:59:59",
            "1969-12-31T23:59:59.9999Z",
            "1969-12-31T10:00:00-14:00",
            "1970-01-01T00:00:00Z",
            "1970-01-01T14:00:00+14:00",
            "1970-01-01T00:00:00",
            "1970-01-01T00:00:00.0001Z",
            "1970-01-01T00:00:00.001Z",
            "1970-01-01T00:00:00.0011Z",
            "1970-01-01T00:00:00.002Z",
            "1970-01-01T00:00:01",
            "71000-01-01T00:00:00Z",
            "1000000000000-01-01T00:00:00Z",
        ]
        .map(datetime);
        for window in ordered.windows(2) {
            assert!(window[0] < window[1], "{} < {}", window[0], window[1]);
            assert_eq!(window[0].cmp(&window[0]), Ordering::Equal);
        }
    }

    #[test]
    fn identity_includes_the_offset() {
        // The same instant written with different offsets:
        // equal but not identical in XSD 1.1, hence distinct terms.
        let utc = datetime("1970-01-01T00:00:00Z");
        let shifted = datetime("1970-01-01T01:00:00+01:00");
        assert_ne!(utc, shifted);
        assert!(inner(&utc).value_eq(inner(&shifted)));
        // `Z` and `+00:00` are one offset; `-00:01` and no offset are not.
        assert_eq!(utc, datetime("1970-01-01T00:00:00+00:00"));
        assert_ne!(
            datetime("1970-01-01T00:00:00-00:01"),
            datetime("1970-01-01T00:01:00")
        );

        // The same holds beyond the compact range.
        let far = datetime("-13798000000-01-01T00:00:00Z");
        let far_shifted = datetime("-13798000000-01-01T01:00:00+01:00");
        assert_ne!(far, far_shifted);
        assert!(inner(&far).value_eq(inner(&far_shifted)));
    }
}
