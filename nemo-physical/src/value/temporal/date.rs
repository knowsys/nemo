//! Calendar days with optional timezone offsets.

use std::{cmp::Ordering, fmt};

use oxsdatatypes::Date as LibraryDate;

use crate::value::temporal::{
    DateTime, Duration, Time, TimezoneOffset,
    instant::{self, Instant},
};

/// An [`xsd:date`](https://www.w3.org/TR/xmlschema11-2/#date) value:
/// a calendar day and the timezone offset it was written with, or no offset for a local day.
///
/// Equality, order and hashing are term identity:
/// the same day in different offsets are distinct values.
/// The order is by the instant the day starts, a local day read as UTC,
/// then by offset, with no offset last.
/// XSD's value comparison is [value_eq](Self::value_eq) and [value_cmp](Self::value_cmp).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date(Instant);

/// The value takes 24 bytes at 8-byte alignment.
const _: () = assert!(size_of::<Date>() == 24 && align_of::<Date>() == 8);

impl Date {
    /// Creates the date of the instant a day starts.
    pub(super) fn from_instant(instant: Instant) -> Self {
        Self(instant)
    }

    /// Parses an `xsd:date` lexical form,
    /// or `None` for malformed input and for a value beyond the value space.
    pub fn from_lexical(lexical: &str) -> Option<Self> {
        Instant::from_lexical::<LibraryDate>(lexical).map(Self)
    }

    /// Creates the date of calendar components,
    /// or `None` for components that do not name a day or for a value beyond the value space.
    pub fn from_components(
        year: i64,
        month: u8,
        day: u8,
        offset: Option<TimezoneOffset>,
    ) -> Option<Self> {
        let lexical = format!(
            "{}{}",
            instant::date_lexical(year, month, day),
            instant::offset_lexical(offset)
        );
        Self::from_lexical(&lexical)
    }

    /// Returns the year, `fn:year-from-date`, which is negative before year one.
    pub fn year(self) -> i64 {
        self.0.to_library::<LibraryDate>().year()
    }

    /// Returns the month of the year, `fn:month-from-date`.
    pub fn month(self) -> u8 {
        self.0.to_library::<LibraryDate>().month()
    }

    /// Returns the day of the month, `fn:day-from-date`.
    pub fn day(self) -> u8 {
        self.0.to_library::<LibraryDate>().day()
    }

    /// Returns the timezone offset the value was written with, or `None` for an unzoned value.
    pub fn offset(self) -> Option<TimezoneOffset> {
        self.0.offset()
    }

    /// Returns the dateTime at which the day starts, in the value's offset.
    pub fn start_of_day(self) -> DateTime {
        DateTime::from_instant(self.0)
    }

    /// Returns the dateTime of a time of day on this day, `fn:dateTime`,
    /// or `None` if both carry an offset and the offsets differ,
    /// or the result is beyond the value space.
    ///
    /// The result carries the offset that is present, if any.
    pub fn at(self, time: Time) -> Option<DateTime> {
        self.0.at(time.instant()).map(DateTime::from_instant)
    }

    /// Returns whether two values are equal in XSD's value space:
    /// both zoned or both unzoned, and starting at the same instant.
    pub fn value_eq(self, other: Self) -> bool {
        self.0.value_eq(other.0)
    }

    /// Returns the order of two values in XSD's value space,
    /// or `None` for a zoned and an unzoned value whose starts are within fourteen hours.
    pub fn value_cmp(self, other: Self) -> Option<Ordering> {
        self.0.value_cmp(other.0)
    }

    /// Returns the value adjusted to another offset, `fn:adjust-date-to-timezone`,
    /// or `None` beyond the value space.
    ///
    /// Given an offset, a zoned value becomes the day on which its start falls in the new offset,
    /// and an unzoned value keeps its day and takes the offset.
    /// Given `None`, the value keeps its day and drops its offset.
    pub fn adjust(self, offset: Option<TimezoneOffset>) -> Option<Self> {
        self.0.adjust::<LibraryDate>(offset).map(Self)
    }

    /// Returns the value a duration later,
    /// `op:add-yearMonthDuration-to-date` and `op:add-dayTimeDuration-to-date`,
    /// or `None` beyond the value space.
    ///
    /// The months are added first, clamping the day to the month's length,
    /// then the seconds.
    /// The result is the day the sum falls on.
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add::<LibraryDate>(duration).map(Self)
    }

    /// Returns the value a duration earlier,
    /// `op:subtract-yearMonthDuration-from-date` and `op:subtract-dayTimeDuration-from-date`,
    /// or `None` beyond the value space.
    pub fn checked_sub(self, duration: Duration) -> Option<Self> {
        self.0.checked_sub::<LibraryDate>(duration).map(Self)
    }

    /// Returns the time from the start of an earlier day to the start of this one, `op:subtract-dates`,
    /// or `None` if one value is zoned and the other not.
    pub fn duration_since(self, earlier: Self) -> Option<Duration> {
        self.0.duration_since::<LibraryDate>(earlier.0)
    }
}

/// The hooks of the compact representations.
impl Date {
    /// The number of offset codes, the unzoned mark included.
    pub(crate) const OFFSET_CODE_COUNT: u64 = instant::OFFSET_CODE_COUNT;

    /// The attoseconds of a minute.
    const ATTOSECONDS_PER_MINUTE: i128 = 60 * instant::ATTOSECONDS_PER_SECOND;

    /// The Unix epoch in attoseconds of the instant.
    const UNIX_EPOCH: i128 = instant::UNIX_EPOCH_SECONDS * instant::ATTOSECONDS_PER_SECOND;

    /// Creates the date whose day starts a number of minutes after the Unix epoch, UTC,
    /// written with a timezone offset.
    pub(crate) fn from_unix_minutes(minutes: i64, offset: Option<TimezoneOffset>) -> Self {
        Self(Instant::new(Self::attoseconds(minutes), offset))
    }

    /// Creates the date whose day starts a number of minutes after the Unix epoch, UTC,
    /// written with the offset of a raw code, or `None` if the code is not one.
    pub(crate) fn from_minutes_and_code(minutes: i64, offset_code: u64) -> Option<Self> {
        Instant::from_code(Self::attoseconds(minutes), offset_code).map(Self)
    }

    /// Returns the minutes from the Unix epoch, UTC, to the instant the day starts.
    ///
    /// Every date has them: a day starts on a whole minute, as offsets are whole minutes,
    /// and the value space spans fewer minutes than the 64-bit range.
    pub(crate) fn unix_minutes(self) -> i64 {
        let since_unix_epoch = self.0.attoseconds() - Self::UNIX_EPOCH;
        debug_assert_eq!(since_unix_epoch % Self::ATTOSECONDS_PER_MINUTE, 0);
        i64::try_from(since_unix_epoch / Self::ATTOSECONDS_PER_MINUTE)
            .expect("the value space spans fewer minutes than the 64-bit range")
    }

    /// Returns the raw offset code, below [OFFSET_CODE_COUNT](Self::OFFSET_CODE_COUNT).
    pub(crate) fn offset_code(self) -> u64 {
        self.0.offset_code()
    }

    /// Returns the attoseconds of the instant a number of minutes after the Unix epoch.
    fn attoseconds(minutes: i64) -> i128 {
        Self::UNIX_EPOCH + i128::from(minutes) * Self::ATTOSECONDS_PER_MINUTE
    }
}

impl fmt::Debug for Date {
    /// Writes the value in its canonical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for Date {
    /// Writes the canonical `xsd:date` lexical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.to_library::<LibraryDate>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::zoned;

    /// Parses the date of a lexical; panics on malformed input.
    fn date(lexical: &str) -> Date {
        Date::from_lexical(lexical).expect(lexical)
    }

    #[test]
    fn canonical_lexicals_round_trip() {
        for lexical in [
            "2024-03-01",
            "2024-03-01Z",
            "2024-03-01+05:30",
            "2024-03-01-14:00",
            "2024-02-29",
            "-0044-03-15Z",
            "0000-01-01",
            "12345-06-07",
            "-13798000000-01-01+14:00",
        ] {
            assert_eq!(date(lexical).to_string(), lexical);
            assert_eq!(format!("{:?}", date(lexical)), lexical);
        }
        assert_eq!(date("2024-03-01+00:00").to_string(), "2024-03-01Z");
    }

    #[test]
    fn malformed_lexicals_are_refused() {
        for lexical in [
            "",
            "2024-02-30",
            "2023-02-29",
            "2024-13-01",
            "2024-1-1",
            "+2024-01-01",
            "02024-01-01",
            "2024-01-01T00:00:00",
            "2024-01-01+14:30",
            "2024-01-01 Z",
            "9223372036854775807-12-31",
        ] {
            assert_eq!(Date::from_lexical(lexical), None, "{lexical}");
        }
    }

    #[test]
    fn components_build_and_read_values() {
        let value = Date::from_components(2024, 3, 1, zoned(330)).expect("valid");
        assert_eq!(value, date("2024-03-01+05:30"));
        assert_eq!((value.year(), value.month(), value.day()), (2024, 3, 1));
        assert_eq!(value.offset(), zoned(330));
        assert_eq!(
            Date::from_components(-44, 3, 15, None).expect("valid"),
            date("-0044-03-15")
        );

        let west = date("2024-01-01-14:00");
        assert_eq!((west.year(), west.month(), west.day()), (2024, 1, 1));
        assert_eq!(Date::from_components(2023, 2, 29, None), None);
        assert_eq!(Date::from_components(2024, 0, 1, None), None);
        assert_eq!(Date::from_components(i64::MIN, 1, 1, None), None);
    }

    #[test]
    fn days_start_and_join_with_times() {
        let time = |lexical: &str| Time::from_lexical(lexical).expect(lexical);
        let datetime = |lexical: &str| DateTime::from_lexical(lexical).expect(lexical);
        assert_eq!(
            date("1999-12-31").start_of_day(),
            datetime("1999-12-31T00:00:00")
        );
        assert_eq!(
            date("1999-12-31-14:00").start_of_day(),
            datetime("1999-12-31T00:00:00-14:00")
        );
        assert_eq!(
            date("1999-12-31").at(time("12:00:00")),
            Some(datetime("1999-12-31T12:00:00"))
        );

        // A time `24:00:00` is the time `00:00:00`, not the next day.
        assert_eq!(
            date("1999-12-31").at(time("24:00:00")),
            Some(datetime("1999-12-31T00:00:00"))
        );
        assert_eq!(
            date("1999-12-31").at(time("12:00:00.25+05:30")),
            Some(datetime("1999-12-31T12:00:00.25+05:30"))
        );
        assert_eq!(
            date("1999-12-31Z").at(time("12:00:00")),
            Some(datetime("1999-12-31T12:00:00Z"))
        );
        assert_eq!(
            date("1999-12-31+05:30").at(time("12:00:00+05:30")),
            Some(datetime("1999-12-31T12:00:00+05:30"))
        );
        assert_eq!(date("1999-12-31Z").at(time("12:00:00+05:30")), None);

        // Splitting and joining are inverses.
        for lexical in [
            "2024-03-01T12:30:00.5+05:30",
            "2024-03-01T23:59:59.999999999999999999-14:00",
            "-0044-03-15T09:00:00",
        ] {
            let value = datetime(lexical);
            let day = value.date().expect("in range");
            assert_eq!(day.at(value.time()), Some(value));
            assert_eq!(day.start_of_day().date(), Some(day));
        }
    }

    #[test]
    fn value_comparison_follows_xsd() {
        assert!(date("2024-03-01Z").value_eq(date("2024-03-01+00:00")));
        assert!(!date("2024-03-01Z").value_eq(date("2024-03-01")));
        assert!(!date("2024-03-01Z").value_eq(date("2024-03-01+01:00")));
        assert_eq!(
            date("2024-03-01").value_cmp(date("2024-03-02")),
            Some(Ordering::Less)
        );
        assert_eq!(
            date("2024-03-01+14:00").value_cmp(date("2024-03-01-14:00")),
            Some(Ordering::Less)
        );
        assert_eq!(date("2024-03-01").value_cmp(date("2024-03-01Z")), None);
        assert_eq!(
            date("2024-03-01").value_cmp(date("2024-03-03Z")),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn order_is_by_start_with_unzoned_last() {
        let ordered = [
            "-13798000000-01-01Z",
            "2024-02-29",
            "2024-03-01+14:00",
            "2024-02-29-11:00",
            "2024-03-01Z",
            "2024-03-01",
            "2024-03-02+14:00",
            "2024-03-01-14:00",
        ]
        .map(date);
        for window in ordered.windows(2) {
            assert!(window[0] < window[1], "{} < {}", window[0], window[1]);
        }
    }

    #[test]
    fn adjusting_follows_xpath() {
        let adjusted = |lexical: &str, offset: Option<TimezoneOffset>| {
            date(lexical).adjust(offset).expect("in range").to_string()
        };
        assert_eq!(
            adjusted("2002-03-07-07:00", zoned(-600)),
            "2002-03-06-10:00"
        );
        assert_eq!(adjusted("2002-03-07", zoned(-600)), "2002-03-07-10:00");
        assert_eq!(adjusted("2002-03-07-07:00", None), "2002-03-07");
        assert_eq!(adjusted("2002-03-07", None), "2002-03-07");
        assert_eq!(
            adjusted("2002-03-07+01:00", zoned(-480)),
            "2002-03-06-08:00"
        );
    }

    #[test]
    fn arithmetic_follows_xpath() {
        let duration = |lexical: &str| Duration::from_lexical(lexical).expect(lexical);
        assert_eq!(
            date("2000-10-30").checked_add(duration("P1Y2M")),
            Some(date("2001-12-30"))
        );
        assert_eq!(
            date("2004-10-30Z").checked_add(duration("P2DT2H30M0S")),
            Some(date("2004-11-01Z"))
        );
        assert_eq!(
            date("2000-10-30").checked_sub(duration("P1Y2M")),
            Some(date("1999-08-30"))
        );
        assert_eq!(
            date("2000-02-29Z").checked_sub(duration("P1D")),
            Some(date("2000-02-28Z"))
        );
        assert_eq!(
            date("2024-01-31").checked_add(duration("P1M")),
            Some(date("2024-02-29"))
        );
        assert_eq!(
            date("2000-10-30").duration_since(date("1999-11-28")),
            Some(duration("P337D"))
        );
        assert_eq!(
            date("2000-10-30+05:00").duration_since(date("1999-11-28Z")),
            Some(duration("P336DT19H"))
        );
        assert_eq!(date("2000-10-30").duration_since(date("1999-11-28Z")), None);
        assert_eq!(
            date("-13798000000-01-01+14:00").checked_sub(duration("P5391559471918Y")),
            None
        );
    }
}
