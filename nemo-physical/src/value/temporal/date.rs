//! Calendar days with optional timezone offsets.

use std::{cmp::Ordering, fmt};

use oxsdatatypes::Date as LibraryDate;

use crate::value::temporal::{
    DateTime, Duration, Time, TimezoneOffset,
    instant::{self, Instant},
};

/// An [`xsd:date`](https://www.w3.org/TR/xmlschema11-2/#date) value,
/// a calendar day and the timezone offset that it was written with.
///
/// A local day has no offset.
/// Equality, order and hashing follow term identity,
/// so the same day in different offsets gives distinct values.
/// The values are ordered by the instant at which the day starts, with a local day read as UTC,
/// and then by offset, with no offset last.
/// XSD's comparison by value is [value_eq](Self::value_eq) and [value_cmp](Self::value_cmp).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date(Instant);

/// Checks at compile time that a value occupies 24 bytes at 8-byte alignment.
const _: () = assert!(size_of::<Date>() == 24 && align_of::<Date>() == 8);

impl Date {
    /// Creates the date from the instant at which its day starts.
    pub(super) fn from_instant(instant: Instant) -> Self {
        Self(instant)
    }

    /// Parses an `xsd:date` lexical form,
    /// or returns `None` for a malformed lexical form or a value beyond the value space.
    pub fn from_lexical(lexical: &str) -> Option<Self> {
        Instant::from_lexical::<LibraryDate>(lexical).map(Self)
    }

    /// Creates the date with the given calendar fields,
    /// or `None` for fields that name no day or a value beyond the value space.
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

    /// Returns the timezone offset that the value was written with,
    /// or `None` for an unzoned value.
    pub fn offset(self) -> Option<TimezoneOffset> {
        self.0.offset()
    }

    /// Returns the dateTime at which the day starts, in the value's offset.
    pub fn start_of_day(self) -> DateTime {
        DateTime::from_instant(self.0)
    }

    /// Returns the dateTime of a time of day on this day, `fn:dateTime`,
    /// or `None` if both values have offsets and the offsets differ,
    /// or if the result is beyond the value space.
    ///
    /// The result has the offset of whichever value has one.
    pub fn at(self, time: Time) -> Option<DateTime> {
        self.0.at(time.instant()).map(DateTime::from_instant)
    }

    /// Returns whether two values are equal by value.
    ///
    /// They are if both are zoned or both are unzoned, and their days start at the same instant.
    pub fn value_eq(self, other: Self) -> bool {
        self.0.value_eq(other.0)
    }

    /// Returns the order of two values by value,
    /// or `None` for a zoned and an unzoned value whose days start at most fourteen hours apart.
    pub fn value_cmp(self, other: Self) -> Option<Ordering> {
        self.0.value_cmp(other.0)
    }

    /// Returns the value adjusted to another offset, `fn:adjust-date-to-timezone`,
    /// or `None` beyond the value space.
    ///
    /// With an offset, a zoned value becomes the day on which its start falls in the new
    /// offset, and an unzoned value keeps its day and gets the offset.
    /// With `None`, the value keeps its day and loses its offset.
    pub fn adjust(self, offset: Option<TimezoneOffset>) -> Option<Self> {
        self.0.adjust::<LibraryDate>(offset).map(Self)
    }

    /// Returns the value a duration later,
    /// `op:add-yearMonthDuration-to-date` and `op:add-dayTimeDuration-to-date`,
    /// or `None` beyond the value space.
    ///
    /// The months are added first, with the day clamped to the length of the month,
    /// and then the seconds.
    /// The result is the day on which the sum falls.
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add::<LibraryDate>(duration).map(Self)
    }

    /// Returns the value a duration earlier,
    /// `op:subtract-yearMonthDuration-from-date` and `op:subtract-dayTimeDuration-from-date`,
    /// or `None` beyond the value space.
    pub fn checked_sub(self, duration: Duration) -> Option<Self> {
        self.0.checked_sub::<LibraryDate>(duration).map(Self)
    }

    /// Returns the time from the start of an earlier day to the start of this one,
    /// `op:subtract-dates`, or `None` if one value is zoned and the other is not.
    pub fn duration_since(self, earlier: Self) -> Option<Duration> {
        self.0.duration_since::<LibraryDate>(earlier.0)
    }
}

/// Conversions to and from the compact representation of dates.
impl Date {
    /// The number of offset codes, including the mark of an unzoned value.
    pub(crate) const OFFSET_CODE_COUNT: u64 = instant::OFFSET_CODE_COUNT;

    /// The number of attoseconds in a minute.
    const ATTOSECONDS_PER_MINUTE: i128 = 60 * instant::ATTOSECONDS_PER_SECOND;

    /// The Unix epoch, in attoseconds since `0001-01-01T00:00:00Z`.
    const UNIX_EPOCH: i128 = instant::UNIX_EPOCH_SECONDS * instant::ATTOSECONDS_PER_SECOND;

    /// Creates the date whose day starts `minutes` minutes after the Unix epoch, UTC,
    /// and which was written with `offset`.
    pub(crate) fn from_unix_minutes(minutes: i64, offset: Option<TimezoneOffset>) -> Self {
        Self(Instant::new(Self::attoseconds(minutes), offset))
    }

    /// Creates the date whose day starts `minutes` minutes after the Unix epoch, UTC,
    /// and whose offset has the given raw code,
    /// or `None` if the code is not an offset code.
    pub(crate) fn from_minutes_and_code(minutes: i64, offset_code: u64) -> Option<Self> {
        Instant::from_code(Self::attoseconds(minutes), offset_code).map(Self)
    }

    /// Returns the minutes from the Unix epoch, UTC, to the instant at which the day starts.
    ///
    /// They are defined for every date:
    /// a day starts on a whole minute, since offsets are whole minutes,
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

    /// Returns the attoseconds since `0001-01-01T00:00:00Z`
    /// of the instant `minutes` minutes after the Unix epoch.
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

    /// Returns the date of a lexical form, and panics for a malformed one.
    fn date(lexical: &str) -> Date {
        Date::from_lexical(lexical).expect(lexical)
    }

    /// A canonical lexical form is written back unchanged, and `+00:00` is written as `Z`.
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

    /// Malformed lexical forms and values beyond the value space are rejected.
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

    /// A date is created from calendar fields and returns them,
    /// and fields that name no day are rejected.
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

    /// A date gives the dateTime at which it starts, and joins with a time into a dateTime.
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

    /// Dates are compared by value as XSD defines it.
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

    /// The term order sorts dates by the instant at which they start,
    /// with an unzoned date after the zoned ones at the same instant.
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

    /// Adjusting a date to an offset gives the results of the examples of XPath.
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

    /// Date arithmetic gives the results of the examples of XPath.
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
