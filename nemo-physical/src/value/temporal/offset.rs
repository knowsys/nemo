//! Timezone offsets.

use std::fmt;

use crate::value::temporal::{Duration, instant::ATTOSECONDS_PER_SECOND};

/// A timezone offset from UTC,
/// a whole number of minutes within fourteen hours either way,
/// as XSD allows on its [date and time values](https://www.w3.org/TR/xmlschema11-2/#dateTime).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimezoneOffset {
    /// Minutes east of UTC.
    minutes: i16,
}

impl TimezoneOffset {
    /// The largest offset magnitude, in minutes.
    const MAX_MINUTES: i16 = 14 * 60;

    /// The number of offsets, one per minute.
    pub(super) const CODE_COUNT: u16 = 2 * Self::MAX_MINUTES.cast_unsigned() + 1;

    /// The offset of UTC, `Z`.
    pub const UTC: Self = Self { minutes: 0 };

    /// The westmost offset, `-14:00`.
    pub const MIN: Self = Self {
        minutes: -Self::MAX_MINUTES,
    };

    /// The eastmost offset, `+14:00`.
    pub const MAX: Self = Self {
        minutes: Self::MAX_MINUTES,
    };

    /// Creates the offset of a number of minutes east of UTC,
    /// or `None` beyond fourteen hours either way.
    pub fn new(minutes: i16) -> Option<Self> {
        (-Self::MAX_MINUTES..=Self::MAX_MINUTES)
            .contains(&minutes)
            .then_some(Self { minutes })
    }

    /// Creates the offset a duration names, the one `fn:adjust-dateTime-to-timezone` takes,
    /// or `None` for a duration that is not whole minutes within fourteen hours either way.
    pub fn from_duration(duration: Duration) -> Option<Self> {
        const ATTOSECONDS_PER_MINUTE: i128 = 60 * ATTOSECONDS_PER_SECOND;

        let attoseconds = duration.total_attoseconds();
        if !duration.is_day_time() || attoseconds % ATTOSECONDS_PER_MINUTE != 0 {
            return None;
        }

        Self::new(i16::try_from(attoseconds / ATTOSECONDS_PER_MINUTE).ok()?)
    }

    /// Minutes east of UTC, negative to the west.
    pub fn minutes(self) -> i16 {
        self.minutes
    }

    /// Returns the order-preserving code of the offset, below [CODE_COUNT](Self::CODE_COUNT).
    pub(super) fn code(self) -> u16 {
        (self.minutes + Self::MAX_MINUTES).cast_unsigned()
    }

    /// Decodes the offset of a code, or `None` for a code at or above [CODE_COUNT](Self::CODE_COUNT).
    pub(super) fn from_code(code: u16) -> Option<Self> {
        (code < Self::CODE_COUNT).then(|| Self {
            minutes: code.cast_signed() - Self::MAX_MINUTES,
        })
    }
}

impl fmt::Debug for TimezoneOffset {
    /// Writes the offset in its canonical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for TimezoneOffset {
    /// Writes the canonical form: `Z` for UTC, `+hh:mm` or `-hh:mm` otherwise.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.minutes == 0 {
            return f.write_str("Z");
        }
        let sign = if self.minutes < 0 { '-' } else { '+' };
        let minutes = self.minutes.unsigned_abs();
        write!(f, "{sign}{:02}:{:02}", minutes / 60, minutes % 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_are_whole_minutes_within_fourteen_hours() {
        assert_eq!(TimezoneOffset::new(0), Some(TimezoneOffset::UTC));
        assert_eq!(TimezoneOffset::new(-840), Some(TimezoneOffset::MIN));
        assert_eq!(TimezoneOffset::new(840), Some(TimezoneOffset::MAX));
        assert_eq!(TimezoneOffset::new(841), None);
        assert_eq!(TimezoneOffset::new(-841), None);
        assert_eq!(TimezoneOffset::new(i16::MIN), None);
        assert_eq!(TimezoneOffset::new(330).expect("in range").minutes(), 330);
    }

    #[test]
    fn offsets_are_named_by_durations_of_whole_minutes() {
        let offset = |lexical| TimezoneOffset::from_duration(Duration::from_lexical(lexical)?);

        assert_eq!(offset("-PT8H"), TimezoneOffset::new(-480));
        assert_eq!(offset("PT5H30M"), TimezoneOffset::new(330));
        assert_eq!(offset("PT0S"), Some(TimezoneOffset::UTC));
        assert_eq!(offset("PT14H1M"), None);
        assert_eq!(offset("PT1H0.5S"), None);
        assert_eq!(offset("P1M"), None);
        assert_eq!(offset("P999999999999D"), None);
    }

    #[test]
    fn offsets_print_canonically() {
        for (minutes, lexical) in [
            (0, "Z"),
            (60, "+01:00"),
            (-60, "-01:00"),
            (330, "+05:30"),
            (-1, "-00:01"),
            (840, "+14:00"),
            (-840, "-14:00"),
        ] {
            let offset = TimezoneOffset::new(minutes).expect("in range");
            assert_eq!(offset.to_string(), lexical);
            assert_eq!(format!("{offset:?}"), lexical);
        }
    }

    #[test]
    fn codes_round_trip_in_order() {
        let mut previous = None;

        for code in 0..TimezoneOffset::CODE_COUNT {
            let offset = TimezoneOffset::from_code(code).expect("below the count");
            assert_eq!(offset.code(), code);
            assert!(previous < Some(offset));
            previous = Some(offset);
        }

        assert_eq!(TimezoneOffset::from_code(TimezoneOffset::CODE_COUNT), None);
        assert_eq!(TimezoneOffset::MIN.code(), 0);
        assert_eq!(TimezoneOffset::UTC.code(), 840);
    }
}
