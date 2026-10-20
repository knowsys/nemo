//! Payload encoding of [Tag::Duration].

use crate::{
    storage::{
        Tag,
        encoding::{PayloadEncoding, bit_field::BitField},
    },
    value::{Duration, ValueDomain},
};

/// The field holding the kind of the duration, its domain, in the top bits.
const KIND: BitField = BitField::new(62..64);

/// The field holding the milliseconds of a dayTimeDuration, below the kind.
const DAY_TIME_MILLIS: BitField = BitField::new(0..62);

/// The field holding the months of a yearMonthDuration, below the kind.
const YEAR_MONTH_MONTHS: BitField = BitField::new(0..62);

/// The field holding the months of a duration with both months and seconds, below the kind.
const MIXED_MONTHS: BitField = BitField::new(42..62);

/// The field holding the milliseconds of a duration with both months and seconds,
/// below its months.
const MIXED_MILLIS: BitField = BitField::new(0..42);

/// The kind of a dayTimeDuration, the lowest in the order of the domains.
const DAY_TIME: u64 = 0;

/// The kind of a yearMonthDuration.
const YEAR_MONTH: u64 = 1;

/// The kind of a duration with both months and seconds, the highest.
const MIXED: u64 = 2;

/// A duration encodes as its kind, then the fields of that kind.
///
/// The kind takes the top two bits, in the order of the domains.
/// The 62 bits below it hold:
///
/// - for a dayTimeDuration, its milliseconds, about ±73 million years;
/// - for a yearMonthDuration, its months;
/// - for a duration with both, its months in the high 20 bits, about ±43 000 years,
///   and its milliseconds in the low 42, about ±69 years.
///
/// Each field is biased,
/// so payloads order by kind, then by months, then by seconds, as durations do.
///
/// A duration finer than a millisecond, or beyond its fields,
/// has no payload and lives in the dictionary.
impl PayloadEncoding for Duration {
    const TAG: Tag = Tag::Duration;
    type Encoded = Option<u64>;

    fn to_payload(self) -> Option<u64> {
        let (months, millis) = (self.total_months(), self.millis()?);

        Some(match self.domain() {
            ValueDomain::DayTimeDuration => {
                KIND.put(DAY_TIME) | DAY_TIME_MILLIS.put_signed(millis)?
            }
            ValueDomain::YearMonthDuration => {
                KIND.put(YEAR_MONTH) | YEAR_MONTH_MONTHS.put_signed(months)?
            }
            _ => {
                KIND.put(MIXED)
                    | MIXED_MONTHS.put_signed(months)?
                    | MIXED_MILLIS.put_signed(millis)?
            }
        })
    }

    fn from_payload(payload: u64) -> Self {
        let (months, millis) = match KIND.get(payload) {
            DAY_TIME => (0, DAY_TIME_MILLIS.get_signed(payload)),
            YEAR_MONTH => (YEAR_MONTH_MONTHS.get_signed(payload), 0),
            _ => (
                MIXED_MONTHS.get_signed(payload),
                MIXED_MILLIS.get_signed(payload),
            ),
        };

        Duration::from_millis(months, millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::payload;

    /// Parses the duration of a lexical; panics on malformed input.
    fn duration(lexical: &str) -> Duration {
        Duration::from_lexical(lexical).expect(lexical)
    }

    #[test]
    fn encoding_round_trips() {
        for lexical in [
            "PT0S",
            "PT5H30M",
            "-PT14H",
            "P12345DT0.001S",
            "P1Y",
            "-P1000Y11M",
            "P1Y2M3DT4H5M6.5S",
            "-P1Y2M3DT4H5M6.5S",
        ] {
            let value = duration(lexical);
            assert_eq!(Duration::from_payload(payload(value)), value, "{lexical}");
        }
    }

    #[test]
    fn order_matches_term_order() {
        let ordered = [
            duration("-P10000D"),
            duration("-PT1S"),
            duration("PT0S"),
            duration("PT1S"),
            duration("P10000D"),
            duration("-P2Y"),
            duration("-P1M"),
            duration("P1M"),
            duration("P2Y"),
            duration("-P1Y1D"),
            duration("-P1MT1S"),
            duration("P1MT1S"),
            duration("P1MT2S"),
            duration("P1Y1D"),
        ];

        for window in ordered.windows(2) {
            let left = payload(window[0]);
            let right = payload(window[1]);
            assert!(left < right, "{:?} < {:?}", window[0], window[1]);
            assert_eq!(window[0].cmp(&window[1]), left.cmp(&right));
        }
    }

    #[test]
    fn the_payload_holds_milliseconds_within_its_fields() {
        assert_eq!(duration("PT0.0001S").to_payload(), None);
        assert_eq!(duration("P1MT0.0001S").to_payload(), None);
        assert_eq!(duration("P1MT3000000000S").to_payload(), None);
        assert_eq!(duration("P50000Y1D").to_payload(), None);
        assert!(duration("P40000Y1D").to_payload().is_some());
    }
}
