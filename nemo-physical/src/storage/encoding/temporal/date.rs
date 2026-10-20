//! Payload encoding of [Tag::Date].

use crate::{
    storage::{
        Tag,
        encoding::{PayloadEncoding, temporal::instant},
    },
    value::Date,
};

/// The offset bits hold every offset code.
const _: () = assert!(Date::OFFSET_CODE_COUNT <= instant::OFFSET_CODES);

/// A date encodes as the instant its day starts, then the code of its offset.
///
/// The instant counts minutes since the Unix epoch,
/// which the payload holds for about ±4.3 billion years.
/// Payloads order by instant, then by offset, as dates do.
///
/// A date beyond that range has no payload and lives in the dictionary.
impl PayloadEncoding for Date {
    const TAG: Tag = Tag::Date;
    type Encoded = Option<u64>;

    fn to_payload(self) -> Option<u64> {
        instant::pack(self.unix_minutes(), self.offset_code())
    }

    fn from_payload(payload: u64) -> Self {
        let (minutes, offset_code) = instant::unpack(payload);
        Date::from_minutes_and_code(minutes, offset_code)
            .expect("date payloads carry a valid offset code")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::payload;

    /// Parses the date of a lexical; panics on malformed input.
    fn date(lexical: &str) -> Date {
        Date::from_lexical(lexical).expect(lexical)
    }

    #[test]
    fn encoding_round_trips() {
        for lexical in [
            "2024-03-01",
            "2024-03-01Z",
            "2024-03-01+14:00",
            "2024-03-01-14:00",
            "-0044-03-15",
            "4000000000-01-01Z",
        ] {
            let value = date(lexical);
            assert_eq!(Date::from_payload(payload(value)), value, "{lexical}");
        }
    }

    #[test]
    fn order_matches_term_order() {
        let ordered = [
            date("-0044-03-15"),
            date("2024-03-01+14:00"),
            date("2024-03-01Z"),
            date("2024-03-01"),
            // The day after, starting fourteen hours ahead of UTC,
            // starts before the day that starts fourteen hours behind.
            date("2024-03-02+14:00"),
            date("2024-03-01-14:00"),
            date("2024-03-02Z"),
        ];

        for window in ordered.windows(2) {
            let left = payload(window[0]);
            let right = payload(window[1]);
            assert!(left < right, "{:?} < {:?}", window[0], window[1]);
            assert_eq!(window[0].cmp(&window[1]), left.cmp(&right));
        }
    }

    #[test]
    fn the_payload_holds_dates_within_its_range() {
        assert_eq!(date("-13798000000-01-01").to_payload(), None);
        assert_eq!(date("5000000000-01-01Z").to_payload(), None);
    }
}
