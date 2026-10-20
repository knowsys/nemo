//! Payload encoding of [Tag::Time].

use crate::{
    storage::{
        Tag,
        encoding::{PayloadEncoding, temporal::instant},
    },
    value::Time,
};

/// The offset bits hold every offset code.
const _: () = assert!(Time::OFFSET_CODE_COUNT <= instant::OFFSET_CODES);

/// A time encodes as its instant, then the code of its offset.
///
/// The instant counts nanoseconds from `1972-12-31T00:00:00Z`,
/// the start of the [reference day](Time) of times.
/// A time lies within fourteen hours of that day, far inside the payload's range.
/// Payloads order by instant, then by offset, as times do.
///
/// A time finer than a nanosecond has no payload and lives in the dictionary.
impl PayloadEncoding for Time {
    const TAG: Tag = Tag::Time;
    type Encoded = Option<u64>;

    fn to_payload(self) -> Option<u64> {
        instant::pack(self.nanos()?, self.offset_code())
    }

    fn from_payload(payload: u64) -> Self {
        let (nanos, offset_code) = instant::unpack(payload);
        Time::from_nanos_and_code(nanos, offset_code)
            .expect("time payloads carry a valid offset code")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::payload;

    /// Parses the time of a lexical; panics on malformed input.
    fn time(lexical: &str) -> Time {
        Time::from_lexical(lexical).expect(lexical)
    }

    #[test]
    fn encoding_round_trips() {
        for lexical in [
            "00:00:00",
            "00:00:00+14:00",
            "23:59:59.999999999-14:00",
            "12:30:05.25Z",
            "24:00:00",
        ] {
            let value = time(lexical);
            assert_eq!(Time::from_payload(payload(value)), value, "{lexical}");
        }
    }

    #[test]
    fn order_matches_term_order() {
        let ordered = [
            time("00:00:00+14:00"),
            time("00:00:00Z"),
            time("00:00:00"),
            time("12:00:00+01:00"),
            time("12:00:00Z"),
            time("23:59:59.999999999-14:00"),
        ];

        for window in ordered.windows(2) {
            let left = payload(window[0]);
            let right = payload(window[1]);
            assert!(left < right, "{:?} < {:?}", window[0], window[1]);
            assert_eq!(window[0].cmp(&window[1]), left.cmp(&right));
        }
    }

    #[test]
    fn the_payload_holds_nanoseconds() {
        assert!(time("12:00:00.123456789Z").to_payload().is_some());
        assert_eq!(time("12:00:00.1234567891Z").to_payload(), None);
    }
}
