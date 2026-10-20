//! Payload encoding of [Tag::DateTime].

use crate::{
    storage::{
        Tag,
        encoding::{PayloadEncoding, temporal::instant},
    },
    value::DateTime,
};

/// The offset bits hold every offset code.
const _: () = assert!(DateTime::OFFSET_CODE_COUNT <= instant::OFFSET_CODES);

/// A dateTime encodes as its instant, then the code of its offset.
///
/// The instant counts milliseconds since the Unix epoch,
/// which the payload holds for about ±71 000 years.
/// Payloads order by instant, then by offset, as dateTimes do.
///
/// A dateTime finer than a millisecond or farther out
/// has no payload and lives in the dictionary.
impl PayloadEncoding for DateTime {
    const TAG: Tag = Tag::DateTime;
    type Encoded = Option<u64>;

    fn to_payload(self) -> Option<u64> {
        instant::pack(self.unix_millis()?, self.offset_code())
    }

    fn from_payload(payload: u64) -> Self {
        let (millis, offset_code) = instant::unpack(payload);
        DateTime::from_millis_and_code(millis, offset_code)
            .expect("dateTime payloads carry a valid offset code")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        testing::{payload, zoned},
        value::TimezoneOffset,
    };

    /// The bias of the payload's count, one past its largest count.
    const INSTANT_BIAS: i64 = 1 << (instant::COUNT.width() - 1);

    /// A dateTime at the given milliseconds since the Unix epoch.
    fn at(millis: i64, offset: Option<TimezoneOffset>) -> DateTime {
        DateTime::from_unix_millis(millis, offset)
    }

    #[test]
    fn encoding_round_trips() {
        let values = [
            at(0, zoned(0)),
            at(0, None),
            at(1_755_600_000_000, zoned(120)),
            at(-62_135_596_800_000, zoned(-840)),
            at(INSTANT_BIAS - 1, None),
            at(-INSTANT_BIAS, zoned(840)),
        ];

        for value in values {
            assert_eq!(DateTime::from_payload(payload(value)), value);
        }
    }

    #[test]
    fn order_matches_term_order() {
        let ordered = [
            at(-INSTANT_BIAS, zoned(0)),
            at(-1, None),
            at(0, zoned(-840)),
            at(0, zoned(0)),
            at(0, zoned(840)),
            at(0, None),
            at(1, zoned(-840)),
            at(INSTANT_BIAS - 1, None),
        ];

        for window in ordered.windows(2) {
            let left = payload(window[0]);
            let right = payload(window[1]);
            assert!(left < right, "{:?} < {:?}", window[0], window[1]);
            assert_eq!(window[0].cmp(&window[1]), left.cmp(&right));
        }
    }

    #[test]
    fn the_payload_holds_milliseconds_in_range() {
        for millis in [INSTANT_BIAS, -INSTANT_BIAS - 1, i64::MAX, i64::MIN] {
            assert_eq!(at(millis, zoned(0)).to_payload(), None);
        }

        assert!(at(INSTANT_BIAS - 1, zoned(0)).to_payload().is_some());
        assert!(at(-INSTANT_BIAS, zoned(0)).to_payload().is_some());

        // Milliseconds within the range have a payload; a finer fraction or a
        // farther instant does not.
        let has_payload = |lexical: &str| {
            DateTime::from_lexical(lexical)
                .expect(lexical)
                .to_payload()
                .is_some()
        };

        assert!(has_payload("2024-01-01T00:00:00.123Z"));
        assert!(has_payload("2024-01-01T00:00:00.1230Z"));
        assert!(!has_payload("2024-01-01T00:00:00.1234Z"));
        assert!(has_payload("71000-01-01T00:00:00Z"));
        assert!(!has_payload("-13798000000-01-01T00:00:00Z"));
        assert!(has_payload("1999-12-31T24:00:00"));
    }
}
