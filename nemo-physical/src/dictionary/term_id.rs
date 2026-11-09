//! Term ids and their layout.
//!
//! A term id packs the [TermLane] of a term and the term's index in its store into one `u64`,
//! which is the payload of a [Term](crate::storage::Tag::Term) storage value.
//! The layout has two *regimes*, which keep the ids small:
//! - while the index fits in 29 bits, the lane is in the three bits above it,
//!   so the id fits in 32 bits and a column of such ids needs 32 bits per value;
//! - beyond, the lane is in the top three bits of the `u64`.
//!
//! In each regime, the ids of one lane form a contiguous range.

/// The number of low bits that hold the index in the 32-bit regime.
const SHIFT_32: u32 = 29;

/// The number of low bits that hold the index in the 64-bit regime.
const SHIFT_64: u32 = 61;

/// The smallest index that does not fit the 32-bit regime.
const LIMIT_32: u64 = 1 << SHIFT_32;

/// The lane of a term, which is its kind of term.
///
/// Real data mixes these kinds in the same columns,
/// so they share the [Term](crate::storage::Tag::Term) tag and split its payloads into lanes.
/// The discriminants are the lane numbers, which take three bits.
/// Zero is not a lane, which keeps every id nonzero and the two regimes disjoint.
/// The numbers 6 and 7 are spare.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TermLane {
    /// IRIs.
    Iri = 1,
    /// Plain strings.
    String = 2,
    /// Language-tagged strings.
    LanguageString = 3,
    /// Literals of datatypes without native support.
    Other = 4,
    /// Nulls, fresh or labeled.
    Null = 5,
}

impl TermLane {
    /// Returns the lane with the given number,
    /// or `None` for a number that is not a lane.
    fn from_number(number: u64) -> Option<Self> {
        match number {
            1 => Some(TermLane::Iri),
            2 => Some(TermLane::String),
            3 => Some(TermLane::LanguageString),
            4 => Some(TermLane::Other),
            5 => Some(TermLane::Null),
            _ => None,
        }
    }
}

/// A term id, which packs a lane and an index into one `u64`.
///
/// The layout is `lane << 29 | index` while the index fits in 29 bits,
/// and `lane << 61 | index` beyond.
/// The regimes cannot collide because no lane is zero.
/// An id of the 32-bit regime is below 2³², and an id of the 64-bit regime is at least 2⁶¹.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TermId(u64);

impl TermId {
    /// Creates an id from its raw form, the payload of its storage value.
    pub(crate) fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw form of this id, the payload of its storage value.
    pub(crate) fn raw(self) -> u64 {
        self.0
    }

    /// Packs a lane and an index into an id,
    /// in the regime that the size of the index requires.
    pub(crate) fn compose(lane: TermLane, index: u64) -> Self {
        let number = lane as u64;

        if index < LIMIT_32 {
            Self(number << SHIFT_32 | index)
        } else {
            debug_assert!(index < 1 << SHIFT_64, "indexes never reach 61 bits");
            Self(number << SHIFT_64 | index)
        }
    }

    /// Splits this id into its lane and index, or returns `None` for a malformed id.
    ///
    /// An id is malformed if its lane number is not a lane,
    /// or if it is in the 64-bit regime with an index that fits the 32-bit regime.
    pub(crate) fn decompose(self) -> Option<(TermLane, u64)> {
        if self.0 < 1 << 32 {
            let index = self.0 & (LIMIT_32 - 1);

            Some((TermLane::from_number(self.0 >> SHIFT_32)?, index))
        } else {
            let index = self.0 & ((1 << SHIFT_64) - 1);

            if index < LIMIT_32 {
                return None;
            }

            Some((TermLane::from_number(self.0 >> SHIFT_64)?, index))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All lanes, for exhaustive layout tests.
    const LANES: [TermLane; 5] = [
        TermLane::Iri,
        TermLane::String,
        TermLane::LanguageString,
        TermLane::Other,
        TermLane::Null,
    ];

    /// Every lane with indexes of both regimes is decomposed back from its id.
    #[test]
    fn lanes_round_trip() {
        for lane in LANES {
            for index in [0, 1, LIMIT_32 - 1, LIMIT_32, u64::from(u32::MAX), 1 << 40] {
                let id = TermId::compose(lane, index);
                assert_eq!(id.decompose(), Some((lane, index)), "{lane:?} {index}");
                assert_eq!(TermId::from_raw(id.raw()), id);
            }
        }
    }

    /// An id fits in a `u32` exactly if its index is below [LIMIT_32].
    #[test]
    fn ids_of_the_32_bit_regime_fit_a_u32() {
        for lane in LANES {
            assert!(TermId::compose(lane, LIMIT_32 - 1).raw() <= u64::from(u32::MAX));
            assert!(TermId::compose(lane, LIMIT_32).raw() > u64::from(u32::MAX));
        }
    }

    /// Within a lane, ids are ordered like their indexes,
    /// and the lanes of one regime do not interleave.
    #[test]
    fn lanes_are_contiguous_and_ordered() {
        let early = TermId::compose(TermLane::String, 0).raw();
        let late = TermId::compose(TermLane::String, LIMIT_32 - 1).raw();

        assert!(early < late);
        assert!(late < TermId::compose(TermLane::LanguageString, 0).raw());
    }

    /// Malformed ids are not decomposed, and the smallest id of a lane is.
    #[test]
    fn malformed_ids_are_rejected() {
        // Lane number zero in both regimes, a spare lane number,
        // and a 64-bit encoding of a small index.
        assert_eq!(TermId::from_raw(0).decompose(), None);
        assert_eq!(TermId::from_raw(LIMIT_32 - 1).decompose(), None);
        assert_eq!(TermId::from_raw(6 << SHIFT_32).decompose(), None);
        assert_eq!(TermId::from_raw(1 << 61 | 5).decompose(), None);

        assert_eq!(
            TermId::from_raw(LIMIT_32).decompose(),
            Some((TermLane::Iri, 0))
        );
    }
}
