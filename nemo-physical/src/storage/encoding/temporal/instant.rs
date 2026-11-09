//! The payload layout that dateTimes, dates and times share.

use crate::storage::encoding::bit_field::BitField;

/// The field of the count, in the unit of the type, above the offset code.
pub(super) const COUNT: BitField = BitField::new(12..64);

/// The field of the code of the timezone offset, in the low bits.
const OFFSET: BitField = BitField::new(0..12);

/// The number of offset codes that the offset field can store.
pub(super) const OFFSET_CODES: u64 = 1 << OFFSET.width();

/// Packs a count and an offset code into a payload,
/// or returns `None` for a count beyond the range of the [COUNT] field.
///
/// Payloads are ordered by count first and by offset code second.
pub(super) fn pack(count: i64, offset_code: u64) -> Option<u64> {
    Some(COUNT.put_signed(count)? | OFFSET.put(offset_code))
}

/// Unpacks a payload into its count and its offset code.
pub(super) fn unpack(payload: u64) -> (i64, u64) {
    (COUNT.get_signed(payload), OFFSET.get(payload))
}
