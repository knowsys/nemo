//! This module provides implementations [`super::DataValue`]s that represent integer numbers.
//! Integers have a more complex domain hierarchy than most other kinds of data, and the related
//! logic is collected here as well.

use super::{DataValue, ValueDomain};

/// Maximal value of an u64, i.e. 2^64-1.
const U64MAX_AS_U64: u64 = std::u64::MAX;
/// Maximal value of an i64, i.e. 2^63-1.
const I64MAX_AS_U64: u64 = U64MAX_AS_U64 >> 1;
/// Maximal value of an u32, i.e. 2^32-1.
const U32MAX_AS_U64: u64 = 0xFFFF_FFFF;
/// Maximal value of an i32, i.e. 2^31-1.
const I32MAX_AS_U64: u64 = U32MAX_AS_U64 >> 1;

/// Maximal value of an i32, i.e. 2^31-1.
const U32MAX_AS_I64: i64 = 0xFFFF_FFFF;
/// Maximal value of an i32, i.e. 2^31-1.
const I32MAX_AS_I64: i64 = U32MAX_AS_I64 >> 1;
/// Minimal value of an i32, i.e. -2^31.
const I32MIN_AS_I64: i64 = -I32MAX_AS_I64 - 1;
/// Minimal value of an i64, i.e. -2^63.
//const I64MIN_AS_I64: i64 = std::i64::MIN;

/// Physical representation of an integer as an u64.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsignedLongDataValue(u64);

impl UnsignedLongDataValue {
    /// Constructor.
    pub fn new(value: u64) -> Self {
        UnsignedLongDataValue(value)
    }
}

impl DataValue for UnsignedLongDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        self.0.to_string()
    }

    /// The function needs to find the tightest domain for the given value.
    fn value_domain(&self) -> ValueDomain {
        if self.0 <= I32MAX_AS_U64 {
            return ValueDomain::NonNegativeInt;
        } else if self.0 <= U32MAX_AS_U64 {
            return ValueDomain::UnsignedInt;
        } else if self.0 <= I64MAX_AS_U64 {
            return ValueDomain::NonNegativeLong;
        } else {
            return ValueDomain::UnsignedLong;
        }
    }

    fn fits_into_i64(&self) -> bool {
        self.0 <= I64MAX_AS_U64
    }

    fn fits_into_i32(&self) -> bool {
        self.0 <= I32MAX_AS_U64
    }

    fn fits_into_u64(&self) -> bool {
        true
    }

    fn fits_into_u32(&self) -> bool {
        self.0 <= U32MAX_AS_U64
    }

    fn to_i64_unchecked(&self) -> i64 {
        self.0.try_into().unwrap()
    }

    fn to_i32_unchecked(&self) -> i32 {
        self.0.try_into().unwrap()
    }

    fn to_u64_unchecked(&self) -> u64 {
        self.0
    }

    fn to_u32_unchecked(&self) -> u32 {
        self.0.try_into().unwrap()
    }
}

/// Physical representation of an integer as an i64.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LongDataValue(i64);

impl LongDataValue {
    /// Constructor.
    pub fn new(value: i64) -> Self {
        LongDataValue(value)
    }
}

impl DataValue for LongDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        self.0.to_string()
    }

    /// The function needs to find the tightest domain for the given value.
    fn value_domain(&self) -> ValueDomain {
        if self.0 >= 0 {
            if self.0 <= I32MAX_AS_I64 {
                return ValueDomain::NonNegativeInt;
            } else if self.0 <= U32MAX_AS_I64 {
                return ValueDomain::UnsignedInt;
            } else {
                return ValueDomain::NonNegativeLong;
            }
        } else {
            if self.0 >= I32MIN_AS_I64 {
                return ValueDomain::Int;
            } else {
                return ValueDomain::Long;
            }
        }
    }

    fn fits_into_i64(&self) -> bool {
        true
    }

    fn fits_into_i32(&self) -> bool {
        self.0 <= I32MAX_AS_I64 && self.0 >= I32MIN_AS_I64
    }

    fn fits_into_u64(&self) -> bool {
        self.0 >= 0
    }

    fn fits_into_u32(&self) -> bool {
        self.0 <= U32MAX_AS_I64 && self.0 >= 0
    }

    fn to_i64(&self) -> Option<i64> {
        Some(self.0)
    }

    fn to_i64_unchecked(&self) -> i64 {
        self.0
    }

    fn to_i32_unchecked(&self) -> i32 {
        self.0.try_into().unwrap()
    }

    fn to_u64_unchecked(&self) -> u64 {
        self.0.try_into().unwrap()
    }

    fn to_u32_unchecked(&self) -> u32 {
        self.0.try_into().unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::{
        LongDataValue, UnsignedLongDataValue, I32MAX_AS_I64, I32MAX_AS_U64, I32MIN_AS_I64,
        U32MAX_AS_I64, U32MAX_AS_U64, U64MAX_AS_U64,
    };
    use crate::datavalues::{DataValue, ValueDomain};

    #[test]
    fn test_unsigned_long_unsigned_long() {
        let value: u64 = U64MAX_AS_U64;
        let long1 = UnsignedLongDataValue::new(value);

        assert_eq!(long1.lexical_value(), value.to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#unsignedLong".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::UnsignedLong);

        assert_eq!(long1.fits_into_i32(), false);
        assert_eq!(long1.fits_into_u32(), false);
        assert_eq!(long1.fits_into_i64(), false);
        assert_eq!(long1.fits_into_u64(), true);

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), None);
        assert_eq!(long1.to_i64(), None);
        assert_eq!(long1.to_u64(), Some(value));
        assert_eq!(long1.to_u64_unchecked(), value);
    }

    #[test]
    fn test_unsigned_long_nonnegative_long() {
        let value: u64 = U32MAX_AS_U64 + 42;
        let long1 = UnsignedLongDataValue::new(value);

        assert_eq!(long1.lexical_value(), value.to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#long".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::NonNegativeLong);

        assert_eq!(long1.fits_into_i32(), false);
        assert_eq!(long1.fits_into_u32(), false);
        assert_eq!(long1.fits_into_i64(), true);
        assert_eq!(long1.fits_into_u64(), true);

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), None);
        assert_eq!(long1.to_i64(), Some(value as i64));
        assert_eq!(long1.to_i64_unchecked(), value as i64);
        assert_eq!(long1.to_u64(), Some(value));
        assert_eq!(long1.to_u64_unchecked(), value);
    }

    #[test]
    fn test_unsigned_long_unsigned_int() {
        let value: u64 = I32MAX_AS_U64 + 42;
        let long1 = UnsignedLongDataValue::new(value);

        assert_eq!(long1.lexical_value(), value.to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#long".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::UnsignedInt);

        assert_eq!(long1.fits_into_i32(), false);
        assert_eq!(long1.fits_into_u32(), true);
        assert_eq!(long1.fits_into_i64(), true);
        assert_eq!(long1.fits_into_u64(), true);

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), Some(value as u32));
        assert_eq!(long1.to_u32_unchecked(), value as u32);
        assert_eq!(long1.to_i64(), Some(value as i64));
        assert_eq!(long1.to_i64_unchecked(), value as i64);
        assert_eq!(long1.to_u64(), Some(value));
        assert_eq!(long1.to_u64_unchecked(), value);
    }

    #[test]
    fn test_unsigned_long_nonnegative_int() {
        let long1 = UnsignedLongDataValue::new(42);

        assert_eq!(long1.lexical_value(), "42".to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#int".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::NonNegativeInt);

        assert_eq!(long1.fits_into_i32(), true);
        assert_eq!(long1.fits_into_u32(), true);
        assert_eq!(long1.fits_into_i64(), true);
        assert_eq!(long1.fits_into_u64(), true);

        assert_eq!(long1.to_i32(), Some(42));
        assert_eq!(long1.to_i32_unchecked(), 42);
        assert_eq!(long1.to_u32(), Some(42));
        assert_eq!(long1.to_u32_unchecked(), 42);
        assert_eq!(long1.to_i64(), Some(42));
        assert_eq!(long1.to_i64_unchecked(), 42);
        assert_eq!(long1.to_u64(), Some(42));
        assert_eq!(long1.to_u64_unchecked(), 42);
    }

    #[test]
    fn test_long_nonnegative_long() {
        let long_value: i64 = U32MAX_AS_I64 + 42;
        let long1 = LongDataValue::new(long_value);

        assert_eq!(long1.lexical_value(), long_value.to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#long".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::NonNegativeLong);

        assert_eq!(long1.fits_into_i32(), false);
        assert_eq!(long1.fits_into_u32(), false);
        assert_eq!(long1.fits_into_i64(), true);
        assert_eq!(long1.fits_into_u64(), true);

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), None);
        assert_eq!(long1.to_i64(), Some(long_value));
        assert_eq!(long1.to_i64_unchecked(), long_value);
        assert_eq!(long1.to_u64(), Some(long_value as u64));
        assert_eq!(long1.to_u64_unchecked(), long_value as u64);
    }

    #[test]
    fn test_long_unsigned_int() {
        let long_value: i64 = I32MAX_AS_I64 + 42;
        let long1 = LongDataValue::new(long_value);

        assert_eq!(long1.lexical_value(), long_value.to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#long".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::UnsignedInt);

        assert_eq!(long1.fits_into_i32(), false);
        assert_eq!(long1.fits_into_u32(), true);
        assert_eq!(long1.fits_into_i64(), true);
        assert_eq!(long1.fits_into_u64(), true);

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), Some((I32MAX_AS_U64 + 42) as u32));
        assert_eq!(long1.to_u32_unchecked(), long_value as u32);
        assert_eq!(long1.to_i64(), Some(long_value));
        assert_eq!(long1.to_i64_unchecked(), long_value);
        assert_eq!(long1.to_u64(), Some(long_value as u64));
        assert_eq!(long1.to_u64_unchecked(), long_value as u64);
    }

    #[test]
    fn test_long_nonnegative_int() {
        let long1 = LongDataValue::new(42);

        assert_eq!(long1.lexical_value(), "42".to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#int".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::NonNegativeInt);

        assert_eq!(long1.fits_into_i32(), true);
        assert_eq!(long1.fits_into_u32(), true);
        assert_eq!(long1.fits_into_i64(), true);
        assert_eq!(long1.fits_into_u64(), true);

        assert_eq!(long1.to_i32(), Some(42));
        assert_eq!(long1.to_i32_unchecked(), 42);
        assert_eq!(long1.to_u32(), Some(42));
        assert_eq!(long1.to_u32_unchecked(), 42);
        assert_eq!(long1.to_i64(), Some(42));
        assert_eq!(long1.to_i64_unchecked(), 42);
        assert_eq!(long1.to_u64(), Some(42));
        assert_eq!(long1.to_u64_unchecked(), 42);
    }

    #[test]
    fn test_long_negative_int() {
        let long_value: i64 = -42;
        let long1 = LongDataValue::new(long_value);

        assert_eq!(long1.lexical_value(), long_value.to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#int".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::Int);

        assert_eq!(long1.fits_into_i32(), true);
        assert_eq!(long1.fits_into_u32(), false);
        assert_eq!(long1.fits_into_i64(), true);
        assert_eq!(long1.fits_into_u64(), false);

        assert_eq!(long1.to_i32(), Some(-42));
        assert_eq!(long1.to_i32_unchecked(), -42);
        assert_eq!(long1.to_u32(), None);
        assert_eq!(long1.to_i64(), Some(long_value));
        assert_eq!(long1.to_i64_unchecked(), long_value);
        assert_eq!(long1.to_u64(), None);
    }

    #[test]
    fn test_long_negative_long() {
        let long_value: i64 = I32MIN_AS_I64 - 42;
        let long1 = LongDataValue::new(long_value);

        assert_eq!(long1.lexical_value(), long_value.to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#long".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::Long);

        assert_eq!(long1.fits_into_i32(), false);
        assert_eq!(long1.fits_into_u32(), false);
        assert_eq!(long1.fits_into_i64(), true);
        assert_eq!(long1.fits_into_u64(), false);

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), None);
        assert_eq!(long1.to_i64(), Some(long_value));
        assert_eq!(long1.to_i64_unchecked(), long_value);
        assert_eq!(long1.to_u64(), None);
    }
}
