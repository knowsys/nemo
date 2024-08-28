//! This module provides implementations of [DataValue]s that represent integer numbers.
//!
//! Integers have a more complex domain hierarchy than most other kinds of data, and the related
//! logic is collected here as well.

use super::{DataValue, ValueDomain};

/// Maximal value of an u64, i.e. 2^64-1.
const U64MAX_AS_U64: u64 = u64::MAX;
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
//const I64MIN_AS_I64: i64 = i64::MIN;

/// Physical representation of an integer as an u64.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnsignedLongDataValue(u64);

impl UnsignedLongDataValue {
    /// Constructor.
    pub(crate) fn new(value: u64) -> Self {
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

    fn canonical_string(&self) -> String {
        "\"".to_owned() + &self.0.to_string() + "\"^^<" + &self.datatype_iri() + ">"
    }

    /// The function needs to find the tightest domain for the given value.
    fn value_domain(&self) -> ValueDomain {
        if self.0 <= I32MAX_AS_U64 {
            ValueDomain::NonNegativeInt
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

impl std::hash::Hash for UnsignedLongDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.0.hash(state);
    }
}

impl std::fmt::Display for UnsignedLongDataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Physical representation of an integer as an i64.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LongDataValue(i64);

impl LongDataValue {
    /// Constructor.
    pub(crate) fn new(value: i64) -> Self {
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

    fn canonical_string(&self) -> String {
        "\"".to_owned() + &self.0.to_string() + "\"^^<" + &self.datatype_iri() + ">"
    }

    /// The function needs to find the tightest domain for the given value.
    fn value_domain(&self) -> ValueDomain {
        if self.0 >= 0 {
            if self.0 <= I32MAX_AS_I64 {
                ValueDomain::NonNegativeInt
            } else if self.0 <= U32MAX_AS_I64 {
                return ValueDomain::UnsignedInt;
            } else {
                return ValueDomain::NonNegativeLong;
            }
        } else if self.0 >= I32MIN_AS_I64 {
            return ValueDomain::Int;
        } else {
            return ValueDomain::Long;
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

impl std::hash::Hash for LongDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.0.hash(state);
    }
}

impl std::fmt::Display for LongDataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod test {
    use super::{
        LongDataValue, UnsignedLongDataValue, I32MAX_AS_I64, I32MAX_AS_U64, I32MIN_AS_I64,
        U32MAX_AS_I64, U32MAX_AS_U64, U64MAX_AS_U64,
    };
    use crate::datavalues::{DataValue, ValueDomain};
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

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
        assert_eq!(
            long1.canonical_string(),
            "\"".to_string()
                + &value.to_string()
                + "\"^^<http://www.w3.org/2001/XMLSchema#unsignedLong>"
        );

        assert!(!long1.fits_into_i32());
        assert!(!long1.fits_into_u32());
        assert!(!long1.fits_into_i64());
        assert!(long1.fits_into_u64());

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), None);
        assert_eq!(long1.to_i64(), None);
        assert_eq!(long1.to_u64(), Some(value));
        assert_eq!(long1.to_u64_unchecked(), value);
    }

    #[test]
    fn test_unsigned_long_nonnegative_long() {
        let value_u64: u64 = U32MAX_AS_U64 + 42;
        let value_i64 = value_u64.try_into().expect("Value is small enough");

        let long1 = UnsignedLongDataValue::new(value_u64);

        assert_eq!(long1.lexical_value(), value_u64.to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#long".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::NonNegativeLong);

        assert!(!long1.fits_into_i32());
        assert!(!long1.fits_into_u32());
        assert!(long1.fits_into_i64());
        assert!(long1.fits_into_u64());

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), None);
        assert_eq!(long1.to_i64(), Some(value_i64));
        assert_eq!(long1.to_i64_unchecked(), value_i64);
        assert_eq!(long1.to_u64(), Some(value_u64));
        assert_eq!(long1.to_u64_unchecked(), value_u64);
    }

    #[test]
    fn test_unsigned_long_unsigned_int() {
        let value_u64: u64 = I32MAX_AS_U64 + 42;
        let value_i64 = value_u64.try_into().expect("Value is small enough");
        let value_u32 = value_u64.try_into().expect("Value is small enough");

        let long1 = UnsignedLongDataValue::new(value_u64);

        assert_eq!(long1.lexical_value(), value_u64.to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#long".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::UnsignedInt);

        assert!(!long1.fits_into_i32());
        assert!(long1.fits_into_u32());
        assert!(long1.fits_into_i64());
        assert!(long1.fits_into_u64());

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), Some(value_u32));
        assert_eq!(long1.to_u32_unchecked(), value_u32);
        assert_eq!(long1.to_i64(), Some(value_i64));
        assert_eq!(long1.to_i64_unchecked(), value_i64);
        assert_eq!(long1.to_u64(), Some(value_u64));
        assert_eq!(long1.to_u64_unchecked(), value_u64);
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

        assert!(long1.fits_into_i32());
        assert!(long1.fits_into_u32());
        assert!(long1.fits_into_i64());
        assert!(long1.fits_into_u64());

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
        let value_i64: i64 = U32MAX_AS_I64 + 42;
        let value_u64 = value_i64.try_into().expect("Value is small enough");

        let long1 = LongDataValue::new(value_i64);

        assert_eq!(long1.lexical_value(), value_i64.to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#long".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::NonNegativeLong);

        assert!(!long1.fits_into_i32());
        assert!(!long1.fits_into_u32());
        assert!(long1.fits_into_i64());
        assert!(long1.fits_into_u64());

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), None);
        assert_eq!(long1.to_i64(), Some(value_i64));
        assert_eq!(long1.to_i64_unchecked(), value_i64);
        assert_eq!(long1.to_u64(), Some(value_u64));
        assert_eq!(long1.to_u64_unchecked(), value_u64);
    }

    #[test]
    fn test_long_unsigned_int() {
        let value_i64: i64 = I32MAX_AS_I64 + 42;
        let value_u64 = value_i64.try_into().expect("Value is small_enough");
        let value_u32 = value_i64.try_into().expect("Value is small_enough");

        let long1 = LongDataValue::new(value_i64);

        assert_eq!(long1.lexical_value(), value_i64.to_string());
        assert_eq!(
            long1.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#long".to_string()
        );
        assert_eq!(long1.value_domain(), ValueDomain::UnsignedInt);
        assert_eq!(
            long1.canonical_string(),
            "\"".to_string()
                + &value_i64.to_string()
                + "\"^^<http://www.w3.org/2001/XMLSchema#long>"
        );

        assert!(!long1.fits_into_i32());
        assert!(long1.fits_into_u32());
        assert!(long1.fits_into_i64());
        assert!(long1.fits_into_u64());

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), Some(value_u32));
        assert_eq!(long1.to_u32_unchecked(), value_u32);
        assert_eq!(long1.to_i64(), Some(value_i64));
        assert_eq!(long1.to_i64_unchecked(), value_i64);
        assert_eq!(long1.to_u64(), Some(value_u64));
        assert_eq!(long1.to_u64_unchecked(), value_u64);
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

        assert!(long1.fits_into_i32());
        assert!(long1.fits_into_u32());
        assert!(long1.fits_into_i64());
        assert!(long1.fits_into_u64());

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

        assert!(long1.fits_into_i32());
        assert!(!long1.fits_into_u32());
        assert!(long1.fits_into_i64());
        assert!(!long1.fits_into_u64());

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

        assert!(!long1.fits_into_i32());
        assert!(!long1.fits_into_u32());
        assert!(long1.fits_into_i64());
        assert!(!long1.fits_into_u64());

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), None);
        assert_eq!(long1.to_i64(), Some(long_value));
        assert_eq!(long1.to_i64_unchecked(), long_value);
        assert_eq!(long1.to_u64(), None);
    }

    /// Test that distinct implementations that represent the same value
    /// have the same hash.
    #[test]
    fn test_hash_eq_contract() {
        let long = LongDataValue::new(42);
        let ulong = UnsignedLongDataValue::new(42);

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();

        long.hash(&mut hasher1);
        ulong.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }
}
