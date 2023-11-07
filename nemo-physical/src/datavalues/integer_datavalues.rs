//! This module provides implementations [`super::DataValue`]s that represent integer numbers.
//! Integers have a more complex domain hierarchy than most other kinds of data, and the related
//! logic is collected here as well.

use super::{DataValue,ValueDomain};

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
const I64MIN_AS_I64: i64 = std::i64::MIN;

/// Physical representation of an integer as an i64.
#[derive(Debug, Clone, Copy)]
pub struct Long(i64);

impl DataValue for Long {
    fn datatype_iri(&self) -> String {
        match self.value_domain() {
            ValueDomain::Long => "http://www.w3.org/2001/XMLSchema#long".to_owned(),
            ValueDomain::PositiveLong => "http://www.w3.org/2001/XMLSchema#long".to_owned(),
            ValueDomain::UnsignedInt => "http://www.w3.org/2001/XMLSchema#long".to_owned(),
            ValueDomain::Int => "http://www.w3.org/2001/XMLSchema#int".to_owned(),
            ValueDomain::PositiveInt => "http://www.w3.org/2001/XMLSchema#int".to_owned(),
            _ => panic!("Unexpected value domain {:?} for i64", self.value_domain()),
        }
    }

    fn lexical_value(&self) -> String {
        self.0.to_string()
    }

    /// The function needs to find the tightest domain for the given value.
    fn value_domain(&self) -> ValueDomain {
        if self.0 >= 0 {
            if self.0 <= I32MAX_AS_I64 {
                return ValueDomain::PositiveInt;
            } else if self.0 <= U32MAX_AS_I64 {
                return ValueDomain::UnsignedInt;
            } else {
                return ValueDomain::PositiveLong;
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

    fn to_i64(&self) -> Option<i64> {
        Some(self.0)
    }

    fn to_i64_unchecked(&self) -> i64 {
        self.0
    }

    fn to_i32_unchecked(&self) -> i32 {
        // TODO: Maybe give a more informative error message here.
        self.0.try_into().unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::{Long,I32MAX_AS_I64,U32MAX_AS_I64,I32MIN_AS_I64};
    use crate::datavalues::{DataValue,ValueDomain};

    #[test]
    fn test_long_positive_long() {
        let long_value: i64 =  U32MAX_AS_I64 + 42;
        let long1 = Long(long_value);

        assert_eq!(long1.lexical_value(), long_value.to_string());
        assert_eq!(long1.datatype_iri(), "http://www.w3.org/2001/XMLSchema#long".to_string());
        assert_eq!(long1.value_domain(), ValueDomain::PositiveLong);

        assert_eq!(long1.to_i64(), Some(long_value));
        assert_eq!(long1.to_i64_unchecked(), long_value);
        assert_eq!(long1.to_i32(), None);

        assert_eq!(long1.fits_into_i32(), false);
        assert_eq!(long1.fits_into_i64(), true);
    }


    #[test]
    fn test_long_unsigned_int() {
        let long_value: i64 =  I32MAX_AS_I64 + 42;
        let long1 = Long(long_value);

        assert_eq!(long1.lexical_value(), long_value.to_string());
        assert_eq!(long1.datatype_iri(), "http://www.w3.org/2001/XMLSchema#long".to_string());
        assert_eq!(long1.value_domain(), ValueDomain::UnsignedInt);

        assert_eq!(long1.to_i64(), Some(long_value));
        assert_eq!(long1.to_i64_unchecked(), long_value);
        assert_eq!(long1.to_i32(), None);

        assert_eq!(long1.fits_into_i32(), false);
        assert_eq!(long1.fits_into_i64(), true);
    }

    #[test]
    fn test_long_positive_int() {
        let long1 = Long(42);

        assert_eq!(long1.lexical_value(), "42".to_string());
        assert_eq!(long1.datatype_iri(), "http://www.w3.org/2001/XMLSchema#int".to_string());
        assert_eq!(long1.value_domain(), ValueDomain::PositiveInt);

        assert_eq!(long1.to_i64(), Some(42));
        assert_eq!(long1.to_i64_unchecked(), 42);
        assert_eq!(long1.to_i32(), Some(42));
        assert_eq!(long1.to_i32_unchecked(), 42);

        assert_eq!(long1.fits_into_i32(), true);
        assert_eq!(long1.fits_into_i64(), true);
    }

    #[test]
    fn test_long_negative_int() {
        let long_value: i64 =  -42;
        let long1 = Long(long_value);

        assert_eq!(long1.lexical_value(), long_value.to_string());
        assert_eq!(long1.datatype_iri(), "http://www.w3.org/2001/XMLSchema#int".to_string());
        assert_eq!(long1.value_domain(), ValueDomain::Int);

        assert_eq!(long1.to_i64(), Some(long_value));
        assert_eq!(long1.to_i64_unchecked(), long_value);
        assert_eq!(long1.to_i32(), Some(-42));
        assert_eq!(long1.to_i32_unchecked(), -42);

        assert_eq!(long1.fits_into_i32(), true);
        assert_eq!(long1.fits_into_i64(), true);
    }

    #[test]
    fn test_long_negative_long() {
        let long_value: i64 = I32MIN_AS_I64 - 42;
        let long1 = Long(long_value);

        assert_eq!(long1.lexical_value(), long_value.to_string());
        assert_eq!(long1.datatype_iri(), "http://www.w3.org/2001/XMLSchema#long".to_string());
        assert_eq!(long1.value_domain(), ValueDomain::Long);

        assert_eq!(long1.to_i64(), Some(long_value));
        assert_eq!(long1.to_i64_unchecked(), long_value);
        assert_eq!(long1.to_i32(), None);

        assert_eq!(long1.fits_into_i32(), false);
        assert_eq!(long1.fits_into_i64(), true);
    }
}