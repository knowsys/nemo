//! This module provides implementations [`super::DataValue`]s that represent integer numbers.
//! Integers have a more complex domain hierarchy than most other kinds of data, and the related
//! logic is collected here as well.

use super::{DataValue,ValueDomain};

/// Physical representation of an integer as an i64.
#[derive(Debug, Clone, Copy)]
pub struct Long(i64);

impl DataValue for Long {
    fn datatype_iri(&self) -> String {
        match self.value_domain() {
            ValueDomain::Long => "http://www.w3.org/2001/XMLSchema#long".to_owned(),
            ValueDomain::Int => "http://www.w3.org/2001/XMLSchema#int".to_owned(),
            _ => panic!("Unexpected value domain for i64"),
        }
    }

    fn lexical_value(&self) -> String {
        self.0.to_string()
    }

    /// The function needs to find the tightest domain for the given value.
    fn value_domain(&self) -> ValueDomain {
        if self.fits_into_i32() {
            ValueDomain::Int
        } else {
            ValueDomain::Long
        }
    }

    fn fits_into_i64(&self) -> bool {
        true
    }

     fn fits_into_i32(&self) -> bool {
        self.0 <= std::i32::MAX.into() && self.0 >= std::i32::MIN.into()
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
    use super::Long;
    use crate::datavalues::{DataValue,ValueDomain};


    #[test]
    fn test_big_long() {
        let long_value: i64 =  <i32 as Into<i64>>::into((std::i32::MAX)) + 42;
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

    #[test]
    fn test_small_long() {
        let long1 = Long(42);

        assert_eq!(long1.lexical_value(), "42".to_string());
        assert_eq!(long1.datatype_iri(), "http://www.w3.org/2001/XMLSchema#int".to_string());
        assert_eq!(long1.value_domain(), ValueDomain::Int);

        assert_eq!(long1.to_i64(), Some(42));
        assert_eq!(long1.to_i64_unchecked(), 42);
        assert_eq!(long1.to_i32(), Some(42));
        assert_eq!(long1.to_i32_unchecked(), 42);

        assert_eq!(long1.fits_into_i32(), true);
        assert_eq!(long1.fits_into_i64(), true);
    }
}