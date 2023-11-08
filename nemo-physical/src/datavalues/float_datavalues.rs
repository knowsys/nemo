//! This module provides implementations [`super::DataValue`]s that represent finite-valued floating
//! point numbers. That means that NaN and positive or negative infinite are not accepted here.
//! This ensures smooth arithmetic and comparison operations are possible.
//! 
//! By convention (following XML Schema), we consider the value spaces of floats of different precisions
//! to be disjoint, and also dijoint with any integer domain.

use super::{DataValue,ValueDomain};
use std::fmt::{Formatter,Display};

/// Error that occurs when trying to use an infinity or NaN for creating a [`Double`].
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct NonFiniteFloatError;

impl Display for NonFiniteFloatError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "floating point number must represent a finite value (no infiniy, no NaN)"
        )
    }
}

impl  std::error::Error for NonFiniteFloatError {}

/// Physical representation of a 64bit floating point number as an f64.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Double(f64);

impl Double {
    /// Wraps the given [`f64`]-`value` as a value over [`Double`].
    ///
    /// # Errors
    /// The given `value` is NaN or an infinity.
    pub fn new(value: f64) -> Result<Double, NonFiniteFloatError> {
        if !value.is_finite() {
            return Err(NonFiniteFloatError.into());
        }

        Ok(Double(value))
    }

    /// Wraps the given [`f64`]-`value` as a value over [`Double`].
    ///
    /// # Panics
    /// The given `value` is [`f64::NAN`].
    pub fn from_number(value: f64) -> Double {
        if !value.is_finite() {
            panic!("Floating point number must represent a finite value (neither infinity nor NaN are allowed).");
        }

        Double(value)
    }
}

impl DataValue for Double {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        self.0.to_string()
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Double
    }

    fn to_f64_unchecked(&self) -> f64 {
        self.0
    }
}

#[cfg(test)]
mod test {
    use super::{Double,NonFiniteFloatError};
    use crate::datavalues::{DataValue,ValueDomain};

    #[test]
    fn test_double() {
        let value: f64 = 2.34e3;
        let double = Double::from_number(value);

        assert_eq!(double.lexical_value(), value.to_string());
        assert_eq!(double.datatype_iri(), "http://www.w3.org/2001/XMLSchema#double".to_string());
        assert_eq!(double.value_domain(), ValueDomain::Double);

        assert_eq!(double.to_f64(), Some(value));
        assert_eq!(double.to_f64_unchecked(), value);
    }

    #[test]
    fn test_double_nan() {
        let result = Double::new(f64::NAN);
        assert_eq!(result.err().unwrap(), NonFiniteFloatError);
    }

    #[test]
    fn test_double_pos_inf() {
        let result = Double::new(f64::INFINITY);
        assert_eq!(result.err().unwrap(), NonFiniteFloatError);
    }

    #[test]
    fn test_double_neg_inf() {
        let result = Double::new(f64::NEG_INFINITY);
        assert_eq!(result.err().unwrap(), NonFiniteFloatError);
    }
}