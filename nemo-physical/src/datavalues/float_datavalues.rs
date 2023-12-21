//! This module provides implementations [`super::DataValue`]s that represent finite-valued floating
//! point numbers. That means that NaN and positive or negative infinite are not accepted here.
//! This ensures smooth arithmetic and comparison operations are possible.
//!
//! By convention (following XML Schema), we consider the value spaces of floats of different precisions
//! to be disjoint, and also to be dijoint with any integer domain.

use crate::datatypes::{Double, Float};

use super::{DataValue, DataValueCreationError, ValueDomain};

/// Physical representation of a 32bit floating point number as an [Float].
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FloatDataValue(Float);

impl FloatDataValue {
    /// Create a new [FloatDataValue].
    pub fn new(value: Float) -> Self {
        Self(value)
    }

    /// Wraps the given [`f32`]-`value` as a value over [`Float`].
    ///
    /// # Errors
    /// The given `value` is NaN.
    pub fn from_f32(value: f32) -> Result<Self, DataValueCreationError> {
        if let Ok(result) = Float::new(value) {
            Ok(Self(result))
        } else {
            Err(DataValueCreationError::NonFiniteFloat {})
        }
    }

    /// Wraps the given [`f32`]-`value` as a value over [`Float`].
    ///
    /// # Panics
    /// The given `value` is NaN.
    pub fn from_f32_unchecked(value: f32) -> Self {
        if let Ok(result) = Float::new(value) {
            Self(result)
        } else {
            panic!("Floating point number must not be NaN.");
        }
    }
}

impl DataValue for FloatDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        self.0.to_string()
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Float
    }

    fn to_float_unchecked(&self) -> Float {
        self.0
    }
}

impl Eq for FloatDataValue {} // Possible since we exclude NaNs

/// Physical representation of a 64bit floating point number as an f64.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DoubleDataValue(Double);

impl DoubleDataValue {
    /// Create a new [DoubleDataValue].
    pub fn new(value: Double) -> Self {
        Self(value)
    }

    /// Wraps the given [`f64`]-`value` as a value over [`Double`].
    ///
    /// # Errors
    /// The given `value` is NaN or an infinity.
    pub fn from_f64(value: f64) -> Result<Self, DataValueCreationError> {
        if let Ok(result) = Double::new(value) {
            Ok(Self(result))
        } else {
            Err(DataValueCreationError::NonFiniteFloat {})
        }
    }

    /// Wraps the given [`f64`]-`value` as a value over [`Double`].
    ///
    /// # Panics
    /// The given `value` is [`f64::NAN`].
    pub fn from_f64_unchecked(value: f64) -> Self {
        if let Ok(result) = Double::new(value) {
            Self(result)
        } else {
            panic!("Floating point number must represent a finite value (neither infinity nor NaN are allowed).");
        }
    }
}

impl DataValue for DoubleDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        self.0.to_string()
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Double
    }

    fn to_double_unchecked(&self) -> Double {
        self.0
    }

    fn canonical_string(&self) -> String {
        "\"".to_owned() + &self.0.to_string() + "\"^^<" + &self.datatype_iri() + ">"
    }
}

impl Eq for DoubleDataValue {} // Possible since we exclude NaNs

#[cfg(test)]
mod test {
    use super::DoubleDataValue;
    use crate::{
        datatypes::Double,
        datavalues::{DataValue, DataValueCreationError, ValueDomain},
    };

    #[test]
    fn datavalue_double() {
        let value = Double::new(2.34e3).unwrap();
        let double = DoubleDataValue::new(value);

        assert_eq!(double.lexical_value(), value.to_string());
        assert_eq!(
            double.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#double".to_string()
        );
        assert_eq!(double.value_domain(), ValueDomain::Double);
        assert_eq!(double.canonical_string(), "\"".to_string() + &value.to_string() + "\"^^<http://www.w3.org/2001/XMLSchema#double>");

        assert_eq!(double.to_double(), Some(value));
        assert_eq!(double.to_double_unchecked(), value);
    }

    #[test]
    fn datavalue_double_nan() {
        let result = DoubleDataValue::from_f64(f64::NAN);
        assert_eq!(
            result.err().unwrap(),
            DataValueCreationError::NonFiniteFloat {}
        );
    }

    #[test]
    fn datavalue_double_pos_inf() {
        let value = Double::new(f64::INFINITY).unwrap();
        let double = DoubleDataValue::new(value);

        assert_eq!(double.to_double(), Some(value));
        assert_eq!(double.to_double_unchecked(), value);
    }

    #[test]
    fn datavalue_double_neg_inf() {
        let value = Double::new(f64::NEG_INFINITY).unwrap();
        let double = DoubleDataValue::new(value);

        assert_eq!(double.to_double(), Some(value));
        assert_eq!(double.to_double_unchecked(), value);
    }
}
