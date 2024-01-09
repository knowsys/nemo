//! This module provides implementations [`super::DataValue`]s that represent finite-valued floating
//! point numbers. That means that NaN and positive or negative infinite are not accepted here.
//! This ensures smooth arithmetic and comparison operations are possible.
//!
//! By convention (following XML Schema), we consider the value spaces of floats of different precisions
//! to be disjoint, and also to be dijoint with any integer domain.

use super::{DataValue, DataValueCreationError, ValueDomain};

/// Physical representation of a finite 32bit floating point number as an `f32`.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct FloatDataValue(f32);

impl FloatDataValue {
    /// Use the given f32 as a [`FloatDataValue`].
    ///
    /// # Errors
    /// The given `value` is NaN.
    pub fn from_f32(value: f32) -> Result<Self, DataValueCreationError> {
        if !value.is_finite() {
            return Err(DataValueCreationError::NonFiniteFloat {});
        }
        Ok(FloatDataValue(value))
    }

    /// Use the given f32 as a [`FloatDataValue`].
    ///
    /// # Panics
    /// The given `value` is NaN.
    pub fn from_f32_unchecked(value: f32) -> Self {
        if !value.is_finite() {
            panic!("floating point number must represent a finite value (neither infinity nor NaN are allowed).");
        }

        FloatDataValue(value)
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

    fn to_f32_unchecked(&self) -> f32 {
        self.0
    }

    fn canonical_string(&self) -> String {
        "\"".to_owned() + &self.0.to_string() + "\"^^<" + &self.datatype_iri() + ">"
    }
}

impl Eq for FloatDataValue {} // Possible since we exclude NaNs

impl std::hash::Hash for FloatDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.0.to_bits().hash(state);
    }
}

impl Ord for FloatDataValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if let Some(comp) = self.partial_cmp(other) {
            comp
        } else {
            unreachable!("all floats allowed for this type are comparable")
        }
    }
}

/// Physical representation of a finite 64bit floating point number as an f64.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct DoubleDataValue(f64);

impl DoubleDataValue {
    /// Use the given f64 as a [`DoubleDataValue`].
    ///
    /// # Errors
    /// The given `value` is NaN or an infinity.
    pub fn from_f64(value: f64) -> Result<Self, DataValueCreationError> {
        if !value.is_finite() {
            return Err(DataValueCreationError::NonFiniteFloat {});
        }
        Ok(DoubleDataValue(value))
    }

    /// Use the given f64 as a [`DoubleDataValue`].
    ///
    /// # Panics
    /// The given `value` is NaN or an infinity.
    pub fn from_f64_unchecked(value: f64) -> Self {
        if !value.is_finite() {
            panic!("floating point number must represent a finite value (neither infinity nor NaN are allowed).");
        }

        DoubleDataValue(value)
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

    fn to_f64_unchecked(&self) -> f64 {
        self.0
    }

    fn canonical_string(&self) -> String {
        "\"".to_owned() + &self.0.to_string() + "\"^^<" + &self.datatype_iri() + ">"
    }
}

impl Eq for DoubleDataValue {} // Possible since we exclude NaNs

impl std::hash::Hash for DoubleDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.0.to_bits().hash(state);
    }
}

impl Ord for DoubleDataValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if let Some(comp) = self.partial_cmp(other) {
            comp
        } else {
            unreachable!("all floats allowed for this type are comparable")
        }
    }
}

#[cfg(test)]
mod test {
    use super::DoubleDataValue;
    use crate::datavalues::{DataValue, DataValueCreationError, ValueDomain};

    #[test]
    fn datavalue_double() {
        let value = 2.34e3;
        let double = DoubleDataValue::from_f64_unchecked(value);

        assert_eq!(double.lexical_value(), value.to_string());
        assert_eq!(
            double.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#double".to_string()
        );
        assert_eq!(double.value_domain(), ValueDomain::Double);
        assert_eq!(
            double.canonical_string(),
            "\"".to_string() + &value.to_string() + "\"^^<http://www.w3.org/2001/XMLSchema#double>"
        );

        assert_eq!(double.to_f64(), Some(value));
        assert_eq!(double.to_f64_unchecked(), value);
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
        let result = DoubleDataValue::from_f64(f64::INFINITY);
        assert_eq!(
            result.err().unwrap(),
            DataValueCreationError::NonFiniteFloat {}
        );
    }

    #[test]
    fn datavalue_double_neg_inf() {
        let result = DoubleDataValue::from_f64(f64::NEG_INFINITY);
        assert_eq!(
            result.err().unwrap(),
            DataValueCreationError::NonFiniteFloat {}
        );
    }
}
