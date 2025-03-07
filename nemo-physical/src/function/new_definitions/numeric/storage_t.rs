//! This module implements the numeric functions for [StorageValueT]s.
//!
//! In general, they are implemented by either
//!     * performing the operation on the underlying storage value in case
//!       all input parameters are of the same type, or by
//!     * casting all parameters to [Double] before performing the operation

use crate::{
    function::new_definitions::casting::storage_t::{
        casting_float_into_double, casting_integer_into_double,
    },
    storagevalues::{double::Double, float::Float, storagevalue::StorageValueT},
};

use super::OperableNumeric;

/// Numeric value
///
/// Types in this enum allow for numeric operations to be performed on them
enum NumericValue {
    Integer(i64),
    Float(Float),
    Double(Double),
}

impl NumericValue {
    /// Convert a [StorageValueT] into a [NumericValue].
    pub fn from_storage_value_t(value: StorageValueT) -> Option<Self> {
        Some(match value {
            StorageValueT::Int64(integer) => NumericValue::Integer(integer),
            StorageValueT::Float(float) => NumericValue::Float(float),
            StorageValueT::Double(double) => NumericValue::Double(double),
            StorageValueT::Id32(_) | StorageValueT::Id64(_) => return None,
        })
    }
}

/// Defines a pair of values on which numeric functions are defined
enum NumericPair {
    Integer(i64, i64),
    Float(Float, Float),
    Double(Double, Double),
}

impl NumericPair {
    fn storage_value_t_to_double(value: StorageValueT) -> Option<Double> {
        Some(match value {
            StorageValueT::Int64(integer) => casting_integer_into_double(integer),
            StorageValueT::Float(float) => casting_float_into_double(float),
            StorageValueT::Double(double) => double,
            StorageValueT::Id32(_) | StorageValueT::Id64(_) => return None,
        })
    }

    /// Create a [NumericPair] from a pair of [StorageValueT]s.
    pub fn from_storage_value_t(first: StorageValueT, second: StorageValueT) -> Option<Self> {
        Some(match (first, second) {
            (StorageValueT::Double(left), StorageValueT::Double(right)) => {
                NumericPair::Double(left, right)
            }
            (StorageValueT::Float(left), StorageValueT::Float(right)) => {
                NumericPair::Float(left, right)
            }
            (StorageValueT::Int64(left), StorageValueT::Int64(right)) => {
                NumericPair::Integer(left, right)
            }
            _ => NumericPair::Double(
                Self::storage_value_t_to_double(first)?,
                Self::storage_value_t_to_double(second)?,
            ),
        })
    }
}

/// Defines a list of values on which numeric functions are defined
enum NumericList {
    Integer(Vec<i64>),
    Float(Vec<Float>),
    Double(Vec<Double>),
}

impl NumericList {
    /// Collects a list of [StorageValueT] into a [NumericList].
    fn collect_list_same_type(result: &mut NumericList, values: &[StorageValueT]) {
        match result {
            NumericList::Integer(vec) => {
                *vec = values
                    .iter()
                    .map(|value| {
                        i64::try_from(*value).expect("all values must be of the same type")
                    })
                    .collect()
            }
            NumericList::Float(vec) => {
                *vec = values
                    .iter()
                    .map(|value| {
                        Float::try_from(*value).expect("all values must be of the same type")
                    })
                    .collect()
            }
            NumericList::Double(vec) => {
                *vec = values
                    .iter()
                    .map(|value| {
                        Double::try_from(*value).expect("all values must be of the same type")
                    })
                    .collect()
            }
        }
    }

    /// Collects a list of [StorageValueT] into a [NumericList].
    ///
    /// If input parameters are of different numeric types then they will be cast to [Double].
    fn collect_list_cast(result: &mut NumericList, values: &[StorageValueT]) {
        let doubles = values
            .iter()
            .map(|value| match value {
                StorageValueT::Id32(_) | StorageValueT::Id64(_) => unreachable!(),
                StorageValueT::Int64(integer) => casting_integer_into_double(*integer),
                StorageValueT::Float(float) => casting_float_into_double(*float),
                StorageValueT::Double(double) => *double,
            })
            .collect::<Vec<Double>>();

        *result = NumericList::Double(doubles)
    }

    /// Convert a list of [StorageValueT] into a [NumericList].
    pub fn from_storage_value_t(values: &[StorageValueT]) -> Option<Self> {
        let mut result = if let Some(value) = values.first() {
            match value {
                StorageValueT::Id32(_) | StorageValueT::Id64(_) => return None,
                StorageValueT::Int64(_) => NumericList::Integer(Vec::with_capacity(values.len())),
                StorageValueT::Float(_) => NumericList::Float(Vec::with_capacity(values.len())),
                StorageValueT::Double(_) => NumericList::Double(Vec::with_capacity(values.len())),
            }
        } else {
            return None;
        };

        let same_type = values
            .iter()
            .all(|other| other.storage_type() == values[0].storage_type());

        if same_type {
            Self::collect_list_same_type(&mut result, values);
        } else {
            Self::collect_list_cast(&mut result, values);
        }

        Some(result)
    }
}

macro_rules! implement_unary {
    ($name: ident) => {
        fn $name(parameter: Self) -> Option<Self> {
            let numeric_value = NumericValue::from_storage_value_t(parameter)?;

            Some(match numeric_value {
                NumericValue::Integer(parameter) => StorageValueT::from(i64::$name(parameter)?),
                NumericValue::Float(parameter) => StorageValueT::from(Float::$name(parameter)?),
                NumericValue::Double(parameter) => StorageValueT::from(Double::$name(parameter)?),
            })
        }
    };
}

macro_rules! implement_binary {
    ($name:ident) => {
        fn $name(first: Self, second: Self) -> Option<Self> {
            let pair = NumericPair::from_storage_value_t(first, second)?;

            Some(match pair {
                NumericPair::Integer(first, second) => {
                    StorageValueT::from(i64::$name(first, second)?)
                }
                NumericPair::Float(first, second) => {
                    StorageValueT::from(Float::$name(first, second)?)
                }
                NumericPair::Double(first, second) => {
                    StorageValueT::from(Double::$name(first, second)?)
                }
            })
        }
    };
}

macro_rules! implement_nary {
    ($name:ident) => {
        fn $name(parameters: &[Self]) -> Option<Self> {
            let list = NumericList::from_storage_value_t(parameters)?;

            Some(match list {
                NumericList::Integer(list) => StorageValueT::from(i64::$name(&list)?),
                NumericList::Float(list) => StorageValueT::from(Float::$name(&list)?),
                NumericList::Double(list) => StorageValueT::from(Double::$name(&list)?),
            })
        }
    };
}

impl OperableNumeric for StorageValueT {
    implement_unary!(numeric_absolute);
    implement_unary!(numeric_negation);
    implement_unary!(numeric_squareroot);
    implement_unary!(numeric_sine);
    implement_unary!(numeric_cosine);
    implement_unary!(numeric_tangent);
    implement_unary!(numeric_round);
    implement_unary!(numeric_ceil);
    implement_unary!(numeric_floor);

    implement_binary!(numeric_addition);
    implement_binary!(numeric_subtraction);
    implement_binary!(numeric_multiplication);
    implement_binary!(numeric_division);
    implement_binary!(numeric_logarithm);
    implement_binary!(numeric_power);
    implement_binary!(numeric_remainder);
    implement_binary!(numeric_bit_shift_left);
    implement_binary!(numeric_bit_shift_right);
    implement_binary!(numeric_bit_shift_right_unsigned);

    implement_nary!(numeric_sum);
    implement_nary!(numeric_product);
    implement_nary!(numeric_minimum);
    implement_nary!(numeric_maximum);
    implement_nary!(numeric_bit_and);
    implement_nary!(numeric_bit_or);
    implement_nary!(numeric_bit_xor);
    implement_nary!(numeric_lukasiewicz);
}
