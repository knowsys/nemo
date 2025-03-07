//! This module implements the numeric functions for [AnyDataValue]s.
//!
//! In general, the implementation refers to the underlying implementation
//! for [StorageValueT]s and returns `None` for any non-numeric type.

use crate::{
    datavalues::{AnyDataValue, DataValue, ValueDomain},
    storagevalues::{double::Double, float::Float, storagevalue::StorageValueT},
};

use super::OperableNumeric;

/// Convert numeric [AnyDataValue]s into [StorageValueT].
fn any_to_storage_value_t(value: &AnyDataValue) -> Option<StorageValueT> {
    match value.value_domain() {
        ValueDomain::Tuple
        | ValueDomain::Map
        | ValueDomain::Null
        | ValueDomain::Boolean
        | ValueDomain::PlainString
        | ValueDomain::LanguageTaggedString
        | ValueDomain::Other
        | ValueDomain::Iri => None,
        ValueDomain::Float => Some(StorageValueT::Float(Float::new_unchecked(
            value.to_f32_unchecked(),
        ))),
        ValueDomain::Double => Some(StorageValueT::Double(Double::new_unchecked(
            value.to_f64_unchecked(),
        ))),
        ValueDomain::UnsignedLong => None, // numeric, but cannot represented in NumericValue
        ValueDomain::NonNegativeLong
        | ValueDomain::UnsignedInt
        | ValueDomain::NonNegativeInt
        | ValueDomain::Long
        | ValueDomain::Int => Some(StorageValueT::Int64(value.to_i64_unchecked())),
    }
}

/// Convert numeric [StorageValueT] back to [AnyDataValue].
fn storage_value_t_to_any(value: StorageValueT) -> AnyDataValue {
    match value {
        StorageValueT::Id32(_) | StorageValueT::Id64(_) => {
            unreachable!("function expects numeric value")
        }
        StorageValueT::Int64(integer) => AnyDataValue::new_integer_from_i64(integer),
        StorageValueT::Float(float) => AnyDataValue::new_float_from_f32(f32::from(float))
            .expect("function expected valid numeric values"),
        StorageValueT::Double(double) => AnyDataValue::new_double_from_f64(f64::from(double))
            .expect("function expected valid numeric values"),
    }
}

macro_rules! implement_unary {
    ($name:ident) => {
        fn $name(parameter: Self) -> Option<Self> {
            let parameter = any_to_storage_value_t(&parameter)?;
            let result = StorageValueT::$name(parameter)?;

            Some(storage_value_t_to_any(result))
        }
    };
}

macro_rules! implement_binary {
    ($name:ident) => {
        fn $name(first: Self, second: Self) -> Option<Self> {
            let first = any_to_storage_value_t(&first)?;
            let second = any_to_storage_value_t(&second)?;

            let result = StorageValueT::$name(first, second)?;

            Some(storage_value_t_to_any(result))
        }
    };
}

macro_rules! implement_nary {
    ($name:ident) => {
        fn $name(parameters: &[Self]) -> Option<Self> {
            let parameters = parameters
                .iter()
                .map(|parameter| any_to_storage_value_t(parameter))
                .collect::<Option<Vec<StorageValueT>>>()?;

            let result = StorageValueT::$name(&parameters)?;

            Some(storage_value_t_to_any(result))
        }
    };
}

impl OperableNumeric for AnyDataValue {
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

    fn numeric_less_than(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        let first = any_to_storage_value_t(&first)?;
        let second = any_to_storage_value_t(&second)?;

        Some(if first < second {
            AnyDataValue::new_boolean(true)
        } else {
            AnyDataValue::new_boolean(false)
        })
    }

    fn numeric_less_than_eq(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        let first = any_to_storage_value_t(&first)?;
        let second = any_to_storage_value_t(&second)?;

        Some(if first <= second {
            AnyDataValue::new_boolean(true)
        } else {
            AnyDataValue::new_boolean(false)
        })
    }

    fn numeric_greater_than(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        let first = any_to_storage_value_t(&first)?;
        let second = any_to_storage_value_t(&second)?;

        Some(if first > second {
            AnyDataValue::new_boolean(true)
        } else {
            AnyDataValue::new_boolean(false)
        })
    }

    fn numeric_greater_than_eq(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        let first = any_to_storage_value_t(&first)?;
        let second = any_to_storage_value_t(&second)?;

        Some(if first >= second {
            AnyDataValue::new_boolean(true)
        } else {
            AnyDataValue::new_boolean(false)
        })
    }
}
