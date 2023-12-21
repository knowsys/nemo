use crate::{
    datavalues::{AnyDataValue, DoubleDataValue, FloatDataValue},
    dictionary::DvDict,
    management::database::Dict,
};

use super::{Double, Float};

/// Trait defined by types that can be converted into an [AnyDataValue]
/// by potentially using a [MetaDictionary].
///
/// TODO: Is this really a good design? We already have methods for creating AnyDatavalue from
/// a variety of types, and from arbitrary StorageValueT. Why do we need additional conversions
/// that duplicate code of the cases already implemented?  
pub trait IntoDataValue {
    /// Convert this value into the appropriate [AnyDataValue].
    fn into_datavalue(self, dictionary: &Dict) -> Option<AnyDataValue>;
}

impl IntoDataValue for u8 {
    fn into_datavalue(self, dictionary: &Dict) -> Option<AnyDataValue> {
        dictionary.id_to_datavalue(self as usize)
    }
}

impl IntoDataValue for u16 {
    fn into_datavalue(self, dictionary: &Dict) -> Option<AnyDataValue> {
        dictionary.id_to_datavalue(self as usize)
    }
}

impl IntoDataValue for u32 {
    fn into_datavalue(self, dictionary: &Dict) -> Option<AnyDataValue> {
        dictionary.id_to_datavalue(self as usize)
    }
}

impl IntoDataValue for u64 {
    fn into_datavalue(self, dictionary: &Dict) -> Option<AnyDataValue> {
        dictionary.id_to_datavalue(self as usize)
    }
}

impl IntoDataValue for usize {
    fn into_datavalue(self, dictionary: &Dict) -> Option<AnyDataValue> {
        dictionary.id_to_datavalue(self)
    }
}

impl IntoDataValue for i8 {
    fn into_datavalue(self, _dictionary: &Dict) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_integer_from_i64(self as i64))
    }
}

impl IntoDataValue for i16 {
    fn into_datavalue(self, _dictionary: &Dict) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_integer_from_i64(self as i64))
    }
}

impl IntoDataValue for i32 {
    fn into_datavalue(self, _dictionary: &Dict) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_integer_from_i64(self as i64))
    }
}

impl IntoDataValue for i64 {
    fn into_datavalue(self, _dictionary: &Dict) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_integer_from_i64(self))
    }
}

impl IntoDataValue for Float {
    fn into_datavalue(self, _dictionary: &Dict) -> Option<AnyDataValue> {
        Some(AnyDataValue::Float(FloatDataValue::from_f32_unchecked(
            self.into(),
        )))
    }
}

impl IntoDataValue for Double {
    fn into_datavalue(self, _dictionary: &Dict) -> Option<AnyDataValue> {
        Some(AnyDataValue::Double(DoubleDataValue::from_f64_unchecked(
            self.into(),
        )))
    }
}
