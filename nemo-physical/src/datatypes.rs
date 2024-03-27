//! This module collects functionality specific to the supported primitive datatypes.

/// Module for defining [StorageTypeName]
pub(crate) mod storage_type_name;
pub(crate) use storage_type_name::StorageTypeName;
/// Module for defining [StorageValueT]
pub(crate) mod storage_value;
pub(crate) use storage_value::StorageValueT;
/// Module for defining [Double]
pub mod double;
pub use double::Double;
/// Module for defining [Float]
pub mod float;
pub use float::Float;
/// Module for defining [FloatIsNaN]
pub(crate) mod float_is_nan;
pub(crate) use float_is_nan::FloatIsNaN;
/// Module for defining [Ring]
pub(crate) mod ring;
pub(crate) use ring::Ring;
/// Module for defining [Field]
pub(crate) mod field;
pub(crate) use field::Field;
/// Module for defining [FloorToUsize]
pub(crate) mod floor_to_usize;
pub(crate) use floor_to_usize::FloorToUsize;

/// Module for defining [ColumnDataType]
pub(crate) mod column_data_type;
pub(crate) use column_data_type::ColumnDataType;

/// Module defining the [RunLengthEncodable] trait and impls
pub(crate) mod run_length_encodable;
pub(crate) use run_length_encodable::RunLengthEncodable;

/// Module for creating AnyDataValue objects.
pub(crate) mod into_datavalue;
