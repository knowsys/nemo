//! This module collects functionality specific to the supported primitive datatypes.

/// Module for defining [`StorageTypeName`]
pub(crate) mod storage_type_name;
pub(crate) use storage_type_name::StorageTypeName;
/// Module for defining [`StorageValueT`]
pub mod storage_value;
pub use storage_value::StorageValueT;
/// Module for defining [`DataTypeName`]
pub mod data_type_name;
pub use data_type_name::DataTypeName;
/// Module for defining [`DataValueT`]
pub mod data_value;
pub use data_value::DataValueT;
/// Module for defining [`Double`]
pub mod double;
pub use double::Double;
/// Module for defining [`Float`]
pub mod float;
pub use float::Float;
/// Module for defining [`FloatIsNaN`]
pub mod float_is_nan;
pub use float_is_nan::FloatIsNaN;
/// Module for defining [`Ring`]
pub mod ring;
pub use ring::Ring;
/// Module for defining [`Field`]
pub mod field;
pub use field::Field;
/// Module for defining [`FloorToUsize`]
pub mod floor_to_usize;
pub use floor_to_usize::FloorToUsize;

/// Module for defining [`ColumnDataType`]
pub mod column_data_type;
pub use column_data_type::ColumnDataType;

/// Module defining the [`RunLengthEncodable`] trait and impls
pub mod run_length_encodable;
pub use run_length_encodable::RunLengthEncodable;

/// Module containing functionality relevant to casting types
pub mod casting;
pub use casting::ImplicitCastError;

/// Module for creating AnyDataValue objects.
pub mod into_datavalue;
