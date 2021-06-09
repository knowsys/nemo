//! This module collects functionality specific to the supported primitive datatypes.

/// Module for defining [`self::DataTypeName`]
pub mod data_type_name;
pub use data_type_name::DataTypeName;
/// Module for defining [`self::Double`]
pub mod double;
pub use double::Double;
/// Module for defining [`self::Float`]
pub mod float;
pub use float::Float;
/// Module for defining [`self::FloatIsNaN`]
pub mod float_is_nan;
pub use float_is_nan::FloatIsNaN;