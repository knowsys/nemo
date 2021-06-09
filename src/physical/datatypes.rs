//! This module collects functionality specific to the supported primitive datatypes.

/// Module for defining [`DataTypeName`]
pub mod data_type_name;
pub use data_type_name::DataTypeName;
/// Module for defining [`Double`]
pub mod double;
pub use double::Double;
/// Module for defining [`Float`]
pub mod float;
pub use float::Float;
/// Module for defining [`FloatIsNaN`]
pub mod float_is_nan;
pub use float_is_nan::FloatIsNaN;