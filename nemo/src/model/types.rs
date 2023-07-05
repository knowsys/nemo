//! This module defines the logical datatype model

/// Module capturing complex types
pub mod complex_types;
/// Module for Type Errors
pub mod error;
/// Module capturing enums wrapping primitive logical values
pub mod primitive_logical_value;
/// Module capturing enum of primitive logical types
pub mod primitive_types;

// TODO: probably put this closer to the parser and also have a default prefix for xsd:
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
