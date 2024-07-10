//! This module defines constants relating to the value formats accepted by import and export directives.

/// The name of the general, best-effort value format. Importers/exporters suporting this format will usually
/// accept "any" input value and interpret it in the most natural way. Likewise, any value should be writable
/// in this format.
pub(crate) const VALUE_FORMAT_ANY: &str = "any";
/// The name of the value format that interprets all values as plain strings. Importers/exporters suporting this
/// format will usually accept any input value and interpret it as strings in the most literal way. Only strings
/// can be written in this format.
pub(crate) const VALUE_FORMAT_STRING: &str = "string";
/// The name of the value format that interprets values as integers whenever possible. Importers/exporters suporting
/// this format will usually only accept input values that are formatted like integers. Conversely, only integer values
/// can be written in this format.
pub(crate) const VALUE_FORMAT_INT: &str = "int";
/// The name of the value format that interprets values as double-precision floating point numbers whenever possible.
/// Importers/exporters suporting this format will usually only accept input values that are formatted like decimal numbers,
/// integers, or floating-point numbers in scientific notation. Conversely, only double values
/// can be written in this format.
pub(crate) const VALUE_FORMAT_DOUBLE: &str = "double";
/// The name of the special value format that indicates that a vlaue should be ignored altogether.
/// The respective column/parameter will be skiped in reading/writing.
pub(crate) const VALUE_FORMAT_SKIP: &str = "skip";
