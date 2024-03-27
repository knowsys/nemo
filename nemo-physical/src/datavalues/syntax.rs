//! Constants for strings that are relevant to the syntax of data values.
//! These are kept in one location, since they are required in various
//! places related to parsing and display.

/// Opening delimiter for tuples.
pub const DELIM_TUPLE_OPEN: &str = "(";
/// Closing delimiter for tuples.
pub const DELIM_TUPLE_CLOSE: &str = ")";
/// Opening delimiter for tuples.
pub const DELIM_MAP_OPEN: &str = "{";
/// Closing delimiter for tuples.
pub const DELIM_MAP_CLOSE: &str = "}";
/// Assignment operator for maps.
pub const MAP_ASSIGN: &str = "=";
/// Separator for key-value pairs in maps
pub const MAP_SEPARATOR: &str = ",";
/// Separator for elements of tuples
pub const TUPLE_SEPARATOR: &str = ",";

/// Initial part of IRI in all XML Schema types:
pub const XSD_PREFIX: &str = "http://www.w3.org/2001/XMLSchema#";
