//! Constants for strings that are relevant to the syntax of data values.
//! These are kept in one location, since they are required in various
//! places related to parsing and display.

/// This module defines the syntax for tuple values
pub mod tuple {
    /// Opening delimiter for tuples.
    pub const OPEN: &str = "(";
    /// Closing delimiter for tuples.
    pub const CLOSE: &str = ")";
    /// Separator for elements of tuples
    pub const SEPARATOR: &str = ",";
}

/// This module defines the syntax for map values
pub mod map {
    /// Opening delimiter for tuples.
    pub const OPEN: &str = "{";
    /// Closing delimiter for tuples.
    pub const CLOSE: &str = "}";
    /// Assignment operator for maps.
    pub const KEY_VALUE_ASSIGN: &str = ":";
    /// Separator for key-value pairs in maps
    pub const SEPARATOR: &str = ",";
}

/// This module defines the boolean values
pub mod boolean {
    /// True
    pub const TRUE: &str = "true";
    /// False
    pub const FALSE: &str = "false";
}

/// This module defines the syntax for IRIs
pub mod iri {
    /// Opening delimiter for IRIs
    pub const OPEN: &str = "<";
    /// Closing delimiter for IRIs
    pub const CLOSE: &str = ">";
}

/// This module defines the syntax for strings
pub mod string {
    /// Language tag indicator after strings
    pub const LANG_TAG: &str = "@";
}

/// RDF datatype indicator
pub const RDF_DATATYPE_INDICATOR: &str = "^^";

/// Initial part of IRI in all XML Schema types:
pub const XSD_PREFIX: &str = "http://www.w3.org/2001/XMLSchema#";
