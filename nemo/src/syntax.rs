//! The Syntax constants for the nemo language.
//!
//! Every utilization of syntax (e.g. parsing or formatting values to
//! string representation) has to reference the constants defined
//! in this module and must not use strings directly.

/// The token used to terminate statements.
/// Although comments often get treated as statements, they don't get
/// terminated with this token.
/// The terminated statements are directives, facts and rules.
pub const STATEMENT_DELIMITER: &str = ".";
/// The token used to separate elements in a sequence.
pub const SEQUENCE_SEPARATOR: &str = ",";

pub mod directive {
    //! This module contains the syntax definitions for directives.

    /// The token identifying a directive keyword.
    pub const INDICATOR_TOKEN: &str = "@";

    /// The string used in the keyword for the base directive.
    pub const BASE: &str = "base";

    /// The string used in the keyword for the prefix directive.
    pub const PREFIX: &str = "prefix";

    /// The token used to separate prefix and name
    pub const NAMESPACE_SEPARATOR: &str = ":";

    /// The token used to assign the prefix in the prefix directive.
    pub const PREFIX_ASSIGNMENT: &str = ":";

    /// The string used in the keyword for the import directive.
    pub const IMPORT: &str = "import";

    /// The token used to assign an import configuration to a predicate.
    pub const IMPORT_ASSIGNMENT: &str = ":-";

    /// The string used in the keyword for the export directive.
    pub const EXPORT: &str = "export";

    /// The token used to assign an export configuration to a predicate.
    pub const EXPORT_ASSIGNMENT: &str = ":-";

    /// The string used in the keyword for the declare directive.
    pub const DECLARE: &str = "declare";

    /// Separator for name datatype pairs in declare directives.
    pub const NAME_DATATYPE_SEPARATOR: &str = ":";

    /// The token used to separate the name and the datatype.
    pub const DECLARE_ASSIGNMENT: &str = ":";

    /// The string used in the keyword for the output directive.
    pub const OUTPUT: &str = "output";

    /// Syntax elements relating to import/export value formats.
    pub mod value_formats {
        /// The string used to represent the import/export format any
        pub const ANY: &str = "any";
        /// The string used to represent the import/export format string
        pub const STRING: &str = "string";
        /// The string used to represent the import/export format integer
        pub const INT: &str = "int";
        /// The string used to represent the import/export format double
        pub const DOUBLE: &str = "double";
        /// The string used to represent the import/export format float
        pub const FLOAT: &str = "float";
        /// The string used to indicate that a column is skipped
        pub const SKIP: &str = "skip";
    }
}

pub mod rule {
    //! This module contains the syntax definitions for rules.

    /// The token separating the rule head from the rule body.
    pub const ARROW: &str = ":-";
    /// The opening token for attributes.
    pub const OPEN_ATTRIBUTE: &str = "#[";
    /// The closing token for attributes.
    pub const CLOSE_ATTRIBUTE: &str = "]";
}
pub mod fact {
    //! This module contains the syntax definitions for facts.
}

pub mod expression {
    //! This module contains the syntax definitions for expressions

    /// Syntax for atoms
    pub mod atom {
        /// Token for opening atom term sequence.
        pub const OPEN: &str = "(";
        /// Token to close atom term sequence.
        pub const CLOSE: &str = ")";
        /// Token to negate an atom
        pub const NEG: &str = "~";
    }

    /// Syntax for aggregates
    pub mod aggregate {
        /// Aggregate indicator
        pub const INDICATOR: &str = "#";
        /// Opening delimiter
        pub const OPEN: &str = "(";
        /// Closing delimiter
        pub const CLOSE: &str = ")";
        /// Symbol to separate distinct variables
        pub const SEPARATOR_DISTINCT: &str = ",";
    }

    /// Syntax for variables
    pub mod variable {
        /// Indicator for universal variables
        pub const UNIVERSAL_INDICATOR: &str = "?";
        /// Indicator for existential variables
        pub const EXISTENTIAL_INDICATOR: &str = "!";
    }

    /// Syntax for operations
    pub mod operation {
        /// Opening delimiter for argument list
        pub const OPEN: &str = "(";
        /// Closing delimiter for argument list
        pub const CLOSE: &str = ")";
    }

    /// Syntax for format strings
    pub mod format_string {
        /// Opening part of a format string
        pub const OPEN: &str = r#"f""#;
        /// Closing part of a format string
        pub const CLOSE: &str = r#"""#;

        /// Opening part of a multi-line format string
        pub const MULTILINE_OPEN: &str = r#"f""""#;
        /// Closing part of a multi-line format string
        pub const MULTILINE_CLOSE: &str = r#"""""#;

        /// Marker of the start of an expression
        pub const EXPRESSION_START: &str = "{";
        /// Marker of the end of an expression
        pub const EXPRESSION_END: &str = "}";
    }
}

pub mod comment {
    //! This module contains the syntax definitions for comments.

    /// The token identifying top level documentation comments.
    pub const TOP_LEVEL: &str = "%!";
    /// The token identifying documentation comments.
    pub const DOC_COMMENT: &str = "%%%";
    /// The token identifying normal comments.
    pub const COMMENT: &str = "%";
    /// The token to handle four comment symbols as a normal comment and not a doc comment.
    pub const COMMENT_LONG: &str = "%%%%";
    /// The continuation of the comment syntax
    pub(crate) const COMMENT_EXT: &str = "%";
    /// The opening token for closed comments.
    pub const CLOSED_OPEN: &str = "/*";
    /// The closing token for closed comments.
    pub const CLOSED_CLOSE: &str = "*/";
}

pub mod operator {
    //! This module defines mathematical operators.

    /// Greater than operation
    pub const GREATER: &str = ">";
    /// Greater or equal operation
    pub const GREATER_EQUAL: &str = ">=";
    /// Less than operation
    pub const LESS: &str = "<";
    /// Less or equal operation
    pub const LESS_EQUAL: &str = "<=";
    /// Is equal operation
    pub const EQUAL: &str = "=";
    /// Is not equal operation
    pub const UNEQUAL: &str = "!=";
    /// Addition
    pub const PLUS: &str = "+";
    /// Subtraction
    pub const MINUS: &str = "-";
    /// Multiplication
    pub const MUL: &str = "*";
    /// Division
    pub const DIV: &str = "/";
}

pub mod builtin {
    //! Collection of all builtin functions and aggregates.

    /// This module contains all strings of the supported builtin functions.
    pub mod function {
        /// Check if two values are equal to each other
        pub(crate) const EQUAL: &str = "EQUALITY";
        /// Check if two values are not equal to each other
        pub(crate) const UNEQUAL: &str = "UNEQUALITY";
        /// Check if a numeric value is greater than another
        pub(crate) const GREATER: &str = "NUMGREATER";
        /// Check if a numeric value is greater or equal to another
        pub(crate) const GREATEREQ: &str = "NUMGREATEREQ";
        /// Check if a numeric value is smaller than another
        pub(crate) const LESS: &str = "NUMLESS";
        /// Check if a numeric value is smaller or equal to another
        pub(crate) const LESSEQ: &str = "NUMLESSEQ";
        /// Check if value is an integer
        pub(crate) const IS_INTEGER: &str = "isInteger";
        /// Check if value is a 32bit floating point number
        pub(crate) const IS_FLOAT: &str = "isFloat";
        /// Check if value is a 64bit floating point number
        pub(crate) const IS_DOUBLE: &str = "isDouble";
        /// Check if value is an iri
        pub(crate) const IS_IRI: &str = "isIri";
        /// Check if value is numeric
        pub(crate) const IS_NUMERIC: &str = "isNumeric";
        /// Check if value is null
        pub(crate) const IS_NULL: &str = "isNull";
        /// Check if value is string
        pub(crate) const IS_STRING: &str = "isString";
        /// Compute the absoule value of a number
        pub(crate) const ABS: &str = "ABS";
        /// Compute the square root of a number
        pub(crate) const SQRT: &str = "SQRT";
        /// Logical negation of a boolean value
        pub(crate) const NOT: &str = "NOT";
        /// String representation of a value
        pub(crate) const FULLSTR: &str = "fullStr";
        /// Lexical value
        pub(crate) const STR: &str = "STR";
        /// Compute the sine of a value
        pub(crate) const SIN: &str = "SIN";
        /// Compute the cosine of a value
        pub(crate) const COS: &str = "COS";
        /// Compute the tangent of a value
        pub(crate) const TAN: &str = "TAN";
        /// Compute the length of a string
        pub(crate) const STRLEN: &str = "STRLEN";
        /// Compute the reverse of a string value
        pub(crate) const STRREV: &str = "STRREV";
        /// Replace characters in strings with their upper case version
        pub(crate) const UCASE: &str = "UCASE";
        /// Replace characters in strings with their lower case version
        pub(crate) const LCASE: &str = "LCASE";
        /// Return URI-encoded (percent-encoded) version of string
        pub(crate) const URIENCODE: &str = "URIENCODE";
        /// Return URI-decoded (percent-decoded) version of string
        pub(crate) const URIDECODE: &str = "URIDECODE";
        /// Round a value to the nearest integer
        pub(crate) const ROUND: &str = "ROUND";
        /// Round up to the nearest integer
        pub(crate) const CEIL: &str = "CEIL";
        /// Round down to the neatest integer
        pub(crate) const FLOOR: &str = "FLOOR";
        /// Return the datatype of the value
        pub(crate) const DATATYPE: &str = "DATATYPE";
        /// Return the language tag of the value
        pub(crate) const LANG: &str = "LANG";
        /// Convert the value to an integer
        pub(crate) const INT: &str = "INT";
        /// Convert the value to a 64bit floating point number
        pub(crate) const DOUBLE: &str = "DOUBLE";
        /// Convert the value to a 32bit floating point number
        pub(crate) const FLOAT: &str = "FLOAT";
        /// Convert a plain string into an IRI
        pub(crate) const IRI: &str = "IRI";
        /// Compute the logarithm of the numerical value
        pub(crate) const LOGARITHM: &str = "LOG";
        /// Raise the numerical value to a power
        pub(crate) const POW: &str = "POW";
        /// Compare two string values
        pub(crate) const COMPARE: &str = "COMPARE";
        /// Check if one string value is contained in another
        pub(crate) const CONTAINS: &str = "CONTAINS";
        /// Return a substring of a given string value
        pub(crate) const SUBSTR: &str = "SUBSTR";
        /// Check if a string starts with a certain string
        pub(crate) const STRSTARTS: &str = "STRSTARTS";
        /// Check if a string ends with a certain string
        pub(crate) const STRENDS: &str = "STRENDS";
        /// Return the first part of a string split by some other string
        pub(crate) const STRBEFORE: &str = "STRBEFORE";
        /// Return the second part of a string split by some other string
        pub(crate) const STRAFTER: &str = "STRAFTER";
        /// Check whether regex pattern holds in a given string
        pub(crate) const REGEX: &str = "REGEX";
        /// Compute the remainder of two numerical values
        pub(crate) const REM: &str = "REM";
        /// Compute the and on the bit representation of integer values
        pub(crate) const BITAND: &str = "BITAND";
        /// Compute the or on the bit representation of integer values
        pub(crate) const BITOR: &str = "BITOR";
        /// Compute the exclusive or on the bit representation of integer values
        pub(crate) const BITXOR: &str = "BITXOR";
        /// Compute the arithmetic bit shift left for integer values
        pub(crate) const BITSHL: &str = "BITSHL";
        /// Compute the unsigned bit shift right for integer values
        pub(crate) const BITSHRU: &str = "BITSHRU";
        /// Compute the arithmetic bit shift right for integer values
        pub(crate) const BITSHR: &str = "BITSHR";
        /// Compute the maximum of numeric values
        pub(crate) const MAX: &str = "MAX";
        /// Compute the minimum of numeric values
        pub(crate) const MIN: &str = "MIN";
        /// Compute the lukasiewicz norm of numeric values
        pub(crate) const LUKA: &str = "LUKA";
        /// Compute the sum of numerical values
        pub(crate) const SUM: &str = "SUM";
        /// Compute the product of numerical values
        pub(crate) const PRODUCT: &str = "PRODUCT";
        /// Compute the difference between to numeric values
        pub(crate) const SUBTRACTION: &str = "SUBTRACTION";
        /// Compute the quotient of two numeric values
        pub(crate) const DIVISION: &str = "DIVISION";
        /// Compute the multiplicative inverse of a numeric value
        pub(crate) const INVERTSIGN: &str = "INVERTSIGN";
        /// Compute the logical and between boolean values
        pub(crate) const AND: &str = "AND";
        /// Compute the logical or between boolean values
        pub(crate) const OR: &str = "OR";
        /// Compute the concatenation of string values
        pub(crate) const CONCAT: &str = "CONCAT";
    }

    /// This module contains all strings of the supported builtin aggregates.
    pub mod aggregate {
        /// Compute the sum of a list of numbers
        pub(crate) const SUM: &str = "sum";
        /// Count the number of values
        pub(crate) const COUNT: &str = "count";
        /// Return the minimum value
        pub(crate) const MIN: &str = "min";
        /// Return the maximum value
        pub(crate) const MAX: &str = "max";
    }
}
pub mod datatypes {
    //! This module defines the syntax for all supported datatypes.

    /// Can represent values of any type
    pub const ANY: &str = "any";
    /// Represents string values
    pub const STRING: &str = "string";
    /// Represents 64bit integer values
    pub const INT: &str = "int";
    /// Represents 64bit floating-point values
    pub const DOUBLE: &str = "double";
    /// Represents 32bit floating-point values
    pub const FLOAT: &str = "float";
}

pub mod datavalues {
    //! This module defines the syntax for datavalues.
    pub use nemo_physical::datavalues::syntax::boolean;
    pub use nemo_physical::datavalues::syntax::iri;
    pub use nemo_physical::datavalues::syntax::map;
    pub use nemo_physical::datavalues::syntax::string;
    pub use nemo_physical::datavalues::syntax::tuple;
    pub use nemo_physical::datavalues::syntax::RDF_DATATYPE_INDICATOR;

    /// Anonymous values such as variables or names
    pub const ANONYMOUS: &str = "_";
    /// Dot for decimal numbers
    pub const DOT: &str = ".";
}

pub mod encoding_prefixes {
    //! This module defines the prefixes for encoded numbers

    /// Reprsents the prefix for binary-encoded unsigned integers
    pub const BIN: &str = "0b";
    /// Reprsents the prefix for octal-encoded unsigned integers
    pub const OCT: &str = "0o";
    /// Reprsents the prefix for hex-encoded unsigned integers
    pub const HEX: &str = "0x";
}

pub mod import_export {
    //! This module defines the import/export configuration options.

    pub mod attribute {
        //! This module defines all the keys
        /// Name of the attribute for specifying the resource in import/export directives.
        pub const RESOURCE: &str = "resource";
        /// Name of the attribute for specifying the format in import/export directives.
        pub const FORMAT: &str = "format";
        /// Name of the attribute for specifying a base IRI in import/export directives.
        pub const BASE: &str = "base";
        /// Name of the attribute for specifying a delimiter in import/export directives for delimiter-separated values format.
        pub const DSV_DELIMITER: &str = "delimiter";
        /// Name of the attribute for specifying the compression in import/export directives.
        pub const COMPRESSION: &str = "compression";
        /// Name of the attribute for specifying the limit in import/export directives.
        pub const LIMIT: &str = "limit";
        // compression
        /// The name of the compression format that means "no compression".
        pub const VALUE_COMPRESSION_NONE: &str = "none";
        /// The name of the compression format that means "no compression".
        pub const VALUE_COMPRESSION_GZIP: &str = "gzip";
        /// Name of the attribute for ignoring DSV headers in import/export directives.
        pub const IGNORE_HEADERS: &str = "ignore_headers";
    }

    pub mod file_format {
        //! All the "predicate names" used in the maps in import/export directives.

        /// The "predicate name" used for the CSV format in import/export directives.
        pub const CSV: &str = "csv";
        /// The "predicate name" used for the DSV format in import/export directives.
        pub const DSV: &str = "dsv";
        /// The "predicate name" used for the TSV format in import/export directives.
        pub const TSV: &str = "tsv";
        /// The "predicate name" used for the generic RDF format in import/export directives.
        pub const RDF_UNSPECIFIED: &str = "rdf";
        /// The "predicate name" used for the Ntriples format in import/export directives.
        pub const RDF_NTRIPLES: &str = "ntriples";
        /// The "predicate name" used for the NQuads format in import/export directives.
        pub const RDF_NQUADS: &str = "nquads";
        /// The "predicate name" used for the Turtle format in import/export directives.
        pub const RDF_TURTLE: &str = "turtle";
        /// The "predicate name" used for the TriG format in import/export directives.
        pub const RDF_TRIG: &str = "trig";
        /// The "predicate name" used for the RDF/XML format in import/export directives.
        pub const RDF_XML: &str = "rdfxml";
        /// The "predicate name" used for the json format in import/export directives.
        pub const JSON: &str = "json";

        // file extensions
        /// The file extension used for CSV files
        pub(crate) const EXTENSION_CSV: &str = "csv";
        /// The file extension used for TSV files
        pub(crate) const EXTENSION_TSV: &str = "tsv";
        /// The file extension used for DSV files
        pub(crate) const EXTENSION_DSV: &str = "dsv";
        /// The file extension used for Ntriples files
        pub(crate) const EXTENSION_RDF_NTRIPLES: &str = "nt";
        /// The file extension used for NQuads files
        pub(crate) const EXTENSION_RDF_NQUADS: &str = "nq";
        /// The file extension used for Turtle files
        pub(crate) const EXTENSION_RDF_TURTLE: &str = "ttl";
        /// The file extension used for TriG files
        pub(crate) const EXTENSION_RDF_TRIG: &str = "trig";
        /// The file extension used for RDF/XML files
        pub(crate) const EXTENSION_RDF_XML: &str = "rdf";
        /// The file extension used for json files
        pub(crate) const EXTENSION_JSON: &str = "json";
        /// The file extension used for gzip files
        pub(crate) const EXTENSION_GZ: &str = "gz";

        // media types
        /// The media type used for CSV resources
        pub(crate) const MEDIA_TYPE_CSV: &str = "text/csv";
        /// The media type used for TSV resources
        pub(crate) const MEDIA_TYPE_TSV: &str = "text/tab-separated-values";
        /// The media type used for DSV resources
        pub(crate) const MEDIA_TYPE_DSV: &str = "text";
        /// The media type used for Ntriples resources
        pub(crate) const MEDIA_TYPE_RDF_NTRIPLES: &str = "application/n-triples";
        /// The media type used for NQuads resources
        pub(crate) const MEDIA_TYPE_RDF_NQUADS: &str = "application/n-quads";
        /// The media type used for Turtle resources
        pub(crate) const MEDIA_TYPE_RDF_TURTLE: &str = "text/turtle";
        /// The media type used for TriG resources
        pub(crate) const MEDIA_TYPE_RDF_TRIG: &str = "application/trig";
        /// The media type used for RDF/XML resources
        pub(crate) const MEDIA_TYPE_RDF_XML: &str = "application/rdf+xml";
        /// The media type used for json resources
        pub(crate) const MEDIA_TYPE_JSON: &str = "application/json";
    }
}
