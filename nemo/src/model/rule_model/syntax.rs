//! Constants for strings that are relevant to the syntax of rules.
//! These are kept in one location, since they are required in various
//! places related to parsing and display.
use nemo_physical::datavalues::syntax;

/// The "predicate name" used for the CSV format in import/export directives.
pub(crate) const FILE_FORMAT_CSV: &str = "csv";
/// The "predicate name" used for the DSV format in import/export directives.
pub(crate) const FILE_FORMAT_DSV: &str = "dsv";
/// The "predicate name" used for the TSV format in import/export directives.
pub(crate) const FILE_FORMAT_TSV: &str = "tsv";
/// The "predicate name" used for the generic RDF format in import/export directives.
pub(crate) const FILE_FORMAT_RDF_UNSPECIFIED: &str = "rdf";
/// The "predicate name" used for the Ntriples format in import/export directives.
pub(crate) const FILE_FORMAT_RDF_NTRIPLES: &str = "ntriples";
/// The "predicate name" used for the NQuads format in import/export directives.
pub(crate) const FILE_FORMAT_RDF_NQUADS: &str = "nquads";
/// The "predicate name" used for the Turtle format in import/export directives.
pub(crate) const FILE_FORMAT_RDF_TURTLE: &str = "turtle";
/// The "predicate name" used for the TriG format in import/export directives.
pub(crate) const FILE_FORMAT_RDF_TRIG: &str = "trig";
/// The "predicate name" used for the RDF/XML format in import/export directives.
pub(crate) const FILE_FORMAT_RDF_XML: &str = "rdfxml";
/// The "predicate name" used for the json format in import/export directives.
pub(crate) const FILE_FORMAT_JSON: &str = "json";

/// Name of the parameter for specifying the resource in import/export directives.
pub(crate) const PARAMETER_NAME_RESOURCE: &str = "resource";
/// Name of the parameter for specifying the format in import/export directives.
pub(crate) const PARAMETER_NAME_FORMAT: &str = "format";
/// Name of the parameter for specifying a base IRI in import/export directives.
pub(crate) const PARAMETER_NAME_BASE: &str = "base";
/// Name of the parameter for specifying a delimiter in import/export directives for delimiter-separated values format.
pub(crate) const PARAMETER_NAME_DSV_DELIMITER: &str = "delimiter";
/// Name of the parameter for specifying the compression in import/export directives.
pub(crate) const PARAMETER_NAME_COMPRESSION: &str = "compression";
/// Name of the parameter for specifying the limit in import/export directives.
pub(crate) const PARAMETER_NAME_LIMIT: &str = "limit";

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

/// The name of the compression format that means "no compression".
pub(crate) const VALUE_COMPRESSION_NONE: &str = "none";
/// The name of the compression format that means "no compression".
pub(crate) const VALUE_COMPRESSION_GZIP: &str = "gzip";
