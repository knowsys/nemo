//! This module defines constants relating to the supported file formats.

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

/// The file extension used for CSV files
pub(crate) const EXTENSION_CSV: &str = "csv";
/// The file extension used for TSV files
pub(crate) const EXTENSION_TSV: &str = "csv";
/// The file extension used for DSV files
pub(crate) const EXTENSION_DSV: &str = "csv";
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
