//! Defines the

use thiserror::Error;

use nemo_physical::{datavalues::DataValueCreationError, resource::Resource};

/// Errors that can occur when reading/writing RDF resources and converting them
/// to/from Nemo's data value representation.
#[allow(variant_size_differences)]
#[derive(Error, Debug)]
pub enum RdfFormatError {
    /// A problem occurred in converting an RDF term to a data value.
    #[error(transparent)]
    DataValueConversion(#[from] DataValueCreationError),
    /// Error of encountering unsupported value in subject position
    #[error("values used as subjects of RDF triples must be IRIs or nulls")]
    RdfInvalidSubject,
    /// Error of encountering RDF* features in data
    #[error("RDF* terms are not supported")]
    RdfStarUnsupported,
    /// Error in Rio's Turtle parser
    #[error(transparent)]
    RioTurtle(#[from] rio_turtle::TurtleError),
    /// Error in Rio's RDF/XML parser
    #[error(transparent)]
    RioXml(#[from] rio_xml::RdfXmlError),
    /// Unable to determine RDF format.
    #[error("could not determine which RDF parser to use for resource {0}")]
    UnknownRdfFormat(Box<Resource>),
}
