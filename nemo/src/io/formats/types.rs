//! Types related to input and output formats.

use std::collections::HashSet;

use thiserror::Error;

use nemo_physical::table_reader::{Resource, TableReader};

use crate::{error::Error, io::resource_providers::ResourceProviders, model::PrimitiveType};

/// A writer for tables that takes iterators over [`PrimitiveType`][PrimitiveType] for each row.
pub trait TableWriter {
    /// Write a record.
    fn write_record(&mut self, record: dyn Iterator<Item = PrimitiveType>) -> Result<(), Error>;
}

/// A supported file format for I/O.
pub trait FileFormatMeta: std::fmt::Debug {
    /// Obtain a [`TableReader`][TableReader] for this format, if supported.
    fn reader(
        &self,
        resource_providers: ResourceProviders,
        resource: Resource,
        logical_types: Vec<PrimitiveType>,
    ) -> Result<Box<dyn TableReader>, Error>;

    /// Obtain a [`TableWriter`] for this format, if supported.
    fn writer(&self) -> Result<Box<dyn TableWriter>, Error>;

    /// Attributes that are valid for this format, but not required.
    fn optional_attributes() -> HashSet<String>;

    /// Attributes that are required for this format.
    fn required_attributes() -> HashSet<String>;

    /// Check whether the given attributes are a valid combination for this format.
    fn validate_attributes(
        attributes: impl IntoIterator<Item = String>,
    ) -> Result<(), FileFormatError> {
        let given = attributes.into_iter().collect();

        if let Some(missing) = Self::required_attributes().difference(&given).next() {
            return Err(FileFormatError::MissingAttribute(missing.to_string()));
        }

        let valid = Self::required_attributes()
            .union(&Self::optional_attributes())
            .cloned()
            .collect();

        if let Some(unknown) = given.difference(&valid).next() {
            return Err(FileFormatError::UnknownAttribute(unknown.to_string()));
        }

        Ok(())
    }
}

/// Errors related to input and output formats
#[derive(Debug, Error)]
pub enum FileFormatError {
    /// Format is not supported for reading.
    #[error(r#"Format "{0}" cannot be read"#)]
    UnsupportedRead(FileFormat),
    /// Format is not supported for writing.
    #[error(r#"Format "{0}" cannot be written"#)]
    UnsupportedWrite(FileFormat),
    /// A required attribute is missing.
    #[error(r#"Missing required attribute "{0}""#)]
    MissingAttribute(String),
    /// A given attribute is not valid for the format.
    #[error(r#"Unknown attribute "{0}""#)]
    UnknownAttribute(String),
}

/// Supported file formats.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FileFormat {
    /// Comma-separated values
    CSV,
    /// Delimiter-separated values
    DSV,
    /// Tab-separated values
    TSV,
    /// RDF Triples
    RDF,
    /// RDF N-Triples
    NTriples,
    /// RDF N-Quads
    NQuads,
    /// RDF Turtle
    Turtle,
    /// RDF/XML
    RDFXML,
}

impl FileFormat {
    // fn format(self) -> Box<dyn FileFormat> {
    //     match self {
    //         Self::DSV | Self::CSV | Self::TSV => Box::new(DSVFormat {}),
    //     }
    // }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DSV => write!(f, "DSV"),
            Self::CSV => write!(f, "CSV"),
            Self::TSV => write!(f, "TSV"),
            Self::RDF => write!(f, "RDF"),
            Self::NTriples => write!(f, "RDF N-Triples"),
            Self::NQuads => write!(f, "RDF N-Quads"),
            Self::Turtle => write!(f, "RDF Turtle"),
            Self::RDFXML => write!(f, "RDF/XML"),
        }
    }
}
