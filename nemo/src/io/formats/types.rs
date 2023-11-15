//! Types related to input and output formats.

use std::{collections::HashSet, fs::File, path::PathBuf, str::FromStr};

use dyn_clone::DynClone;
use thiserror::Error;

use nemo_physical::table_reader::TableReader;

use crate::{
    error::Error,
    io::resource_providers::ResourceProviders,
    model::{Constant, Identifier, Key, Map, PrimitiveType, TupleConstraint},
};

use super::{dsv::DSVFormat, rdf_triples::RDFFormat};

pub(crate) mod attributes {
    pub(crate) const PATH: &str = "path";
}

/// A writer for tables that takes iterators over [`PrimitiveType`][PrimitiveType] for each row.
pub trait TableWriter {
    /// Write a record.
    fn write_record(&mut self, record: dyn Iterator<Item = PrimitiveType>) -> Result<(), Error>;
}

/// IO Direction
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Direction {
    /// Processing input.
    Reading,
    /// Processing output.
    Writing,
}

/// A supported file format for I/O.
pub trait FileFormatMeta: std::fmt::Debug + DynClone + Send {
    /// Return the associated format.
    fn file_format(&self) -> FileFormat;

    /// Verify that the given path is readable.
    fn verify_path_is_readable(&self, path: &str) -> Result<(), FileFormatError> {
        let path = PathBuf::from(path);

        if let Err(err) = File::open(path.clone()) {
            return Err(FileFormatError::IOUnreadable { error: err, path });
        }

        Ok(())
    }

    /// Verify that the given path is writable.

    /// Obtain a [`TableReader`] for this format, if supported.
    fn reader(
        &self,
        attributes: &Map,
        declared_types: &TupleConstraint,
        resource_providers: ResourceProviders,
        inferred_types: &TupleConstraint,
    ) -> Result<Box<dyn TableReader>, Error>;

    /// Obtain a [`TableWriter`] for this format, if supported.
    fn writer(&self, attributes: &Map) -> Result<Box<dyn TableWriter>, Error>;

    /// Attributes that are valid for this format, but not required.
    fn optional_attributes(&self, direction: Direction) -> HashSet<Key>;

    /// Attributes that are required for this format.
    fn required_attributes(&self, direction: Direction) -> HashSet<Key>;

    /// Check whether the given pairs of attributes and values are a
    /// valid combination for this format.
    fn validate_attribute_values(
        &self,
        direction: Direction,
        attributes: &Map,
    ) -> Result<(), FileFormatError>;

    /// Check whether the given attributes are a valid combination for this format.
    fn validate_attributes(
        &self,
        direction: Direction,
        attributes: &Map,
    ) -> Result<(), FileFormatError> {
        let given = attributes.pairs.keys().cloned().collect();

        log::debug!("given attributes: {given:?}");

        if let Some(missing) = self
            .required_attributes(direction)
            .difference(&given)
            .next()
        {
            return Err(FileFormatError::MissingAttribute(missing.to_string()));
        }

        let valid = self
            .required_attributes(direction)
            .union(&self.optional_attributes(direction))
            .cloned()
            .collect();

        if let Some(unknown) = given.difference(&valid).next() {
            return Err(FileFormatError::UnknownAttribute(unknown.to_string()));
        }

        self.validate_attribute_values(direction, attributes)
    }
}

dyn_clone::clone_trait_object!(FileFormatMeta);

/// An import/export specification.
#[derive(Debug, Clone)]
pub struct ImportExportSpec {
    /// Is this for reading or for writing?
    pub(crate) direction: Direction,
    /// The predicate we're handling.
    pub(crate) predicate: Identifier,
    /// The type constraints for the predicate.
    pub(crate) constraints: TupleConstraint,
    /// The file format we're using.
    pub(crate) format: Box<dyn FileFormatMeta>,
    /// The attributes we've been given.
    pub(crate) attributes: Map,
}

impl ImportExportSpec {
    /// Obtain a [`TableReader`] for this import specification.
    pub fn reader(
        &self,
        resource_providers: ResourceProviders,
        inferred_types: &TupleConstraint,
    ) -> Result<Box<dyn TableReader>, Error> {
        self.format.reader(
            &self.attributes,
            &self.constraints,
            resource_providers,
            inferred_types,
        )
    }

    /// Obtain a [`TableWriter`] for this export specification.
    pub fn writer(&self) -> Result<Box<dyn TableWriter>, Error> {
        self.format.writer(&self.attributes)
    }
}

impl PartialEq for ImportExportSpec {
    fn eq(&self, other: &Self) -> bool {
        self.direction == other.direction
            && self.predicate == other.predicate
            && self.constraints == other.constraints
            && self.format.file_format() == other.format.file_format()
            && self.attributes == other.attributes
    }
}

impl Eq for ImportExportSpec {}

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
    /// Format name is not known.
    #[error(r#"Unknown file format "{0}""#)]
    UnknownFormat(String),
    /// Attribute value is invalid.
    #[error(r#"Invalid attribute value "{value}" for attribute "{attribute}": {description}"#)]
    InvalidAttributeValue {
        /// The given value.
        value: Constant,
        /// The attribute the value was given for.
        attribute: Key,
        /// A description of why the value was invalid.
        description: String,
    },
    /// File could not be read
    #[error(r#"File "{path}" could not be read."#)]
    IOUnreadable {
        /// Contains the wrapped error
        error: std::io::Error,
        /// Path that could not be read
        path: PathBuf,
    },
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
    /// Return the associated [`FileFormatMeta`] for this format.
    pub fn into_meta(self) -> Box<dyn FileFormatMeta> {
        match self {
            Self::DSV => Box::new(DSVFormat::new()),
            Self::CSV => Box::new(DSVFormat::with_delimiter(b',')),
            Self::TSV => Box::new(DSVFormat::with_delimiter(b'\t')),
            Self::RDF | Self::NTriples | Self::Turtle | Self::RDFXML => Box::new(RDFFormat {}),
            Self::NQuads => todo!(),
        }
    }
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

impl FromStr for FileFormat {
    type Err = FileFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let canonicalised = s.to_uppercase();

        match canonicalised.as_str() {
            "DSV" => Ok(Self::DSV),
            "CSV" => Ok(Self::CSV),
            "TSV" => Ok(Self::TSV),
            "RDF" => Ok(Self::RDF),
            "NTRIPLES" => Ok(Self::NTriples),
            "NQUADS" => Ok(Self::NQuads),
            "TURTLE" => Ok(Self::Turtle),
            "RDFXML" => Ok(Self::RDFXML),
            _ => Err(FileFormatError::UnknownFormat(s.to_string())),
        }
    }
}
