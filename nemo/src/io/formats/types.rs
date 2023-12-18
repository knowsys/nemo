//! Types related to input and output formats.

use std::{collections::HashSet, io::Write, path::PathBuf, str::FromStr};

use dyn_clone::DynClone;
use nemo_physical::{datasources::table_providers::TableProvider, resource::Resource};
use thiserror::Error;

use crate::{
    error::Error,
    io::{
        formats::{
            dsv::DsvFormat,
            rdf::{RDFFormat, RDFVariant},
        },
        resource_providers::ResourceProviders,
    },
    model::{Constant, Identifier, Key, Map, TupleConstraint},
};

pub(crate) mod attributes {
    pub(crate) const RESOURCE: &str = "resource";
}

/// IO Direction
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Direction {
    /// Processing input.
    Reading,
    /// Processing output.
    Writing,
}

/// A trait for file formats that append a format-specific file extension to a path.
pub trait PathWithFormatSpecificExtension {
    /// Returns an appropriate file extension for this format, if
    /// necessary.
    fn extension(&self) -> Option<&str>;

    /// Augment the given [path][PathBuf] with an extension corresponding to this
    /// format.
    fn path_with_extension(&self, path: PathBuf) -> PathBuf {
        match self.extension() {
            Some(new_extension) => path.with_extension(match path.extension() {
                Some(existing_extension) => {
                    let existing_extension = existing_extension.to_str().expect("valid UTF-8");

                    if existing_extension == new_extension {
                        existing_extension.to_string()
                    } else {
                        format!("{existing_extension}.{new_extension}")
                    }
                }
                None => new_extension.to_string(),
            }),
            None => path,
        }
    }
}

/// A trait for writing tables to a [writer][Write], one record at a
/// time.
pub trait TableWriter {
    /// Write a record in the table to the given [writer][Write].
    fn write_record(&mut self, record: &[String], writer: &mut dyn Write) -> Result<(), Error>;
}

/// A supported file format for I/O.
/// TODO: Consider renaming. This is not a "format" but already a concrete resource (or set of resources) in
/// a format. In particular, implementations will report a specific file name etc.
pub trait FileFormatMeta: std::fmt::Debug + DynClone + Send {
    /// Return the associated format.
    fn file_format(&self) -> FileFormat;

    /// Obtain a [`TableProvider`] for this format, if supported.
    fn reader(
        &self,
        attributes: &Map,
        declared_types: &TupleConstraint,
        resource_providers: ResourceProviders,
    ) -> Result<Box<dyn TableProvider>, Error>;

    /// Obtain a [`TableWriter`] for this format, if supported.
    fn writer(&self, attributes: &Map) -> Result<Box<dyn TableWriter>, Error>;

    /// Obtain all resources used for this format.
    fn resources(&self, attributes: &Map) -> Vec<Resource>;

    /// Attributes that are valid for this format, but not required.
    fn optional_attributes(&self, direction: Direction) -> HashSet<Key>;

    /// Attributes that are required for this format.
    fn required_attributes(&self, direction: Direction) -> HashSet<Key>;

    /// Check whether the given attributes and the declared types are valid
    fn validated_and_refined_type_declaration(
        &mut self,
        direction: Direction,
        attributes: &Map,
        declared_types: TupleConstraint,
    ) -> Result<TupleConstraint, FileFormatError> {
        self.validate_attributes(direction, attributes)?;
        self.validate_and_refine_type_declaration(declared_types)
    }

    /// Check whether the given type declaration is valid for this
    /// format. Return a refined type declaration.
    fn validate_and_refine_type_declaration(
        &mut self,
        declared_types: TupleConstraint,
    ) -> Result<TupleConstraint, FileFormatError>;

    /// Check whether the given pairs of attributes and values are a
    /// valid combination for this format.
    fn validate_attribute_values(
        &mut self,
        direction: Direction,
        attributes: &Map,
    ) -> Result<(), FileFormatError>;

    /// Check whether the given attributes are a valid combination for this format.
    fn validate_attributes(
        &mut self,
        direction: Direction,
        attributes: &Map,
    ) -> Result<(), FileFormatError> {
        let given = attributes.pairs.keys().cloned().collect();

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
#[derive(Clone, Debug)]
pub struct ImportExportSpec {
    /// The predicate we're handling.
    pub(crate) predicate: Identifier,
    /// The type constraint for the predicate, as declared for the import/export.
    /// TODO: Consider moving this into the [`FileFormatMeta`].
    /// TODO: This will most likely become an attribute anyway, specifying formats for individual columns (etc.). Maybe replace by an arity.
    pub(crate) constraint: TupleConstraint,
    /// The file format and resource we're using.
    pub(crate) format: Box<dyn FileFormatMeta>,
    /// The attributes we've been given.
    /// TODO: Maybe it is enough to have this inside the [FileFormatMeta]?
    pub(crate) attributes: Map,
}

impl ImportExportSpec {
    /// Obtain a [`TableProvider`] for this import specification.
    pub fn reader(
        &self,
        resource_providers: ResourceProviders,
    ) -> Result<Box<dyn TableProvider>, Error> {
        self.format
            .reader(&self.attributes, &self.constraint, resource_providers)
    }

    /// Write the given table to the given [writer][Write], using this export specification.
    pub fn write_table(
        &self,
        table: impl Iterator<Item = Vec<String>>,
        writer: &mut dyn Write,
    ) -> Result<(), Error> {
        let mut table_writer = self.format.writer(&self.attributes)?;

        for record in table {
            table_writer.write_record(&record, writer)?;
        }

        Ok(())
    }
}

impl PartialEq for ImportExportSpec {
    fn eq(&self, other: &Self) -> bool {
        self.predicate == other.predicate
            && self.constraint == other.constraint
            && self.format.file_format() == other.format.file_format()
            && self.attributes == other.attributes
    }
}

impl Eq for ImportExportSpec {}

/// An import specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportSpec(ImportExportSpec);

impl ImportSpec {
    /// Obtain a [`TableProvider`] for this import specification.
    pub fn reader(
        &self,
        resource_providers: ResourceProviders,
    ) -> Result<Box<dyn TableProvider>, Error> {
        self.0.reader(resource_providers)
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Identifier {
        &self.0.predicate
    }

    /// Return the type constraints.
    pub fn type_constraint(&self) -> &TupleConstraint {
        &self.0.constraint
    }

    /// Return the file format.
    pub fn file_format(&self) -> FileFormat {
        self.0.format.file_format()
    }

    /// Return the attributes.
    pub fn attributes(&self) -> &Map {
        &self.0.attributes
    }

    /// Return the used resources.
    pub fn resources(&self) -> Vec<Resource> {
        self.0.format.resources(&self.0.attributes)
    }
}

impl From<ImportExportSpec> for ImportSpec {
    fn from(value: ImportExportSpec) -> Self {
        Self(value)
    }
}

/// An export specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportSpec(ImportExportSpec);

impl ExportSpec {
    /// Write the given table to the given [writer][Write].
    pub fn write_table(
        &self,
        table: impl Iterator<Item = Vec<String>>,
        writer: &mut dyn Write,
    ) -> Result<(), Error> {
        self.0.write_table(table, writer)
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Identifier {
        &self.0.predicate
    }

    /// Return the type constraints.
    pub fn type_constraint(&self) -> &TupleConstraint {
        &self.0.constraint
    }

    /// Return the file format.
    pub fn file_format(&self) -> FileFormat {
        self.0.format.file_format()
    }

    /// Return the attributes.
    pub fn attributes(&self) -> &Map {
        &self.0.attributes
    }

    /// Return the used resources.
    pub fn resources(&self) -> Vec<Resource> {
        self.0.format.resources(&self.0.attributes)
    }
}

impl From<ImportExportSpec> for ExportSpec {
    fn from(value: ImportExportSpec) -> Self {
        Self(value)
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
    /// Arity is unsupported for this format.
    #[error(r#"Unsupported arity "{arity}" for format {format}"#)]
    InvalidArity {
        /// The given arity.
        arity: usize,
        /// The file format.
        format: FileFormat,
    },
    /// Arity is unsupported for this format, exact value is required.
    #[error(r#"Unsupported arity "{arity}" for format {format}, must be {required}"#)]
    InvalidArityExact {
        /// The given arity.
        arity: usize,
        /// The required arity.
        required: usize,
        /// The file format.
        format: FileFormat,
    },
    /// Format does not support complex types
    #[error(r"Format {format} does not support complex types")]
    UnsupportedComplexTypes {
        /// The file format.
        format: FileFormat,
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
    /// RDF Triples or Quads, with the given format variant.
    RDF(RDFVariant),
}

impl FileFormat {
    /// Return the associated [`FileFormatMeta`] for this format.
    pub fn into_meta(self) -> Box<dyn FileFormatMeta> {
        match self {
            Self::DSV => Box::new(DsvFormat::new()),
            Self::CSV => Box::new(DsvFormat::csv()),
            Self::TSV => Box::new(DsvFormat::tsv()),
            Self::RDF(variant) => Box::new(RDFFormat::with_variant(variant)),
        }
    }
}

impl PathWithFormatSpecificExtension for FileFormat {
    /// Return an appropriate file name extension for this format.
    fn extension(&self) -> Option<&str> {
        match self {
            FileFormat::CSV => Some("csv"),
            FileFormat::DSV => Some("dsv"),
            FileFormat::TSV => Some("tsv"),
            FileFormat::RDF(variant) => variant.extension(),
        }
    }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DSV => write!(f, "DSV"),
            Self::CSV => write!(f, "CSV"),
            Self::TSV => write!(f, "TSV"),
            Self::RDF(variant) => write!(f, "{variant}"),
        }
    }
}

impl FromStr for FileFormat {
    type Err = FileFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let canonicalized = s.to_uppercase();

        match canonicalized.as_str() {
            "DSV" => return Ok(Self::DSV),
            "CSV" => return Ok(Self::CSV),
            "TSV" => return Ok(Self::TSV),
            "RDF" => return Ok(Self::RDF(RDFVariant::Unspecified)),
            _ => (),
        }

        if let Ok(variant) = canonicalized.parse::<RDFVariant>() {
            return Ok(Self::RDF(variant));
        }

        Err(FileFormatError::UnknownFormat(s.to_string()))
    }
}
