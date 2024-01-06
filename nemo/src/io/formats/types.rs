//! Types related to input and output formats.

use std::{collections::HashSet, io::Write, path::PathBuf, str::FromStr};

use dyn_clone::DynClone;
use nemo_physical::{
    datasources::table_providers::TableProvider, datavalues::AnyDataValue, resource::Resource,
};
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

/// A trait for exporting table data, e.g., to some file.
pub trait TableWriter {
    /// Export a table.
    fn export_table_data<'a>(
        &mut self,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error>;
}

/// A supported file format for I/O. For example, nemo supports the CSV format,
/// which can be used for import and export. An implementation of [`FileFormatMeta`]
/// provides methods to validate and refine parameters that were used with
/// this format, and to create suitable [`TableProvider`] and [`TableWriter`] objects
/// to read and write data in the given format.
///
/// Implementations of this trait are usually lightweight, with minimal data to distinguish
/// similar formats (such as CSV and DSV). Most data related to the actual import or export
/// resides in an additional map of attributes that use format-specific keys. The format can
/// interpret and validate these attributes, and it uses them to make the [`TableProvider`]
/// and [`TableWriter`]. The methods in this trait can panic if required attributes are
/// missing or malformed. The method [FileFormatMeta::validated_and_refined_type_declaration]
/// should be used to check that the attributes are safe to use.
pub(crate) trait FileFormatMeta: std::fmt::Debug + DynClone + Send {
    /// Return the associated format.
    fn file_format(&self) -> FileFormat;

    /// Obtain a [`TableProvider`] for this format, if supported.
    ///
    /// # Panics
    /// If given attributes and/or types are not valid.
    fn reader(
        &self,
        attributes: &Map,
        declared_types: &TupleConstraint,
        resource_providers: ResourceProviders,
    ) -> Result<Box<dyn TableProvider>, Error>;

    /// Obtain a [`TableWriter`] for this format and the given writer, if supported.
    /// If writing is not supported, an error will be returned.
    ///
    /// # Panics
    /// If given attributes are not valid.
    fn writer(
        &self,
        attributes: &Map,
        writer: Box<dyn Write>,
    ) -> Result<Box<dyn TableWriter>, Error>;

    /// Obtain all resources used for this format. In typical cases, this is a single
    /// file to read from or write to.
    ///
    /// # Panics
    /// If required resource is not given or is malformed.
    fn resources(&self, attributes: &Map) -> Vec<Resource>;

    /// Attributes that are valid for this format, but not required. This is used to
    /// perform some basic validity checks that are the same for all formats.
    fn optional_attributes(&self, direction: Direction) -> HashSet<Key>;

    /// Attributes that are required for this format. This is used to
    /// perform some basic validity checks that are the same for all formats.
    ///
    /// If a format requires a certain attribute only under some conditions (e.g., if a
    /// specific value is used for one attribute, then a second is required), this can
    /// be checked in [FileFormatMeta::validate_attribute_values].
    fn required_attributes(&self, direction: Direction) -> HashSet<Key>;

    /// Check whether the given attributes and the declared types are valid.
    /// This is the main validation method used by callers. It triggers all other
    /// validations.
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
    /// valid combination for this format. This method is called internally and
    /// can implement arbitrary validations that cannot be caught by the coarse
    /// checks for optional and required attributes.
    ///
    /// # Panics
    ///
    /// If required attributes are missing entirely. Typically,
    /// [FileFormatMeta::validate_attribute_values] should be used instead.
    fn validate_attribute_values(
        &mut self,
        direction: Direction,
        attributes: &Map,
    ) -> Result<(), FileFormatError>;

    /// Check whether the given pairs of attributes and values are a
    /// valid combination for this format.
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

/// An import/export specification. This object captures all information that is typically
/// present in an import or export directive in a Nemo program, including the main format,
/// optional attributes that define additional parameters, and an indentifier to map the data
/// to or from (i.e., a predicate name).
#[derive(Clone, Debug)]
pub struct ImportExportSpec {
    /// The predicate we're handling.
    pub(crate) predicate: Identifier,
    /// The type constraint for the predicate, as declared for the import/export.
    /// TODO: This will most likely become an attribute anyway, specifying formats for individual columns (etc.). Maybe replace by an arity.
    pub(crate) constraint: TupleConstraint,
    /// The file format and resource we're using.
    pub(crate) format: Box<dyn FileFormatMeta>,
    /// The attributes we've been given.
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

    /// Obtain a [`TableWriter`] for this export specification.
    pub fn writer(&self, writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error> {
        self.format.writer(&self.attributes, writer)
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
    /// Obtain a [`TableWriter`] for this export specification.
    pub fn writer(&self, writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error> {
        self.0.writer(writer)
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
    pub(crate) fn into_meta(self) -> Box<dyn FileFormatMeta> {
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
