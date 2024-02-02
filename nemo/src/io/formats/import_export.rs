//! Definitions for [ImportExportHandler] and related types.

use std::{
    collections::HashSet,
    io::{BufRead, Write},
    path::PathBuf,
};

use dyn_clone::DynClone;
use nemo_physical::{datasources::table_providers::TableProvider, resource::Resource};

use crate::{
    error::Error,
    model::{
        Constant, ExportDirective, FileFormat, ImportDirective, ImportExportDirective, Key, Map,
        NumericLiteral, PARAMETER_NAME_FORMAT, PARAMETER_NAME_RESOURCE,
    },
};

use thiserror::Error;

use super::{
    types::{Direction, TableWriter},
    DsvHandler, RdfHandler,
};

/// An [ImportExportHandler] represents a data format for input and/or output, and provides
/// specific methods for handling data of that format. Each handler is configured by format-specific
/// attributes, which define the behavior in detail, including the kind of data that this format
/// is compatible with. The attributes are provided when creating the format, and should then
/// be validated.
///
/// An implementation of [`ImportExportHandler`] provides methods to validate and refine parameters
/// that were used with this format, to create suitable [`TableProvider`] and [`TableWriter`] objects
/// to read and write data in the given format, and to report information about the type of
/// data that this format can handle (such as predicate arity and type).
pub(crate) trait ImportExportHandler: std::fmt::Debug + DynClone + Send {
    /// Return the associated [FileFormat].
    fn file_format(&self) -> FileFormat;

    /// Obtain a [`TableProvider`] for this format and the given reader, if supported.
    /// If reading is not supported, an error will be returned.
    ///
    /// The arity is the arity of predicates that the caller expects to receive from this
    /// call. If the handler has a fixed arity (as returned by [ImportExportHandler::arity]),
    /// then this will normally be the same value (clashing arity requirements are detected
    /// during program analysis). However, even if not speciied, the arity might also be inferred
    /// from the usage of the imported data in a program, or other requirements known to the caller.
    fn reader(&self, read: Box<dyn BufRead>, arity: usize)
        -> Result<Box<dyn TableProvider>, Error>;

    /// Obtain a [`TableWriter`] for this format and the given writer, if supported.
    /// If writing is not supported, an error will be returned.
    fn writer(&self, writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error>;

    /// Obtain the resource used for this data exchange.
    /// In typical cases, this is the name of a file to read from or write to.
    fn resource(&self) -> Option<Resource>;

    /// Returns the arity of the data for this format, if specified.
    fn arity(&self) -> Option<usize>;

    /// Returns the default file extension for data of this format, if any.
    /// This will be used when making default file names.
    fn file_extension(&self) -> Option<String>;
}

dyn_clone::clone_trait_object!(ImportExportHandler);

/// Struct with static methods to manage the conversion of [ImportExportDirective]s to
/// [ImportExportHandler]s.
pub(crate) struct ImportExportHandlers;

impl ImportExportHandlers {
    /// Obtain an [ImportExportHandler] for the given [ImportDirective], and return
    /// an error if the given attributes are not suitable for the chosen format.
    pub(crate) fn import_handler(
        directive: &ImportDirective,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        Self::handler(&directive.0, Direction::Import)
    }

    /// Obtain an [ImportExportHandler] for the given [ExportDirective], and return
    /// an error if the given attributes are not suitable for the chosen format.
    pub(crate) fn export_handler(
        directive: &ExportDirective,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        Self::handler(&directive.0, Direction::Export)
    }

    /// Obtain an [ImportExportHandler] for the given [ImportExportDirective], and return
    /// an error if the given attributes are not suitable for the chosen format.
    fn handler(
        directive: &ImportExportDirective,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        match directive.format {
            FileFormat::CSV => DsvHandler::try_new_csv(&directive.attributes, direction),
            FileFormat::DSV => DsvHandler::try_new_dsv(&directive.attributes, direction),
            FileFormat::TSV => DsvHandler::try_new_tsv(&directive.attributes, direction),
            FileFormat::RDF(variant) => {
                RdfHandler::try_new(variant, &directive.attributes, direction)
            }
        }
    }

    /// Check if all given attributes are among the valid attributes,
    /// and return an error otherwise.
    pub(super) fn check_attributes(
        attributes: &Map,
        valid_attributes: &Vec<&str>,
    ) -> Result<(), ImportExportError> {
        let given: HashSet<Key> = attributes.pairs.keys().cloned().collect();
        let valid: HashSet<Key> = valid_attributes
            .into_iter()
            .map(|att| Key::identifier_from_str(att))
            .collect();

        if let Some(unknown) = given.difference(&valid).next() {
            return Err(ImportExportError::UnknownAttribute(unknown.to_string()));
        }
        Ok(())
    }

    /// Extract the resource from the given attributes. This can be `None` for export (where
    /// we can use default names). If the value is invalid or missing for import, an error
    /// is returned.
    pub(super) fn extract_resource(
        attributes: &Map,
        direction: Direction,
    ) -> Result<Option<Resource>, ImportExportError> {
        let resource: Option<Resource> =
            Self::extract_string_or_iri(attributes, PARAMETER_NAME_RESOURCE, true)?;
        if resource.is_none() && direction == Direction::Import {
            return Err(ImportExportError::MissingAttribute(
                PARAMETER_NAME_RESOURCE.to_string(),
            ));
        }
        Ok(resource)
    }

    /// Extract a string value for the given attribute name. Returns an error if the
    /// value is mistyped ([ImportExportError::InvalidAttributeValue]) or missing ([ImportExportError::MissingAttribute]).
    /// It can be specified whether it should be allowed that the atttribute is not set at all (and
    /// `None` would then be returned). If given, the value must always be a string, however.
    pub(super) fn extract_string(
        attributes: &Map,
        attribute_name: &str,
        allow_missing: bool,
    ) -> Result<Option<String>, ImportExportError> {
        if let Some(c) = Self::extract_att_value(attributes, attribute_name, allow_missing)? {
            match c {
                Constant::StringLiteral(string) => {
                    return Ok(Some(string.to_owned()));
                }
                _ => {
                    return Err(ImportExportError::invalid_att_value_error(
                        attribute_name,
                        c.clone(),
                        "expecting string value",
                    ))
                }
            }
        } else {
            return Ok(None);
        }
    }

    /// Extract a string or IRI value for the given attribute name. Returns an error if the
    /// value is mistyped ([ImportExportError::InvalidAttributeValue]) or missing ([ImportExportError::MissingAttribute]).
    /// It can be specified whether it should be allowed that the atttribute is not set at all (and
    /// `None` would then be returned). If given, the value must always be a string or an IRI, however.
    /// This method is used for parameters that are actually strings in nature, but where we conveniently want to
    /// allow the user to omit the quotes.
    pub(super) fn extract_string_or_iri(
        attributes: &Map,
        attribute_name: &str,
        allow_missing: bool,
    ) -> Result<Option<String>, ImportExportError> {
        if let Some(c) = Self::extract_att_value(attributes, attribute_name, allow_missing)? {
            match c {
                Constant::StringLiteral(string) => {
                    return Ok(Some(string.to_owned()));
                }
                Constant::Abstract(id) => {
                    return Ok(Some(id.name()));
                }
                _ => {
                    return Err(ImportExportError::invalid_att_value_error(
                        attribute_name,
                        c.clone(),
                        "expecting string or IRI value",
                    ))
                }
            }
        } else {
            return Ok(None);
        }
    }

    /// Extract an integer value for the given attribute name. Returns an error if the
    /// value is mistyped ([ImportExportError::InvalidAttributeValue]) or missing ([ImportExportError::MissingAttribute]).
    /// It can be specified whether it should be allowed that the atttribute is not set at all (and
    /// `None` would then be returned). If given, the value must always be an integer, however.
    pub(super) fn extract_integer(
        attributes: &Map,
        attribute_name: &str,
        allow_missing: bool,
    ) -> Result<Option<i64>, ImportExportError> {
        if let Some(c) = Self::extract_att_value(attributes, attribute_name, allow_missing)? {
            match c {
                Constant::NumericLiteral(NumericLiteral::Integer(i)) => {
                    return Ok(Some(i));
                }
                _ => {
                    return Err(ImportExportError::invalid_att_value_error(
                        attribute_name,
                        c.clone(),
                        "expecting integer value",
                    ))
                }
            }
        } else {
            return Ok(None);
        }
    }

    /// Extract an IRI value string for the given attribute name. Returns an error if the
    /// value is mistyped ([ImportExportError::InvalidAttributeValue]) or missing ([ImportExportError::MissingAttribute]).
    /// It can be specified whether it should be allowed that the atttribute is not set at all (and
    /// `None` would then be returned). If given, the value must always be an IRI, however.
    pub(super) fn extract_iri(
        attributes: &Map,
        attribute_name: &str,
        allow_missing: bool,
    ) -> Result<Option<String>, ImportExportError> {
        if let Some(c) = Self::extract_att_value(attributes, attribute_name, allow_missing)? {
            match c {
                Constant::Abstract(id) => {
                    return Ok(Some(id.name()));
                }
                _ => {
                    return Err(ImportExportError::invalid_att_value_error(
                        attribute_name,
                        c.clone(),
                        "expecting IRI value",
                    ))
                }
            }
        } else {
            return Ok(None);
        }
    }

    /// Extract a value for the given attribute name. The boolean flag constrols
    /// if an error should be generated if the attribute is missing, or if `Ok(None)`
    /// should be returned in this case.
    pub(super) fn extract_att_value(
        attributes: &Map,
        attribute_name: &str,
        allow_missing: bool,
    ) -> Result<Option<Constant>, ImportExportError> {
        if let Some(c) = attributes
            .pairs
            .get(&Key::identifier_from_str(attribute_name))
        {
            return Ok(Some(c.clone()));
        } else if allow_missing {
            return Ok(None);
        } else {
            return Err(ImportExportError::MissingAttribute(
                attribute_name.to_string(),
            ));
        }
    }

    /// Extract the list of strings that specify value formats. If no list is given, `None`
    /// is returned. Errors may occur if the attribute is given but the value is not a list of strings.
    pub(super) fn extract_value_format_strings(
        attributes: &Map,
    ) -> Result<Option<Vec<String>>, ImportExportError> {
        let value_format_strings: Option<Vec<String>>;
        if let Some(c) = Self::extract_att_value(attributes, PARAMETER_NAME_FORMAT, true)? {
            let mut value_formats: Vec<String> = Vec::new();
            match c {
                Constant::TupleLiteral(tuple) => {
                    for c in (*tuple).iter() {
                        match c {
                            Constant::StringLiteral(string) => {
                                value_formats.push(string.to_owned());
                            }
                            _ => {
                                return Err(ImportExportError::invalid_att_value_error(
                                    PARAMETER_NAME_FORMAT,
                                    c.clone(),
                                    "list must contain strings only",
                                ))
                            }
                        }
                    }
                }
                _ => {
                    return Err(ImportExportError::invalid_att_value_error(
                        PARAMETER_NAME_FORMAT,
                        c.clone(),
                        "expecting list of value formats",
                    ))
                }
            }
            value_format_strings = Some(value_formats);
        } else {
            value_format_strings = None;
        }
        Ok(value_format_strings)
    }
}

/// Errors related to the creation and usage of [ImportExportHandler]s.
#[derive(Debug, Error)]
pub enum ImportExportError {
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
    /// File format name is not known.
    #[error(r#"Unknown file format "{0}""#)]
    UnknownFileFormat(String),
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
    /// Value format is unsupported for this format.
    #[error(r#"Unsupported value format "{value_format}" for format {format}"#)]
    InvalidValueFormat {
        /// The given value format.
        value_format: String,
        /// The file format.
        format: FileFormat,
    },
    /// Arity is unsupported for this format.
    #[error(r#"import produces tuples of arity {arity}, but it should be arity {expected}"#)]
    InvalidArity {
        /// The given arity.
        arity: usize,
        /// The expected arity.
        expected: usize,
    },
    /// Arity is unsupported for this format, exact value is required.
    #[error(r#"unsupported arity "{arity}" for format {format}, must be {required}"#)]
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
    IoError {
        /// Contains the wrapped error
        error: std::io::Error,
        /// Path that could not be read
        path: PathBuf,
    },
}

impl ImportExportError {
    /// Convenience method to create ImportExportError::InvalidAttributeValue from static strings, which is a common
    /// task in handlers.
    pub(crate) fn invalid_att_value_error(
        attribute: &str,
        value: Constant,
        reason: &str,
    ) -> ImportExportError {
        ImportExportError::InvalidAttributeValue {
            attribute: Key::identifier_from_str(attribute),
            value: value.clone(),
            description: reason.to_string(),
        }
    }
}
