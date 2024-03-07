//! Definitions for the [ImportExportHandler] trait that provides the main
//! handle for supported file formats, and of [ImportExportHandlers] as a
//! main entry point for obtaining such handlers.

use std::{
    collections::HashSet,
    io::{BufRead, Write},
    path::PathBuf,
};

use dyn_clone::DynClone;
use nemo_physical::{
    datasources::table_providers::TableProvider,
    datavalues::{AnyDataValue, DataValue, MapDataValue, TupleDataValue, ValueDomain},
    resource::Resource,
};

use crate::{
    error::Error,
    io::compression_format::CompressionFormat,
    model::{
        ExportDirective, FileFormat, ImportDirective, ImportExportDirective,
        PARAMETER_NAME_COMPRESSION, PARAMETER_NAME_FORMAT, PARAMETER_NAME_RESOURCE,
        VALUE_COMPRESSION_GZIP, VALUE_COMPRESSION_NONE, VALUE_FORMAT_ANY, VALUE_FORMAT_SKIP,
    },
};

use thiserror::Error;

use super::{
    types::{Direction, TableWriter},
    DsvHandler, RdfHandler,
};

/// Representation of a resource (file, URL, etc.) for import or export.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ImportExportResource {
    /// Value to indicate that the resource was not given.
    Unspecified,
    /// A concrete resource string.
    Resource(Resource),
    /// Use stdout (only for export)
    Stdout,
}

impl ImportExportResource {
    /// Retrieve the contained resource, if any.
    pub(crate) fn resource(&self) -> Option<Resource> {
        if let ImportExportResource::Resource(resource) = &self {
            Some(resource.clone())
        } else {
            None
        }
    }
}

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
    ///
    /// The arity is the arity of predicates that the caller would like to write. If the handler
    /// has a fixed arity (as returned by [ImportExportHandler::arity]),
    /// then this will normally be the same value (clashing arity requirements are detected
    /// during program analysis). However, even if not speciied, the arity might still be known
    /// in normal circumstances.
    fn writer(&self, writer: Box<dyn Write>, arity: usize) -> Result<Box<dyn TableWriter>, Error>;

    /// Obtain the resource used for this data exchange.
    /// In typical cases, this is the name of a file to read from or write to.
    /// If no resource was specified, or if the resource is not identified by a
    /// name (such as stdout), then `None` is returned.
    fn resource(&self) -> Option<Resource> {
        self.import_export_resource().resource()
    }

    /// Returns true if the selected resource is stdout.
    fn resource_is_stdout(&self) -> bool {
        self.import_export_resource() == &ImportExportResource::Stdout
    }

    /// Returns the expected arity of the predicate related to this directive, if specified.
    /// For import, this is the arity of the data that is created, for export it is the
    /// arity of the data that is consumed.
    fn predicate_arity(&self) -> Option<usize>;

    /// Returns the default file extension for data of this format, if any.
    /// This will be used when making default file names.
    fn file_extension(&self) -> Option<String>;

    /// Returns the chosen compression format for imported/exported data.
    fn compression_format(&self) -> Option<CompressionFormat>;

    /// Returns the [ImportExportResource] used for this data exchange.
    fn import_export_resource(&self) -> &ImportExportResource;
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
        attributes: &MapDataValue,
        valid_attributes: &[&str],
    ) -> Result<(), ImportExportError> {
        let given: HashSet<AnyDataValue> = attributes
            .map_keys()
            .expect("map values always have keys")
            .cloned()
            .collect();
        let valid: HashSet<AnyDataValue> = valid_attributes
            .iter()
            .map(|att| AnyDataValue::new_iri(att.to_string()))
            .collect();

        if let Some(unknown) = given.difference(&valid).next() {
            return Err(ImportExportError::UnknownAttribute(unknown.to_string()));
        }
        Ok(())
    }

    /// Extract the resource from the given attributes. This can be [ImportExportResource::Unspecified]
    /// for export (where we can use default names). If the value is invalid or missing for import, an
    /// error is returned.
    pub(super) fn extract_resource(
        attributes: &MapDataValue,
        direction: Direction,
    ) -> Result<ImportExportResource, ImportExportError> {
        let resource: Option<Resource> =
            Self::extract_string_or_iri(attributes, PARAMETER_NAME_RESOURCE, true)?;

        if let Some(string) = resource {
            if string.is_empty() {
                Ok(ImportExportResource::Stdout)
            } else {
                Ok(ImportExportResource::Resource(string))
            }
        } else {
            if direction == Direction::Import {
                return Err(ImportExportError::MissingAttribute(
                    PARAMETER_NAME_RESOURCE.to_string(),
                ));
            }
            Ok(ImportExportResource::Unspecified)
        }
    }

    /// Extract the compression format from the given attributes, and possibly resource.
    /// If a resource is given, then the resource name without the compression-specific
    /// extension is also returned.
    ///
    /// An error is returned if an unknown compression format was explicitly specified,
    /// or if the compression format of the resource is not in agreement with an explicitly
    /// stated one.
    pub(super) fn extract_compression_format(
        attributes: &MapDataValue,
        resource: &ImportExportResource,
    ) -> Result<(Option<CompressionFormat>, Option<Resource>), ImportExportError> {
        let cf_name = Self::extract_string_or_iri(attributes, PARAMETER_NAME_COMPRESSION, true)
            .expect("no errors with allow missing");

        let stated_compression_format: Option<CompressionFormat>;
        if let Some(cf_name) = &cf_name {
            match cf_name.as_str() {
                VALUE_COMPRESSION_NONE => stated_compression_format = Some(CompressionFormat::None),
                VALUE_COMPRESSION_GZIP => stated_compression_format = Some(CompressionFormat::Gzip),
                _ => {
                    return Err(ImportExportError::invalid_att_value_error(
                        PARAMETER_NAME_COMPRESSION,
                        AnyDataValue::new_plain_string(cf_name.to_owned()),
                        format!(
                            "unknown compression format, supported formats: {:?}",
                            [VALUE_COMPRESSION_GZIP, VALUE_COMPRESSION_NONE]
                        )
                        .as_str(),
                    ));
                }
            }
        } else {
            stated_compression_format = None;
        }

        let resource_compression_format: Option<CompressionFormat>;
        let inner_resource: Option<Resource>;
        if let ImportExportResource::Resource(res) = resource {
            let (rcf, inner_res) = CompressionFormat::from_resource(res);
            resource_compression_format = Some(rcf);
            inner_resource = Some(inner_res);
        } else {
            resource_compression_format = None;
            inner_resource = None;
        }

        match (stated_compression_format, resource_compression_format) {
            (Some(scf), None) => Ok((Some(scf), inner_resource)),
            (None, Some(rcf)) => Ok((Some(rcf), inner_resource)),
            (Some(scf), Some(rcf)) => {
                if scf == rcf {
                    Ok((Some(scf), inner_resource))
                } else {
                    Err(ImportExportError::invalid_att_value_error(
                        PARAMETER_NAME_COMPRESSION,
                        AnyDataValue::new_plain_string(
                            cf_name.expect("given if stated compression is known"),
                        ),
                        "compression method should match resource extension"
                            .to_string()
                            .as_str(),
                    ))
                }
            }
            (None, None) => Ok((None, inner_resource)),
        }
    }

    /// Extract a string value for the given attribute name. Returns an error if the
    /// value is mistyped ([ImportExportError::InvalidAttributeValue]) or missing ([ImportExportError::MissingAttribute]).
    /// It can be specified whether it should be allowed that the atttribute is not set at all (and
    /// `None` would then be returned). If given, the value must always be a string, however.
    pub(super) fn extract_string(
        attributes: &MapDataValue,
        attribute_name: &str,
        allow_missing: bool,
    ) -> Result<Option<String>, ImportExportError> {
        if let Some(c) = Self::extract_att_value(attributes, attribute_name, allow_missing)? {
            match c.value_domain() {
                ValueDomain::PlainString => Ok(Some(c.to_plain_string_unchecked())),
                _ => Err(ImportExportError::invalid_att_value_error(
                    attribute_name,
                    c.clone(),
                    "expecting string value",
                )),
            }
        } else {
            Ok(None)
        }
    }

    /// Extract a string or IRI value for the given attribute name. Returns an error if the
    /// value is mistyped ([ImportExportError::InvalidAttributeValue]) or missing ([ImportExportError::MissingAttribute]).
    /// It can be specified whether it should be allowed that the atttribute is not set at all (and
    /// `None` would then be returned). If given, the value must always be a string or an IRI, however.
    /// This method is used for parameters that are actually strings in nature, but where we conveniently want to
    /// allow the user to omit the quotes.
    pub(super) fn extract_string_or_iri(
        attributes: &MapDataValue,
        attribute_name: &str,
        allow_missing: bool,
    ) -> Result<Option<String>, ImportExportError> {
        if let Some(c) = Self::extract_att_value(attributes, attribute_name, allow_missing)? {
            if let Some(s) = Self::string_from_datavalue(&c) {
                Ok(Some(s))
            } else {
                Err(ImportExportError::invalid_att_value_error(
                    attribute_name,
                    c.clone(),
                    "expecting string or IRI value",
                ))
            }
        } else {
            Ok(None)
        }
    }

    /// Extract an integer value for the given attribute name. Returns an error if the
    /// value is mistyped ([ImportExportError::InvalidAttributeValue]) or missing ([ImportExportError::MissingAttribute]).
    /// It can be specified whether it should be allowed that the attribute is not set at all (and
    /// `None` would then be returned). If given, the value must always be an integer, however.
    pub(super) fn extract_integer(
        attributes: &MapDataValue,
        attribute_name: &str,
        allow_missing: bool,
    ) -> Result<Option<i64>, ImportExportError> {
        if let Some(c) = Self::extract_att_value(attributes, attribute_name, allow_missing)? {
            if c.fits_into_i64() {
                Ok(Some(c.to_i64_unchecked()))
            } else {
                Err(ImportExportError::invalid_att_value_error(
                    attribute_name,
                    c.clone(),
                    "expecting integer value",
                ))
            }
        } else {
            Ok(None)
        }
    }

    /// Extract an IRI value string for the given attribute name. Returns an error if the
    /// value is mistyped ([ImportExportError::InvalidAttributeValue]) or missing ([ImportExportError::MissingAttribute]).
    /// It can be specified whether it should be allowed that the atttribute is not set at all (and
    /// `None` would then be returned). If given, the value must always be an IRI, however.
    pub(super) fn extract_iri(
        attributes: &MapDataValue,
        attribute_name: &str,
        allow_missing: bool,
    ) -> Result<Option<String>, ImportExportError> {
        if let Some(c) = Self::extract_att_value(attributes, attribute_name, allow_missing)? {
            match c.value_domain() {
                ValueDomain::Iri => Ok(Some(c.to_iri_unchecked())),
                _ => Err(ImportExportError::invalid_att_value_error(
                    attribute_name,
                    c.clone(),
                    "expecting IRI value",
                )),
            }
        } else {
            Ok(None)
        }
    }

    /// Extract a value for the given attribute name. The boolean flag constrols
    /// if an error should be generated if the attribute is missing, or if `Ok(None)`
    /// should be returned in this case.
    pub(super) fn extract_att_value(
        attributes: &MapDataValue,
        attribute_name: &str,
        allow_missing: bool,
    ) -> Result<Option<AnyDataValue>, ImportExportError> {
        if let Some(c) = attributes.map_element(&AnyDataValue::new_iri(attribute_name.to_string()))
        {
            Ok(Some(c.clone()))
        } else if allow_missing {
            return Ok(None);
        } else {
            return Err(ImportExportError::MissingAttribute(
                attribute_name.to_string(),
            ));
        }
    }

    /// Extract the list of strings that specify value formats. If no list is given, `None`
    /// is returned. Errors may occur if the attribute is given but the value is not a list of strings,
    /// or if all values are skipped.
    ///
    /// See [ImportExportHandlers::extract_value_format_strings_and_arity] for a method that also
    /// checks the arity information, and uses it to make default formats if needed.
    pub(super) fn extract_value_format_strings(
        attributes: &MapDataValue,
    ) -> Result<Option<Vec<String>>, ImportExportError> {
        let value_format_strings: Option<Vec<String>>;
        if let Some(c) = Self::extract_att_value(attributes, PARAMETER_NAME_FORMAT, true)? {
            let mut value_formats: Vec<String> = Vec::new();
            if c.value_domain() == ValueDomain::Tuple {
                for i in 0..c.len_unchecked() {
                    let v = c.tuple_element_unchecked(i);
                    if let Some(s) = Self::string_from_datavalue(v) {
                        value_formats.push(s);
                    } else {
                        return Err(ImportExportError::invalid_att_value_error(
                            PARAMETER_NAME_FORMAT,
                            v.clone(),
                            "list must contain strings only",
                        ));
                    }
                }
            } else {
                return Err(ImportExportError::invalid_att_value_error(
                    PARAMETER_NAME_FORMAT,
                    c.clone(),
                    "expecting list of value formats",
                ));
            }
            value_format_strings = Some(value_formats);
        } else {
            value_format_strings = None;
        }

        // Check if any non-skipped value is contained
        if let Some(true) = value_format_strings
            .as_ref()
            .map(|v| v.iter().all(|fmt| *fmt == VALUE_FORMAT_SKIP))
        {
            return Err(ImportExportError::invalid_att_value_error(
                PARAMETER_NAME_FORMAT,
                Self::datavalue_from_format_strings(&value_format_strings.expect("checked above")),
                "cannot import/export zero-ary data",
            ));
        }

        Ok(value_format_strings)
    }

    /// Returns a list of string names of value formats that can be used as a
    /// default if only the arity of a predicate is known.
    pub(super) fn default_value_format_strings(arity: usize) -> Vec<String> {
        vec![VALUE_FORMAT_ANY; arity]
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// Get a list of value format strings while taking the expected arity of data in
    /// the file into account.
    ///
    /// Formats will first be extracted from the attributes. For import, the total number
    /// of formats must match the expected file arity. For export, the total number of
    /// non-skip formats must match the expected file arity.
    ///
    /// If no formats are given, we assume that "skip" is not used, so file arity =
    /// predicate arity = format number, and we can make a list of default value formats.
    /// `None` is only returned if the file arity was not given (in which case this function
    /// is the same as [ImportExportHandlers::extract_value_format_strings]).
    ///
    /// The given `file_arity` is not checked: callers are expected to have ensured that it
    /// is a non-zero usize that fits into i64.
    pub(super) fn extract_value_format_strings_with_file_arity(
        attributes: &MapDataValue,
        file_arity: Option<usize>,
        direction: Direction,
    ) -> Result<Option<Vec<String>>, ImportExportError> {
        let value_format_strings: Option<Vec<String>> =
            Self::extract_value_format_strings(attributes)?;

        if let Some(file_arity) = file_arity {
            if let Some(ref vfs) = value_format_strings {
                let declared_file_arity = match direction {
                    Direction::Import => vfs.len(),
                    Direction::Export => vfs.iter().fold(0, |acc: usize, fmt| {
                        // Only count formats other than VALUE_FORMAT_SKIP:
                        if *fmt == VALUE_FORMAT_SKIP {
                            acc
                        } else {
                            acc + 1
                        }
                    }),
                };

                // Check if arity is consistent with given value formats.
                if file_arity != declared_file_arity {
                    return Err(ImportExportError::invalid_att_value_error(
                        PARAMETER_NAME_FORMAT,
                        Self::datavalue_from_format_strings(vfs),
                        format!(
                            "value format declaration must be compatible with expected arity {} of tuples in file",
                            file_arity
                        )
                        .as_str(),
                    ));
                }

                Ok(value_format_strings)
            } else {
                Ok(Some(Self::default_value_format_strings(file_arity)))
            }
        } else {
            Ok(value_format_strings)
        }
    }

    /// Turn a list of formats into a data value for error reporting.
    fn datavalue_from_format_strings(format_strings: &[String]) -> AnyDataValue {
        TupleDataValue::from_iter(
            format_strings
                .iter()
                .map(|format| AnyDataValue::new_plain_string(format.to_owned()))
                .collect::<Vec<AnyDataValue>>(),
        )
        .into()
    }

    /// Extract a string from an [AnyDataValue] that is a plain string
    /// or IRI. This is in particularly used to allow users to omit the quotes
    /// around simple attribute values.
    fn string_from_datavalue(v: &AnyDataValue) -> Option<String> {
        match v.value_domain() {
            ValueDomain::PlainString => Some(v.to_plain_string_unchecked()),
            ValueDomain::Iri => Some(v.to_iri_unchecked()),
            _ => None,
        }
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
        value: AnyDataValue,
        /// The attribute the value was given for.
        attribute: AnyDataValue,
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
        value: AnyDataValue,
        reason: &str,
    ) -> ImportExportError {
        ImportExportError::InvalidAttributeValue {
            attribute: AnyDataValue::new_iri(attribute.to_string()),
            value: value.clone(),
            description: reason.to_string(),
        }
    }
}
