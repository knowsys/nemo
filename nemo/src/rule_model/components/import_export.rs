//! Import and export directives are a direct representation of the syntactic information
//! given in rule files.

pub mod attributes;
pub mod compression;
pub mod file_formats;

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    hash::Hash,
};

use attributes::ImportExportAttribute;
use compression::CompressionFormat;
use file_formats::{AttributeRequirement, FileFormat};
use nemo_physical::datavalues::DataValue;
use spargebra::Query;


use crate::{
    io::formats::{
        dsv::value_format::{DsvValueFormat, DsvValueFormats},
        rdf::value_format::RdfValueFormat,
        Direction,
    },
    rule_model::{
        error::{hint::Hint, validation_error::ValidationErrorKind, ValidationErrorBuilder},
        origin::Origin,
        substitution::Substitution,
    },
};

use super::{
    tag::Tag,
    term::{map::Map, primitive::Primitive, Term},
    ProgramComponent, ProgramComponentKind,
};

/// An import/export specification. This object captures all information that is typically
/// present in an import or export directive in a Nemo program, including the main format,
/// optional attributes that define additional parameters, and an indentifier to map the data
/// to or from (i.e., a predicate name).
#[derive(Debug, Clone, Eq)]
pub(crate) struct ImportExportDirective {
    /// Origin of this component
    origin: Origin,

    /// The predicate we're handling.
    predicate: Tag,
    /// The file format and resource we're using.
    format: FileFormat,
    /// The attributes we've been given.
    attributes: Map,
}

impl ImportExportDirective {
    /// For a given [Term] return its contents as a plain string.
    ///
    /// This returns a value if the term is an iri and
    /// returns `None` otherwise.
    pub fn plain_value(term: &Term) -> Option<String> {
        if let Term::Primitive(Primitive::Ground(any_value)) = term {
            return any_value.value().to_iri();
        }

        None
    }

    /// For a given [Term] return its contents as a plain string.
    ///
    /// This returns a value if the term is a plain string and
    /// returns `None` otherwise.
    pub fn string_value(term: &Term) -> Option<String> {
        if let Term::Primitive(Primitive::Ground(any_value)) = term {
            return any_value.value().to_plain_string();
        }

        None
    }

    /// For a given [Term] return its contents as an integer.
    ///
    /// This returns a value if the term is an integer and
    /// returns `None` otherwise.
    pub fn integer_value(term: &Term) -> Option<i64> {
        if let Term::Primitive(Primitive::Ground(any_value)) = term {
            return any_value.value().to_i64();
        }

        None
    }

    /// For a given [Term] return its contents as an boolean.
    ///
    /// This returns a value if the term is an boolean and
    /// returns `None` otherwise.
    pub fn boolean_value(term: &Term) -> Option<bool> {
        if let Term::Primitive(Primitive::Ground(any_value)) = term {
            return any_value.value().to_boolean();
        }

        None
    }

    /// Return a [HashMap] containing the attributes of this directive.
    pub fn attribute_map(&self) -> HashMap<ImportExportAttribute, Term> {
        let mut result = HashMap::new();

        for (key, value) in self.attributes.key_value() {
            if let Some(name) =
                Self::plain_value(key).and_then(|plain| ImportExportAttribute::from_name(&plain))
            {
                result.insert(name, value.clone());
            }
        }

        result
    }

    /// Return a [HahsMap] containing the attributes of this directive,
    /// including the origin of each key.
    fn attribute_map_key(&self) -> HashMap<ImportExportAttribute, (&Origin, &Term)> {
        let mut result = HashMap::new();

        for (key, value) in self.attributes.key_value() {
            if let Some(name) =
                Self::plain_value(key).and_then(|plain| ImportExportAttribute::from_name(&plain))
            {
                result.insert(name, (key.origin(), value));
            }
        }

        result
    }

    /// Return the expected arity based on the format or given type information,
    ///
    /// Returns `None` if it is not possible to deduce this information.
    pub fn expected_arity(&self) -> Option<usize> {
        if let Some(arity) = self.format.arity() {
            return Some(arity);
        }

        match self.format {
            FileFormat::CSV | FileFormat::DSV | FileFormat::TSV => {
                if let Some(Term::Tuple(tuple)) =
                    self.attribute_map().get(&ImportExportAttribute::Format)
                {
                    return DsvValueFormats::from_tuple(tuple).map(|format| format.arity());
                }
            }
            _ => {}
        }

        None
    }

    /// Helper function for the display implementations of
    /// [ImportDirective] and [ExportDirective]
    /// to format the content of this object.
    fn display_content(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} :- {} {} .",
            self.predicate, self.format, self.attributes
        )
    }
}

impl ImportExportDirective {
    /// Validate directive.
    pub fn validate(
        &self,
        direction: Direction,
        builder: &mut ValidationErrorBuilder,
    ) -> Option<()> {
        if direction == Direction::Export && self.format == FileFormat::JSON {
            builder.report_error(self.origin, ValidationErrorKind::UnsupportedJsonExport);
            return None;
        }

        let attributes = self.attribute_map_key();
        for (attribute, requirement) in self.format.attributes() {
            if requirement == AttributeRequirement::Required && !attributes.contains_key(&attribute)
            {
                builder.report_error(
                    self.origin,
                    ValidationErrorKind::ImportExportMissingRequiredAttribute {
                        attribute: attribute.name().to_string(),
                        direction: direction.to_string(),
                    },
                );
            }
        }

        let expected_attributes = self
            .format
            .attributes()
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        for (attribute, (&attribute_origin, value)) in attributes.iter() {
            if !expected_attributes.contains(attribute) {
                builder
                    .report_error(
                        attribute_origin,
                        ValidationErrorKind::ImportExportUnrecognizedAttribute {
                            format: self.format.name().to_string(),
                            attribute: attribute.name().to_string(),
                        },
                    )
                    .add_hint_option(Hint::similar(
                        "parameter",
                        attribute.name(),
                        expected_attributes.iter().map(|attribute| attribute.name()),
                    ));
            }

            if let ProgramComponentKind::OneOf(types) = attribute.value_type() {
                if types.iter().any(|&typ| typ == value.kind()) {
                    continue;
                }

                builder.report_error(
                    *value.origin(),
                    ValidationErrorKind::ImportExportAttributeValueType {
                        parameter: attribute.name().to_string(),
                        given: value.kind().name().to_string(),
                        expected: format!(
                            "one of {}",
                            types
                                .iter()
                                .map(|typ| typ.name())
                                .intersperse(", ")
                                .collect::<String>()
                        ),
                    },
                );
            } else if attribute.value_type() != value.kind() {
                builder.report_error(
                    *value.origin(),
                    ValidationErrorKind::ImportExportAttributeValueType {
                        parameter: attribute.name().to_string(),
                        given: value.kind().name().to_string(),
                        expected: attribute.value_type().name().to_string(),
                    },
                );

                continue;
            }

            let _ = match attribute {
                ImportExportAttribute::Format => match self.format {
                    FileFormat::CSV | FileFormat::DSV | FileFormat::TSV => {
                        Self::validate_attribute_format_dsv(value, builder)
                    }
                    FileFormat::NTriples
                    | FileFormat::NQuads
                    | FileFormat::Turtle
                    | FileFormat::RDFXML
                    | FileFormat::TriG => Self::validate_attribute_format_rdf(value, builder),
                    FileFormat::JSON => Ok(()),
                    FileFormat::Sparql => Ok(()),
                },
                ImportExportAttribute::Delimiter => Self::validate_delimiter(value, builder),
                ImportExportAttribute::Compression => Self::validate_compression(value, builder),
                ImportExportAttribute::Limit => Self::validate_limit(value, builder),
                ImportExportAttribute::Query => Self::validate_query(value, builder),
                // TODO: should we verify the correctness of the endpoint somewhere?
                ImportExportAttribute::Endpoint => Ok(()),
                ImportExportAttribute::Base => Ok(()),
                ImportExportAttribute::Resource => Ok(()),
                &ImportExportAttribute::IgnoreHeaders => Ok(()),
            };
        }

        Some(())
    }

    /// Validate the format attribute for dsv
    fn validate_attribute_format_dsv(
        value: &Term,
        builder: &mut ValidationErrorBuilder,
    ) -> Result<(), ()> {
        if let Term::Tuple(tuple) = value {
            for argument in tuple.arguments() {
                if ImportExportDirective::plain_value(argument)
                    .and_then(|name| DsvValueFormat::from_name(&name))
                    .is_none()
                {
                    builder.report_error(
                        *argument.origin(),
                        ValidationErrorKind::ImportExportValueFormat {
                            file_format: String::from("dsv"),
                        },
                    );

                    return Err(());
                }
            }
        }

        Ok(())
    }

    /// Validate the format attribute for dsv
    fn validate_attribute_format_rdf(
        value: &Term,
        builder: &mut ValidationErrorBuilder,
    ) -> Result<(), ()> {
        if let Term::Tuple(tuple) = value {
            for argument in tuple.arguments() {
                if ImportExportDirective::plain_value(argument)
                    .and_then(|name| RdfValueFormat::from_name(&name))
                    .is_none()
                {
                    builder.report_error(
                        *argument.origin(),
                        ValidationErrorKind::ImportExportValueFormat {
                            file_format: String::from("rdf"),
                        },
                    );

                    return Err(());
                }
            }

            Ok(())
        } else {
            unreachable!("value should be of correct type")
        }
    }

    /// Check if the delimiter is a single character string.
    fn validate_delimiter(value: &Term, builder: &mut ValidationErrorBuilder) -> Result<(), ()> {
        if let Some(delimiter) = ImportExportDirective::string_value(value) {
            if delimiter.len() != 1 {
                builder.report_error(*value.origin(), ValidationErrorKind::ImportExportDelimiter);

                return Err(());
            }
        }

        Ok(())
    }

    /// Check if the limit is a non-negative number.
    fn validate_limit(value: &Term, builder: &mut ValidationErrorBuilder) -> Result<(), ()> {
        if let Term::Primitive(Primitive::Ground(ground)) = value {
            if !ground.value().fits_into_u64() {
                builder.report_error(
                    *value.origin(),
                    ValidationErrorKind::ImportExportLimitNegative,
                );
                return Err(());
            }
        }

        Ok(())
    }

    /// Check if the compression format is supported.
    fn validate_compression(value: &Term, builder: &mut ValidationErrorBuilder) -> Result<(), ()> {
        if let Some(compression) = ImportExportDirective::string_value(value) {
            if CompressionFormat::from_name(&compression).is_none() {
                builder.report_error(
                    *value.origin(),
                    ValidationErrorKind::ImportExportUnknownCompression {
                        format: compression,
                    },
                );

                return Err(());
            }
        }

        Ok(())
    }

    /// Parse the query to verify it is valid.
    fn validate_query(value: &Term, builder: &mut ValidationErrorBuilder) -> Result<(), ()> {
        if let Some(query) = ImportExportDirective::string_value(value) {
            if let Err(e) = Query::parse(&query[..], None) {
                builder.report_error(
                    *value.origin(),
                    ValidationErrorKind::ImportExportInvalidSparqlQuery {oxi_error: e.to_string()},
                );
                return Err(());
            }           
        }

        Ok(())
    }
    
}

impl PartialEq for ImportExportDirective {
    fn eq(&self, other: &Self) -> bool {
        self.predicate == other.predicate
            && self.format == other.format
            && self.attributes == other.attributes
    }
}

impl Hash for ImportExportDirective {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
        self.format.hash(state);
        self.attributes.hash(state);
    }
}

/// An import specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportDirective(pub(crate) ImportExportDirective);

impl ImportDirective {
    /// Create a new [ImportDirective].
    pub fn new(
        predicate: Tag,
        format: FileFormat,
        attributes: Map,
        bindings: Substitution,
    ) -> Self {
        let mut attributes = attributes;
        bindings.apply(&mut attributes);

        Self(ImportExportDirective {
            origin: Origin::default(),
            predicate,
            format,
            attributes: attributes.reduce(),
        })
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Tag {
        &self.0.predicate
    }

    /// Return the file format.
    pub fn file_format(&self) -> FileFormat {
        self.0.format
    }

    /// Return the attributes.
    pub fn attributes(&self) -> HashMap<ImportExportAttribute, Term> {
        self.0.attribute_map()
    }

    /// Return the expected arity based on the format or given type information,
    ///
    /// Returns `None` if it is not possible to deduce this information.
    pub fn expected_arity(&self) -> Option<usize> {
        self.0.expected_arity()
    }
}

impl From<ImportExportDirective> for ImportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
    }
}

impl Display for ImportDirective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("@import ")?;
        self.0.display_content(f)
    }
}

impl ProgramComponent for ImportDirective {
    fn origin(&self) -> &Origin {
        &self.0.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.0.origin = origin;
        self
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        self.0.validate(Direction::Import, builder)
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Import
    }
}

/// An export specification.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExportDirective(pub(crate) ImportExportDirective);

impl ExportDirective {
    /// Create a new [ExportDirective].
    pub fn new(
        predicate: Tag,
        format: FileFormat,
        attributes: Map,
        bindings: Substitution,
    ) -> Self {
        let mut attributes = attributes;
        bindings.apply(&mut attributes);

        Self(ImportExportDirective {
            origin: Origin::default(),
            predicate,
            format,
            attributes: attributes.reduce(),
        })
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Tag {
        &self.0.predicate
    }

    /// Return the file format.
    pub fn file_format(&self) -> FileFormat {
        self.0.format
    }

    /// Return the attributes.
    pub fn attributes(&self) -> HashMap<ImportExportAttribute, Term> {
        self.0.attribute_map()
    }

    /// Return the expected arity based on the format or given type information,
    ///
    /// Returns `None` if it is not possible to deduce this information.
    pub fn expected_arity(&self) -> Option<usize> {
        self.0.expected_arity()
    }
}

impl From<ImportExportDirective> for ExportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
    }
}

impl Display for ExportDirective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("@export ")?;
        self.0.display_content(f)
    }
}

impl ProgramComponent for ExportDirective {
    fn origin(&self) -> &Origin {
        &self.0.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.0.origin = origin;
        self
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        self.0.validate(Direction::Export, builder)
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Export
    }
}
