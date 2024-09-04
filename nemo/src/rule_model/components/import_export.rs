//! Import and export directives are a direct representation of the syntactic information
//! given in rule files.

pub mod attributes;
pub mod compression;
pub mod file_formats;

use std::{collections::HashMap, fmt::Display, hash::Hash};

use attributes::ImportExportAttribute;
use file_formats::FileFormat;
use nemo_physical::datavalues::DataValue;

use crate::{
    io::formats::dsv::value_format::DsvValueFormats,
    rule_model::{error::ValidationErrorBuilder, origin::Origin},
};

use super::{
    tag::Tag,
    term::{map::Map, primitive::Primitive, Term},
    ProgramComponent,
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
    /// This returns a value if the term is am iri and
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

    /// Return a [HashMap] containing the attributes of this directive.
    fn attribute_map(&self) -> HashMap<ImportExportAttribute, &Term> {
        let mut result = HashMap::new();

        for (key, value) in self.attributes.key_value() {
            if let Some(name) =
                Self::plain_value(&key).and_then(|plain| ImportExportAttribute::from_name(&plain))
            {
                result.insert(name, value);
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
                if let Some(value_format) = self.attribute_map().get(&ImportExportAttribute::Format)
                {
                    if let Term::Tuple(tuple) = value_format {
                        return DsvValueFormats::from_tuple(tuple).map(|format| format.arity());
                    }
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
    pub fn new(predicate: Tag, format: FileFormat, attributes: Map) -> Self {
        Self(ImportExportDirective {
            origin: Origin::default(),
            predicate,
            format,
            attributes,
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
    pub fn attributes(&self) -> HashMap<ImportExportAttribute, &Term> {
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
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ValidationError>
    where
        Self: Sized,
    {
        todo!()
    }

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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized,
    {
        todo!()
    }
}

/// An export specification.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExportDirective(pub(crate) ImportExportDirective);

impl ExportDirective {
    /// Create a new [ExportDirective].
    pub fn new(predicate: Tag, format: FileFormat, attributes: Map) -> Self {
        Self(ImportExportDirective {
            origin: Origin::default(),
            predicate,
            format,
            attributes,
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
    pub fn attributes(&self) -> HashMap<ImportExportAttribute, &Term> {
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
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ValidationError>
    where
        Self: Sized,
    {
        todo!()
    }

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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized,
    {
        todo!()
    }
}
