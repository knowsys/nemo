//! Import and export directives are a direct representation of the syntactic information
//! given in rule files.

use crate::{
    io::format_builder::ImportExportBuilder,
    rule_model::{
        error::{validation_error::ValidationErrorKind, ValidationErrorBuilder},
        origin::Origin,
    },
    syntax,
};

use super::{
    tag::Tag,
    term::{
        operation::Operation,
        primitive::{variable::Variable, Primitive},
        Term,
    },
    IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind,
};

/// Direction of import/export activities.
///
/// We often share code for the two directions, and a direction
/// is then used to enable smaller distinctions where needed.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Direction {
    /// Processing input.
    Import,
    /// Processing output.
    Export,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Import => f.write_str("import"),
            Direction::Export => f.write_str("export"),
        }
    }
}

/// Attribute value pairs defining import or export parameters
#[derive(Debug, Clone, Eq)]
pub struct ImportExportSpec {
    /// Origin of this component
    origin: Origin,

    /// File format
    format: Tag,

    /// List of tuples containing attribute-value-pairs
    map: Vec<(Term, Term)>,
}

impl ImportExportSpec {
    /// Create a new [ImportExportSpec].
    pub fn new<Pairs: IntoIterator<Item = (Term, Term)>>(format: &str, map: Pairs) -> Self {
        Self {
            origin: Origin::Created,
            format: Tag::new(format.to_string()),
            map: map.into_iter().collect(),
        }
    }

    /// Create a new empty [ImportExportSpec].
    pub fn empty(format: &str) -> Self {
        Self {
            origin: Origin::Created,
            format: Tag::new(format.to_string()),
            map: Vec::default(),
        }
    }

    /// Return the tag of this map.
    pub fn format(&self) -> Tag {
        self.format.clone()
    }

    /// Return an iterator over the key value pairs in this map.
    pub fn key_value(&self) -> impl Iterator<Item = &(Term, Term)> {
        self.map.iter()
    }

    /// Return the number of entries in this map.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Return whether this map is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return whether this term is ground,
    /// i.e. if it does not contain any variables.
    pub fn is_ground(&self) -> bool {
        self.key_value()
            .all(|(key, value)| key.is_ground() && value.is_ground())
    }

    /// Reduce the [Term]s in each key-value pair returning a copy.
    pub fn reduce(&self) -> Self {
        Self {
            origin: self.origin,
            format: self.format.clone(),
            map: self
                .key_value()
                .map(|(key, value)| (key.reduce(), value.reduce()))
                .collect(),
        }
    }
}

impl std::fmt::Display for ImportExportSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}{{", self.format))?;

        for (term_index, (key, value)) in self.map.iter().enumerate() {
            f.write_fmt(format_args!("{}: {}", key, value))?;

            if term_index < self.map.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str("}")
    }
}

impl PartialEq for ImportExportSpec {
    fn eq(&self, other: &Self) -> bool {
        self.format == other.format && self.map == other.map
    }
}

impl PartialOrd for ImportExportSpec {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.map.partial_cmp(&other.map)
    }
}

impl std::hash::Hash for ImportExportSpec {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.format.hash(state);
        self.map.hash(state);
    }
}

impl ProgramComponent for ImportExportSpec {
    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        for (key, value) in self.key_value() {
            key.validate(builder)?;
            value.validate(builder)?;
        }

        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Map
    }
}

impl IterableVariables for ImportExportSpec {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(
            self.map
                .iter()
                .flat_map(|(key, value)| key.variables().chain(value.variables())),
        )
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(
            self.map
                .iter_mut()
                .flat_map(|(key, value)| key.variables_mut().chain(value.variables_mut())),
        )
    }
}

impl IterablePrimitives for ImportExportSpec {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        Box::new(
            self.map
                .iter()
                .flat_map(|(key, value)| key.primitive_terms().chain(value.primitive_terms())),
        )
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
        Box::new(
            self.map.iter_mut().flat_map(|(key, value)| {
                key.primitive_terms_mut().chain(value.primitive_terms_mut())
            }),
        )
    }
}

/// An import/export specification. This object captures all information that is typically
/// present in an import or export directive in a Nemo program, including the main format,
/// optional attributes that define additional parameters, and an indentifier to map the data
/// to or from (i.e., a predicate name).
#[derive(Clone, Debug)]
pub(crate) struct ImportExportDirective {
    /// Origin of this component
    origin: Origin,
    /// The predicate we're handling.
    predicate: Tag,
    /// The specified format and import/export attributes
    spec: ImportExportSpec,
    /// Additional variable bindings
    bindings: Vec<Operation>,
}

/// An import specification.
#[derive(Debug, Clone)]
pub struct ImportDirective(pub(crate) ImportExportDirective);

impl ImportDirective {
    /// Create a new [ImportDirective].
    pub fn new(predicate: Tag, spec: ImportExportSpec, bindings: Vec<Operation>) -> Self {
        Self(ImportExportDirective {
            origin: Origin::default(),
            predicate,
            spec,
            bindings,
        })
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Tag {
        &self.0.predicate
    }

    /// Return the attribute specification.
    pub(crate) fn spec(&self) -> &ImportExportSpec {
        &self.0.spec
    }
}

impl From<ImportExportDirective> for ImportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for ImportDirective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "@import {} :- {}.",
            self.predicate(),
            self.spec()
        ))
    }
}

impl ProgramComponent for ImportDirective {
    type ValidationResult = ImportExportBuilder;

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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<ImportExportBuilder>
    where
        Self: Sized,
    {
        if !self.predicate().is_valid() {
            builder.report_error(
                *self.predicate().origin(),
                ValidationErrorKind::InvalidTermTag(self.predicate().to_string()),
            );
        }

        self.spec().validate(builder)?;

        ImportExportBuilder::new(
            self.0.spec.clone(),
            &self.0.bindings,
            Direction::Import,
            builder,
        )
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Import
    }
}

/// An export specification.
#[derive(Debug, Clone)]
pub struct ExportDirective(pub(crate) ImportExportDirective);

impl ExportDirective {
    /// Create a new [ExportDirective].
    pub fn new(predicate: Tag, spec: ImportExportSpec, bindings: Vec<Operation>) -> Self {
        Self(ImportExportDirective {
            origin: Origin::default(),
            predicate,
            spec,
            bindings,
        })
    }

    /// Return a new [ExportDirective] with file format csv.
    pub fn new_csv(predicate: Tag) -> Self {
        Self::new(
            predicate,
            ImportExportSpec::empty(syntax::import_export::file_format::CSV),
            Vec::default(),
        )
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Tag {
        &self.0.predicate
    }

    /// Return the attribute specification.
    pub(crate) fn spec(&self) -> &ImportExportSpec {
        &self.0.spec
    }
}

impl From<ImportExportDirective> for ExportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for ExportDirective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "@export {} :- {}.",
            self.predicate(),
            self.spec()
        ))
    }
}

impl ProgramComponent for ExportDirective {
    type ValidationResult = ImportExportBuilder;

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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<ImportExportBuilder>
    where
        Self: Sized,
    {
        if !self.predicate().is_valid() {
            builder.report_error(
                *self.predicate().origin(),
                ValidationErrorKind::InvalidTermTag(self.predicate().to_string()),
            );
        }

        self.spec().validate(builder)?;

        ImportExportBuilder::new(
            self.0.spec.clone(),
            &self.0.bindings,
            Direction::Export,
            builder,
        )
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Export
    }
}
