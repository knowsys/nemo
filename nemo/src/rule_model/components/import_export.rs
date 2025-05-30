//! Import and export directives are a direct representation of the syntactic information
//! given in rule files.

use specification::ImportExportSpec;

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
    term::{operation::Operation, Term},
    IterablePrimitives, ProgramComponent, ProgramComponentKind,
};

pub mod attribute;
pub mod specification;

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

impl std::fmt::Display for ImportExportDirective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{} :- {}.", self.predicate, self.spec))?;

        for binding in &self.bindings {
            f.write_str(", ")?;
            f.write_fmt(format_args!("{binding}"))?;
        }

        Ok(())
    }
}

impl IterablePrimitives for ImportExportDirective {
    type TermType = Term;

    fn primitive_terms<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a super::term::primitive::Primitive> + 'a> {
        Box::new(
            self.spec
                .values()
                .flat_map(|term| term.primitive_terms())
                .chain(self.bindings.iter().flat_map(|op| op.primitive_terms())),
        )
    }

    fn primitive_terms_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Self::TermType> + 'a> {
        Box::new(
            self.spec
                .values_mut()
                .flat_map(|term| term.primitive_terms_mut())
                .chain(
                    self.bindings
                        .iter_mut()
                        .flat_map(|op| op.primitive_terms_mut()),
                ),
        )
    }
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

impl std::fmt::Display for ImportDirective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("@import ")?;
        self.0.fmt(f)
    }
}

impl From<ImportExportDirective> for ImportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
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

impl IterablePrimitives for ImportDirective {
    type TermType = Term;

    fn primitive_terms<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a super::term::primitive::Primitive> + 'a> {
        self.0.primitive_terms()
    }

    fn primitive_terms_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Self::TermType> + 'a> {
        self.0.primitive_terms_mut()
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
        f.write_str("@export ")?;
        self.0.fmt(f)
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

impl IterablePrimitives for ExportDirective {
    type TermType = Term;

    fn primitive_terms<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a super::term::primitive::Primitive> + 'a> {
        self.0.primitive_terms()
    }

    fn primitive_terms_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Self::TermType> + 'a> {
        self.0.primitive_terms_mut()
    }
}
