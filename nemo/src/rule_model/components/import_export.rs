//! Import and export directives are a direct representation of the syntactic information
//! given in rule files.

use specification::ImportExportSpec;

use crate::{
    io::format_builder::ImportExportBuilder,
    rule_model::{
        components::rule::Rule, error::ValidationReport, origin::Origin,
        pipeline::id::ProgramComponentId,
    },
    syntax,
};

use super::{
    ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent, IterablePrimitives,
    IterableVariables, ProgramComponent, ProgramComponentKind, component_iterator,
    component_iterator_mut,
    tag::Tag,
    term::{Term, operation::Operation, primitive::variable::Variable},
};

pub mod attribute;
pub mod clause;
pub mod io_type;
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
    /// Id of this component
    id: ProgramComponentId,

    /// The predicate we're handling.
    predicate: Tag,
    /// The specified format and import/export attributes
    spec: ImportExportSpec,
    /// Additional variable bindings
    bindings: Vec<Operation>,
    /// Sub-rules for filtered import/export
    filter_rules: Vec<Rule>,
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

impl IterableVariables for ImportExportDirective {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(
            self.spec
                .variables()
                .chain(self.bindings.iter().flat_map(|binding| binding.variables())),
        )
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(
            self.spec.variables_mut().chain(
                self.bindings
                    .iter_mut()
                    .flat_map(|binding| binding.variables_mut()),
            ),
        )
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

impl IterableComponent for ImportExportDirective {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let iterator_spec = self.spec.children();
        let iterator_operations = component_iterator(self.bindings.iter());

        Box::new(iterator_spec.chain(iterator_operations))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let iterator_spec = self.spec.children_mut();
        let iterator_operations = component_iterator_mut(self.bindings.iter_mut());

        Box::new(iterator_spec.chain(iterator_operations))
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
            id: ProgramComponentId::default(),
            predicate,
            spec,
            bindings,
            filter_rules: Vec::new(),
        })
    }

    /// Change the predicate that this import writes to.
    pub fn set_predicate(&mut self, predicate: Tag) {
        self.0.predicate = predicate;
    }

    /// Add a new sub-[Rule] for filtered imports.
    pub fn add_filter_rule(&mut self, rule: Rule) {
        self.0.filter_rules.push(rule);
    }

    /// Return the filter rules
    pub fn filter_rules(&self) -> &[Rule] {
        &self.0.filter_rules
    }

    /// Return the corresponding [ImportExportBuilder] if this component is valid.
    pub fn builder_report(&self, report: &mut ValidationReport) -> Option<ImportExportBuilder> {
        ImportExportBuilder::new(
            self.predicate().clone(),
            self.spec(),
            self.bindings(),
            self.filter_rules(),
            Direction::Import,
            report,
        )
    }

    /// Return the corresponding [ImportExportBuilder] if this component is valid.
    pub fn builder(&self) -> Option<ImportExportBuilder> {
        let mut report = ValidationReport::default();
        ImportExportBuilder::new(
            self.predicate().clone(),
            self.spec(),
            self.bindings(),
            self.filter_rules(),
            Direction::Import,
            &mut report,
        )
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Tag {
        &self.0.predicate
    }

    /// Return the attribute specification.
    pub fn spec(&self) -> &ImportExportSpec {
        &self.0.spec
    }

    /// Return the additional bindings of this statement.
    pub fn bindings(&self) -> &[Operation] {
        &self.0.bindings
    }

    /// Check whether this imports from stdin.
    pub fn is_stdin(&self) -> bool {
        // TODO: There must be a better way
        if let Some(builder) = self.builder() {
            builder
                .resource()
                .map(|resource| resource.is_pipe())
                .unwrap_or(false)
        } else {
            false
        }
    }

    /// Return the expected arity of this directive, if any.
    pub fn expected_arity(&self) -> Option<usize> {
        // TODO: There must be a better way
        self.builder()?.expected_arity()
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

impl ComponentBehavior for ImportDirective {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Import
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();
        let _ = self.builder_report(&mut report);
        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for ImportDirective {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.0.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.0.origin = origin;
    }
}

impl ComponentIdentity for ImportDirective {
    fn id(&self) -> ProgramComponentId {
        self.0.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.0.id = id;
    }
}

impl IterableComponent for ImportDirective {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        self.0.children()
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        self.0.children_mut()
    }
}

impl IterableVariables for ImportDirective {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        self.0.variables()
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        self.0.variables_mut()
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
            id: ProgramComponentId::default(),
            predicate,
            spec,
            bindings,
            filter_rules: Vec::new(),
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

    /// Add a new sub-[Rule] for filtered exports.
    pub fn add_filter_rule(&mut self, rule: Rule) {
        self.0.filter_rules.push(rule);
    }

    /// Return the filter rules
    pub fn filter_rules(&self) -> &[Rule] {
        &self.0.filter_rules
    }

    /// Return the corresponding [ImportExportBuilder] if this component is valid.
    pub fn builder_report(&self, report: &mut ValidationReport) -> Option<ImportExportBuilder> {
        ImportExportBuilder::new(
            self.predicate().clone(),
            self.spec(),
            self.bindings(),
            self.filter_rules(),
            Direction::Export,
            report,
        )
    }

    /// Return the corresponding [ImportExportBuilder] if this component is valid.
    pub fn builder(&self) -> Option<ImportExportBuilder> {
        let mut report = ValidationReport::default();
        ImportExportBuilder::new(
            self.predicate().clone(),
            self.spec(),
            self.bindings(),
            self.filter_rules(),
            Direction::Export,
            &mut report,
        )
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Tag {
        &self.0.predicate
    }

    /// Return the attribute specification.
    pub fn spec(&self) -> &ImportExportSpec {
        &self.0.spec
    }

    /// Return the additional bindings of this statement.
    pub fn bindings(&self) -> &[Operation] {
        &self.0.bindings
    }

    /// Return the expected arity of this directive, if any.
    pub fn expected_arity(&self) -> Option<usize> {
        // TODO: There must be a better way
        self.builder()?.expected_arity()
    }
}

impl ComponentBehavior for ExportDirective {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Export
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();
        let _ = self.builder_report(&mut report);
        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for ExportDirective {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.0.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.0.origin = origin;
    }
}

impl ComponentIdentity for ExportDirective {
    fn id(&self) -> ProgramComponentId {
        self.0.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.0.id = id;
    }
}

impl IterableComponent for ExportDirective {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        self.0.children()
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        self.0.children_mut()
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

impl IterableVariables for ExportDirective {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        self.0.variables()
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        self.0.variables_mut()
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
