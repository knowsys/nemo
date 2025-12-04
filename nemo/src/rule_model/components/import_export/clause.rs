//! This module defines [ImportClause].

use std::{collections::HashMap, fmt::Display};

use crate::rule_model::{
    components::{
        ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent,
        IterableVariables, ProgramComponent, ProgramComponentKind, component_iterator,
        component_iterator_mut,
        import_export::ImportDirective,
        tag::Tag,
        term::primitive::{ground::GroundTerm, variable::Variable},
    },
    error::ValidationReport,
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

/// Represents an import executed during the evaluation of a rule.
#[derive(Debug, Clone)]
pub struct ImportClause {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Import
    import: ImportDirective,

    /// Output variables
    variables: Vec<Variable>,
}

impl Display for ImportClause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}(", self.import.predicate()))?;

        for (term_index, term) in self.variables.iter().enumerate() {
            term.fmt(f)?;

            if term_index < self.variables.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(")")
    }
}

impl ImportClause {
    /// Create a new [ImportClause]
    pub fn new(import: ImportDirective, variables: Vec<Variable>) -> Self {
        Self {
            origin: Origin::Created,
            id: ProgramComponentId::default(),
            import,
            variables,
        }
    }

    /// Merge two [ImportClause]s.
    ///
    /// TODO: Maybe more documentation
    ///
    /// Return `None` if they are incompatible.
    pub(crate) fn try_merge(
        left: Self,
        right: Self,
        equalities: &HashMap<Variable, GroundTerm>,
    ) -> Option<Self> {
        let (import, variables) =
            left.import
                .try_merge(&left.variables, &right.import, &right.variables, equalities)?;

        Some(Self {
            origin: Origin::Component(Box::new(left)), // TODO: track reference to [right] as well
            id: ProgramComponentId::default(),
            import,
            variables: variables
                .into_iter()
                .filter(|variable| !equalities.contains_key(variable))
                .collect(),
        })
    }

    /// Push constant assignments to variables into the underlying [ImportDirective].
    pub(crate) fn push_constants(&mut self, equalities: &HashMap<Variable, GroundTerm>) {
        self.import.push_constants(&self.variables, equalities);
        self.variables = self
            .variables
            .iter()
            .filter(|variable| !equalities.contains_key(variable))
            .cloned()
            .collect();
    }

    /// Try to create a negated version of this clause.
    pub(crate) fn try_negate(clause: Self) -> Option<Self> {
        Some(Self {
            origin: Origin::Component(Box::new(clause.clone())),
            id: ProgramComponentId::default(),
            import: clause.import.try_negate()?,
            variables: clause.variables
        })
    }

    /// Return a reference to the output variables.
    pub fn output_variables(&self) -> &Vec<Variable> {
        &self.variables
    }

    /// Return a reference to the underlying [ImportDirective].
    pub fn import_directive(&self) -> &ImportDirective {
        &self.import
    }

    /// Return the predicate of the import clause.
    pub fn predicate(&self) -> &Tag {
        self.import.predicate()
    }
}

impl ComponentBehavior for ImportClause {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Import
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        for variable in &self.variables {
            variable.validate()?;
        }

        self.import.validate()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for ImportClause {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for ImportClause {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableVariables for ImportClause {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        let import_variables = self.import.variables();

        Box::new(import_variables.chain(self.variables.iter()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        let import_variables = self.import.variables_mut();

        Box::new(import_variables.chain(self.variables.iter_mut()))
    }
}

impl IterableComponent for ImportClause {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let import_iter = self.import.children();
        let variable_iter = component_iterator(self.variables.iter());
        Box::new(import_iter.chain(variable_iter))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let import_iter = self.import.children_mut();
        let variable_iter = component_iterator_mut(self.variables.iter_mut());
        Box::new(import_iter.chain(variable_iter))
    }
}

/// A possibly negated [ImportClause]
#[derive(Debug, Clone)]
pub enum ImportLiteral {
    /// Not negated
    Positive(ImportClause),
    /// Negated
    Negative(ImportClause),
}

impl Display for ImportLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportLiteral::Positive(clause) => write!(f, "{clause}"),
            ImportLiteral::Negative(clause) => write!(f, "~{clause}"),
        }
    }
}

impl ImportLiteral {
    /// Create a new positive [ImportLiteral]
    pub fn positive(import: ImportDirective, variables: Vec<Variable>) -> Self {
        Self::Positive(ImportClause::new(import, variables))
    }

    /// Create a new negative [ImportLiteral]
    pub fn negative(import: ImportDirective, variables: Vec<Variable>) -> Self {
        Self::Negative(ImportClause::new(import, variables))
    }

    /// Merge two [ImportClause]s.
    ///
    /// TODO: Maybe more documentation
    ///
    /// Return `None` if they are incompatible.
    pub fn try_merge(
        left: Self,
        right: Self,
        equalities: &HashMap<Variable, GroundTerm>,
    ) -> Option<Self> {
        match (left, right) {
            (ImportLiteral::Positive(left), ImportLiteral::Positive(right)) => Some(ImportLiteral::Positive(ImportClause::try_merge(left, right, equalities)?)),
            (ImportLiteral::Positive(positive), ImportLiteral::Negative(negative)) |
            (ImportLiteral::Negative(negative), ImportLiteral::Positive(positive)) =>
                Some(ImportLiteral::Positive(ImportClause::try_merge(positive, ImportClause::try_negate(negative)?, equalities)?)),
            (ImportLiteral::Negative(left), ImportLiteral::Negative(right)) => Some(ImportLiteral::Negative(ImportClause::try_merge(left, right, equalities)?)),
        }
    }

    /// Return a reference to the output variables.
    pub fn output_variables(&self) -> &Vec<Variable> {
        match self {
            ImportLiteral::Positive(clause) => clause.output_variables(),
            ImportLiteral::Negative(clause) => clause.output_variables(),
        }
    }

    /// Return a reference to the underlying [ImportDirective].
    pub fn import_directive(&self) -> &ImportDirective {
        match self {
            ImportLiteral::Positive(clause) => clause.import_directive(),
            ImportLiteral::Negative(clause) => clause.import_directive(),
        }
    }

    /// Return the predicate of the import clause.
    pub fn predicate(&self) -> &Tag {
        match self {
            ImportLiteral::Positive(clause) => clause.predicate(),
            ImportLiteral::Negative(clause) => clause.predicate(),
        }
    }
}

impl ComponentBehavior for ImportLiteral {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Import
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        match self {
            ImportLiteral::Positive(clause) => clause.validate(),
            ImportLiteral::Negative(clause) => clause.validate(),
        }
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for ImportLiteral {
    type Source = Origin;

    fn origin(&self) -> Origin {
        match self {
            ImportLiteral::Positive(clause) => clause.origin(),
            ImportLiteral::Negative(clause) => clause.origin(),
        }
    }

    fn set_origin(&mut self, origin: Origin) {
        match self {
            ImportLiteral::Positive(clause) => clause.set_origin(origin),
            ImportLiteral::Negative(clause) => clause.set_origin(origin),
        }
    }
}

impl ComponentIdentity for ImportLiteral {
    fn id(&self) -> ProgramComponentId {
        match self {
            ImportLiteral::Positive(clause) => clause.id(),
            ImportLiteral::Negative(clause) => clause.id(),
        }
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        match self {
            ImportLiteral::Positive(clause) => clause.set_id(id),
            ImportLiteral::Negative(clause) => clause.set_id(id),
        }
    }
}

impl IterableVariables for ImportLiteral {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        match self {
            ImportLiteral::Positive(clause) => clause.variables(),
            ImportLiteral::Negative(clause) => clause.variables(),
        }
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        match self {
            ImportLiteral::Positive(clause) => clause.variables_mut(),
            ImportLiteral::Negative(clause) => clause.variables_mut(),
        }
    }
}

impl IterableComponent for ImportLiteral {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        match self {
            ImportLiteral::Positive(clause) => clause.children(),
            ImportLiteral::Negative(clause) => clause.children(),
        }
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        match self {
            ImportLiteral::Positive(clause) => clause.children_mut(),
            ImportLiteral::Negative(clause) => clause.children_mut(),
        }
    }
}
