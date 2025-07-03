//! This module defines [Statement].

use std::fmt::Display;

use delegate::delegate;

use crate::rule_model::{
    components::{
        fact::Fact,
        import_export::{ExportDirective, ImportDirective},
        output::Output,
        parameter::ParameterDeclaration,
        rule::Rule,
        term::{
            primitive::{variable::Variable, Primitive},
            Term,
        },
        ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent,
        IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind,
    },
    error::ValidationReport,
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

/// Statement contained in a program, like a fact or a rule.
#[derive(Debug, Clone)]
pub enum Statement {
    /// Rule
    Rule(Rule),
    /// Fact
    Fact(Fact),
    /// Import
    Import(ImportDirective),
    /// Export
    Export(ExportDirective),
    /// Output
    Output(Output),
    /// Parameter
    Parameter(ParameterDeclaration),
}

impl Statement {
    /// Check whether this statement is a rule.
    pub fn is_rule(&self) -> bool {
        matches!(self, Self::Rule(_))
    }

    /// Check whether this statement is a fact.
    pub fn is_fact(&self) -> bool {
        matches!(self, Self::Fact(_))
    }

    /// Check whether this statement is a import.
    pub fn is_import(&self) -> bool {
        matches!(self, Self::Import(_))
    }

    /// Check whether this statement is a export.
    pub fn is_export(&self) -> bool {
        matches!(self, Self::Export(_))
    }

    /// Check whether this statement is a output.
    pub fn is_output(&self) -> bool {
        matches!(self, Self::Output(_))
    }

    /// Check whether this statement is a parameter.
    pub fn is_parameter(&self) -> bool {
        matches!(self, Self::Parameter(_))
    }
}

impl Display for Statement {
    delegate! {
        to match self {
            Self::Rule(statement) => statement,
            Self::Fact(statement) => statement,
            Self::Import(statement) => statement,
            Self::Export(statement) => statement,
            Self::Output(statement) => statement,
            Self::Parameter(statement) => statement,
        } {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;
        }
    }
}

impl ComponentBehavior for Statement {
    delegate! {
        to match self {
            Self::Rule(statement) => statement,
            Self::Fact(statement) => statement,
            Self::Import(statement) => statement,
            Self::Export(statement) => statement,
            Self::Output(statement) => statement,
            Self::Parameter(statement) => statement,
        } {
            fn kind(&self) -> ProgramComponentKind;
            fn validate(&self) -> Result<(), ValidationReport>;
            fn boxed_clone(&self) -> Box<dyn ProgramComponent>;
        }
    }
}

impl ComponentSource for Statement {
    type Source = Origin;

    delegate! {
        to match self {
            Self::Rule(statement) => statement,
            Self::Fact(statement) => statement,
            Self::Import(statement) => statement,
            Self::Export(statement) => statement,
            Self::Output(statement) => statement,
            Self::Parameter(statement) => statement,
        } {
            fn origin(&self) -> Origin;
            fn set_origin(&mut self, origin: Origin);
        }
    }
}

impl ComponentIdentity for Statement {
    delegate! {
        to match self {
            Self::Rule(statement) => statement,
            Self::Fact(statement) => statement,
            Self::Import(statement) => statement,
            Self::Export(statement) => statement,
            Self::Output(statement) => statement,
            Self::Parameter(statement) => statement,
        } {
            fn id(&self) -> ProgramComponentId;
            fn set_id(&mut self, id: ProgramComponentId);
        }
    }
}

impl IterableComponent for Statement {
    delegate! {
        to match self {
            Self::Rule(statement) => statement,
            Self::Fact(statement) => statement,
            Self::Import(statement) => statement,
            Self::Export(statement) => statement,
            Self::Output(statement) => statement,
            Self::Parameter(statement) => statement,
        } {
            #[allow(late_bound_lifetime_arguments)]
            fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a>;
            #[allow(late_bound_lifetime_arguments)]
            fn children_mut<'a>(
                &'a mut self,
            ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a>;
        }
    }
}

impl IterableVariables for Statement {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        match self {
            Self::Rule(statement) => statement.variables(),
            Self::Fact(statement) => statement.variables(),
            Self::Import(statement) => statement.variables(),
            Self::Export(statement) => statement.variables(),
            Self::Output(statement) => statement.variables(),
            Self::Parameter(statement) => statement.variables(),
        }
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        match self {
            Self::Rule(statement) => statement.variables_mut(),
            Self::Fact(statement) => statement.variables_mut(),
            Self::Import(statement) => statement.variables_mut(),
            Self::Export(statement) => statement.variables_mut(),
            Self::Output(statement) => statement.variables_mut(),
            Self::Parameter(statement) => statement.variables_mut(),
        }
    }
}

impl IterablePrimitives for Statement {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        match self {
            Self::Rule(statement) => statement.primitive_terms(),
            Self::Fact(statement) => statement.primitive_terms(),
            Self::Import(statement) => statement.primitive_terms(),
            Self::Export(statement) => statement.primitive_terms(),
            Self::Output(statement) => statement.primitive_terms(),
            Self::Parameter(statement) => statement.primitive_terms(),
        }
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
        match self {
            Self::Rule(statement) => statement.primitive_terms_mut(),
            Self::Fact(statement) => statement.primitive_terms_mut(),
            Self::Import(statement) => statement.primitive_terms_mut(),
            Self::Export(statement) => statement.primitive_terms_mut(),
            Self::Output(statement) => statement.primitive_terms_mut(),
            Self::Parameter(statement) => statement.primitive_terms_mut(),
        }
    }
}

impl From<Rule> for Statement {
    fn from(value: Rule) -> Self {
        Self::Rule(value)
    }
}

impl From<Fact> for Statement {
    fn from(value: Fact) -> Self {
        Self::Fact(value)
    }
}

impl From<ImportDirective> for Statement {
    fn from(value: ImportDirective) -> Self {
        Self::Import(value)
    }
}

impl From<ExportDirective> for Statement {
    fn from(value: ExportDirective) -> Self {
        Self::Export(value)
    }
}

impl From<Output> for Statement {
    fn from(value: Output) -> Self {
        Self::Output(value)
    }
}

impl From<ParameterDeclaration> for Statement {
    fn from(value: ParameterDeclaration) -> Self {
        Self::Parameter(value)
    }
}
