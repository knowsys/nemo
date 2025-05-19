//! This module defines [Primitive].

pub mod ground;
pub mod variable;

use std::{fmt::Display, hash::Hash};

use ground::GroundTerm;
use nemo_physical::datavalues::AnyDataValue;
use variable::{
    existential::ExistentialVariable, global::GlobalVariable, universal::UniversalVariable,
    Variable,
};

use crate::rule_model::{
    components::{
        ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent,
        IterableVariables, ProgramComponent, ProgramComponentKind,
    },
    error::{TranslationReport, ValidationReport},
    origin::Origin,
    pipeline::id::ProgramComponentId,
    translation::{ComponentParseReport, TranslationComponent},
};

use super::{value_type::ValueType, Term};

/// Primitive term
///
/// Represents a basic, indivisble values, which can either be [GroundTerm]s or [Variable]s.
/// Such terms are the atomic values used in the construction of more complex expressions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum Primitive {
    /// Variable
    Variable(Variable),
    /// Ground term
    Ground(GroundTerm),
}

impl Primitive {
    /// Parse a primitive term from a [`str`] reference.
    pub fn parse(input: &str) -> Result<Self, ComponentParseReport> {
        let Term::Primitive(term) = Term::parse(input)? else {
            return Err(ComponentParseReport::Translation(
                TranslationReport::default(),
            ));
        };

        Ok(term)
    }

    /// Create a universal variable term.
    pub fn universal_variable(name: &str) -> Self {
        Self::Variable(Variable::universal(name))
    }

    /// Create a anynmous variable term.
    pub fn anonymous_variable() -> Self {
        Self::Variable(Variable::anonymous())
    }

    /// Create a global variable term.
    pub fn global_variable(name: &str) -> Self {
        Self::Variable(Variable::global(name))
    }

    /// Create a existential variable term.
    pub fn existential_variable(name: &str) -> Self {
        Self::Variable(Variable::existential(name))
    }

    /// Create a groud term.
    pub fn ground(value: AnyDataValue) -> Self {
        Self::Ground(GroundTerm::new(value))
    }

    /// Create an IRI term.
    pub fn constant(iri: &str) -> Self {
        Self::Ground(GroundTerm::constant(iri))
    }

    /// Create a language tagged string term.
    pub fn language_tagged(value: &str, tag: &str) -> Self {
        Self::Ground(GroundTerm::language_tagged(value, tag))
    }

    /// Return `true` if this term is considered "ground".
    ///
    /// This is the case if the term is not a variable.
    pub fn is_ground(&self) -> bool {
        match self {
            Primitive::Variable(_) => false,
            Primitive::Ground(_) => true,
        }
    }

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        match self {
            Primitive::Variable(_) => ValueType::Any,
            Primitive::Ground(term) => term.value_type(),
        }
    }
}

impl From<Variable> for Primitive {
    fn from(value: Variable) -> Self {
        Self::Variable(value)
    }
}

impl From<UniversalVariable> for Primitive {
    fn from(value: UniversalVariable) -> Self {
        Self::from(Variable::from(value))
    }
}

impl From<ExistentialVariable> for Primitive {
    fn from(value: ExistentialVariable) -> Self {
        Self::from(Variable::from(value))
    }
}

impl From<GlobalVariable> for Primitive {
    fn from(value: GlobalVariable) -> Self {
        Self::from(Variable::from(value))
    }
}

impl From<GroundTerm> for Primitive {
    fn from(value: GroundTerm) -> Self {
        Self::Ground(value)
    }
}

impl From<AnyDataValue> for Primitive {
    fn from(value: AnyDataValue) -> Self {
        Self::Ground(GroundTerm::from(value))
    }
}

impl From<bool> for Primitive {
    fn from(value: bool) -> Self {
        Self::from(GroundTerm::from(value))
    }
}

impl From<i64> for Primitive {
    fn from(value: i64) -> Self {
        Self::from(GroundTerm::from(value))
    }
}

impl From<i32> for Primitive {
    fn from(value: i32) -> Self {
        Self::from(GroundTerm::from(value))
    }
}

impl From<u64> for Primitive {
    fn from(value: u64) -> Self {
        Self::from(GroundTerm::from(value))
    }
}

impl From<String> for Primitive {
    fn from(value: String) -> Self {
        Self::from(GroundTerm::from(value))
    }
}

impl From<&str> for Primitive {
    fn from(value: &str) -> Self {
        Self::from(GroundTerm::from(value))
    }
}

impl Display for Primitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Primitive::Variable(variable) => variable.fmt(f),
            Primitive::Ground(ground) => ground.fmt(f),
        }
    }
}

impl ComponentBehavior for Primitive {
    fn kind(&self) -> ProgramComponentKind {
        match self {
            Primitive::Variable(term) => term.kind(),
            Primitive::Ground(term) => term.kind(),
        }
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        match self {
            Primitive::Variable(term) => term.validate(),
            Primitive::Ground(term) => term.validate(),
        }
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        match self {
            Primitive::Variable(term) => term.boxed_clone(),
            Primitive::Ground(term) => term.boxed_clone(),
        }
    }
}

impl ComponentSource for Primitive {
    type Source = Origin;

    fn origin(&self) -> Origin {
        match self {
            Primitive::Variable(term) => term.origin(),
            Primitive::Ground(term) => term.origin(),
        }
    }

    fn set_origin(&mut self, origin: Origin) {
        match self {
            Primitive::Variable(term) => term.set_origin(origin),
            Primitive::Ground(term) => term.set_origin(origin),
        }
    }
}

impl ComponentIdentity for Primitive {
    fn id(&self) -> ProgramComponentId {
        match self {
            Primitive::Variable(term) => term.id(),
            Primitive::Ground(term) => term.id(),
        }
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        match self {
            Primitive::Variable(term) => term.set_id(id),
            Primitive::Ground(term) => term.set_id(id),
        }
    }
}

impl IterableComponent for Primitive {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        match self {
            Primitive::Variable(term) => term.children(),
            Primitive::Ground(term) => term.children(),
        }
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        match self {
            Primitive::Variable(term) => term.children_mut(),
            Primitive::Ground(term) => term.children_mut(),
        }
    }
}

impl IterableVariables for Primitive {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(
            match self {
                Primitive::Variable(variable) => Some(variable),
                Primitive::Ground(_) => None,
            }
            .into_iter(),
        )
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(
            match self {
                Primitive::Variable(variable) => Some(variable),
                Primitive::Ground(_) => None,
            }
            .into_iter(),
        )
    }
}

#[cfg(test)]
mod test {
    use crate::rule_model::components::term::primitive::variable::Variable;

    use super::Primitive;

    #[test]
    fn parse_primitive() {
        let variable = Primitive::parse("?x").unwrap();
        let ground = Primitive::parse("2").unwrap();

        assert_eq!(Primitive::from(Variable::universal("x")), variable);
        assert_eq!(Primitive::from(2), ground);
    }
}
