//! This module defines [Variable]

use std::fmt::Display;

use existential::ExistentialVariable;
use global::GlobalVariable;
use universal::UniversalVariable;

use crate::rule_model::{
    components::{
        ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent,
        ProgramComponentKind,
    },
    error::ValidationReport,
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

use super::ProgramComponent;

pub mod existential;
pub mod global;
pub mod universal;

/// Variable
///
/// A general placeholder that can be bound to any value.
/// We distinguish [UniversalVariable] and [ExistentialVariable].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum Variable {
    /// Universal variable
    Universal(UniversalVariable),
    /// Existential variable
    Existential(ExistentialVariable),
    /// Global variable
    Global(GlobalVariable),
}

impl Variable {
    /// Create a new universal variable.
    pub fn universal(name: &str) -> Self {
        Self::Universal(UniversalVariable::new(name))
    }

    /// Create a new existential variable.
    pub fn existential(name: &str) -> Self {
        Self::Existential(ExistentialVariable::new(name))
    }

    /// Create a new global variable.
    pub fn global(name: &str) -> Self {
        Self::Global(GlobalVariable::new(name))
    }

    /// Create a new anonymous variable.
    pub fn anonymous() -> Self {
        Self::Universal(UniversalVariable::new_anonymous())
    }

    /// Return the name of the variable or `None` if it is anonymous
    pub fn name(&self) -> Option<&str> {
        match self {
            Variable::Universal(variable) => variable.name(),
            Variable::Existential(variable) => Some(variable.name()),
            Variable::Global(global_variable) => Some(global_variable.name()),
        }
    }

    /// Return whether this is a universal variable.
    pub fn is_universal(&self) -> bool {
        matches!(self, Variable::Universal(_))
    }

    /// Return whether this is an existential variable.
    pub fn is_existential(&self) -> bool {
        matches!(self, Variable::Existential(_))
    }

    /// Return whether this is an global variable.
    pub fn is_global(&self) -> bool {
        matches!(self, Variable::Global(_))
    }

    /// Return whether this is an anonymous universal variable.
    pub fn is_anonymous(&self) -> bool {
        if let Variable::Universal(universal) = self {
            return universal.is_anonymous();
        }

        false
    }

    /// Change the name of this variable.
    pub fn rename(&mut self, name: String) {
        match self {
            Variable::Universal(variable) => variable.rename(name),
            Variable::Existential(variable) => variable.rename(name),
            Variable::Global(variable) => variable.rename(name),
        }
    }
}

impl From<UniversalVariable> for Variable {
    fn from(value: UniversalVariable) -> Self {
        Self::Universal(value)
    }
}

impl From<ExistentialVariable> for Variable {
    fn from(value: ExistentialVariable) -> Self {
        Self::Existential(value)
    }
}

impl From<GlobalVariable> for Variable {
    fn from(value: GlobalVariable) -> Self {
        Self::Global(value)
    }
}

impl Display for Variable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Variable::Universal(variable) => variable.fmt(f),
            Variable::Existential(variable) => variable.fmt(f),
            Variable::Global(variable) => variable.fmt(f),
        }
    }
}

impl ComponentBehavior for Variable {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Variable
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        match self {
            Variable::Universal(variable) => variable.validate(),
            Variable::Existential(variable) => variable.validate(),
            Variable::Global(variable) => variable.validate(),
        }
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}
impl ComponentSource for Variable {
    type Source = Origin;

    fn origin(&self) -> Origin {
        match self {
            Variable::Universal(variable) => variable.origin(),
            Variable::Existential(variable) => variable.origin(),
            Variable::Global(variable) => variable.origin(),
        }
    }

    fn set_origin(&mut self, origin: Origin) {
        match self {
            Variable::Universal(variable) => variable.set_origin(origin),
            Variable::Existential(variable) => variable.set_origin(origin),
            Variable::Global(variable) => variable.set_origin(origin),
        }
    }
}

impl ComponentIdentity for Variable {
    fn id(&self) -> ProgramComponentId {
        match self {
            Variable::Universal(variable) => variable.id(),
            Variable::Existential(variable) => variable.id(),
            Variable::Global(variable) => variable.id(),
        }
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        match self {
            Variable::Universal(variable) => variable.set_id(id),
            Variable::Existential(variable) => variable.set_id(id),
            Variable::Global(variable) => variable.set_id(id),
        }
    }
}

impl IterableComponent for Variable {}

#[cfg(test)]
mod test {
    use crate::rule_model::translation::TranslationComponent;

    use super::Variable;

    #[test]
    fn parse_variable() {
        let universal = Variable::parse("?x").unwrap();
        let existential = Variable::parse("!v").unwrap();
        let anonymous = Variable::parse("_").unwrap();

        assert_eq!(Variable::universal("x"), universal);
        assert_eq!(Variable::existential("v"), existential);
        assert_eq!(Variable::anonymous(), anonymous);
    }
}
