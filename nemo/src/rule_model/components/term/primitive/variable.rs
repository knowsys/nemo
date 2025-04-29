//! This module defines [Variable]

use std::fmt::Display;

use existential::ExistentialVariable;
use global::GlobalVariable;
use universal::UniversalVariable;

use crate::rule_model::{
    components::ProgramComponentKind, error::ValidationErrorBuilder, origin::Origin,
};

use super::ProgramComponent;

pub mod existential;
pub mod global;
pub mod universal;

/// Name of a variable
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VariableName(String);

impl VariableName {
    /// Create a new [VariableName].
    pub fn new(name: String) -> Self {
        Self(name)
    }

    /// Validate variable name.
    pub fn is_valid(&self) -> bool {
        !self.0.starts_with("__")
    }
}

impl Display for VariableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Variable
///
/// A general placeholder that can be bound to any value.
/// We distinguish [UniversalVariable] and [ExistentialVariable].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
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
    pub fn name(&self) -> Option<String> {
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
    pub fn rename(&mut self, name: VariableName) {
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

impl ProgramComponent for Variable {
    fn origin(&self) -> &Origin {
        match self {
            Variable::Universal(variable) => variable.origin(),
            Variable::Existential(variable) => variable.origin(),
            Variable::Global(variable) => variable.origin(),
        }
    }

    fn set_origin(self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        match self {
            Variable::Universal(variable) => Self::Universal(variable.set_origin(origin)),
            Variable::Existential(variable) => Self::Existential(variable.set_origin(origin)),
            Variable::Global(variable) => Self::Global(variable.set_origin(origin)),
        }
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        match &self {
            Variable::Universal(universal) => universal.validate(builder),
            Variable::Existential(existential) => existential.validate(builder),
            Variable::Global(existential) => existential.validate(builder),
        }
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Variable
    }
}

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
