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
    components::{IterableVariables, ProgramComponent, ProgramComponentKind},
    error::{ComponentParseError, ValidationErrorBuilder},
    origin::Origin,
    translation::TranslationComponent,
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
    pub fn parse(input: &str) -> Result<Self, ComponentParseError> {
        let Term::Primitive(term) = Term::parse(input)? else {
            return Err(ComponentParseError::ParseError);
        };

        Ok(term)
    }

    /// Return `true` if this term is considered "ground".
    ///
    /// This is the case if the term is not a universal or existential variable
    /// (we consider global variables to be ground).
    pub fn is_ground(&self) -> bool {
        match self {
            Primitive::Variable(variable) => variable.is_global(),
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

impl ProgramComponent for Primitive {
    fn origin(&self) -> &Origin {
        match self {
            Self::Variable(variable) => variable.origin(),
            Self::Ground(ground) => ground.origin(),
        }
    }

    fn set_origin(self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        match self {
            Self::Variable(variable) => Self::Variable(variable.set_origin(origin)),
            Self::Ground(ground) => Self::Ground(ground.set_origin(origin)),
        }
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        match self {
            Primitive::Variable(variable) => variable.validate(builder),
            Primitive::Ground(ground) => ground.validate(builder),
        }
    }

    fn kind(&self) -> ProgramComponentKind {
        match self {
            Primitive::Variable(primitive) => primitive.kind(),
            Primitive::Ground(primitive) => primitive.kind(),
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
