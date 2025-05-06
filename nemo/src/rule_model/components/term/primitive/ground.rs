//! This module defines [GroundTerm].

use std::{collections::HashMap, fmt::Display, hash::Hash};

use nemo_physical::datavalues::{AnyDataValue, DataValue, IriDataValue, ValueDomain};

use crate::rule_model::{
    components::{
        term::{value_type::ValueType, Term},
        ComponentBehavior, ComponentIdentity, IterableComponent, ProgramComponent,
        ProgramComponentKind,
    },
    error::{ComponentParseError, ValidationErrorBuilder},
    origin::Origin,
    pipeline::id::ProgramComponentId,
    translation::TranslationComponent,
};

/// Primitive ground term
///
/// Represents a basic, indivisible constant value like integers, or strings.
/// Such terms are the atomic values used in the construction of more complex expressions.
#[derive(Debug, Clone)]
pub struct GroundTerm {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Value of this term
    value: AnyDataValue,
}

impl GroundTerm {
    /// Create a new [GroundTerm].
    pub fn new(value: AnyDataValue) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            value,
        }
    }

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        ValueType::from(self.value.value_domain())
    }

    /// Return the [AnyDataValue] of this term
    pub fn value(&self) -> AnyDataValue {
        self.value.clone()
    }
}

impl From<AnyDataValue> for GroundTerm {
    fn from(value: AnyDataValue) -> Self {
        Self::new(value)
    }
}

impl From<i32> for GroundTerm {
    fn from(value: i32) -> Self {
        Self::new(AnyDataValue::new_integer_from_i64(value.into()))
    }
}

impl From<i64> for GroundTerm {
    fn from(value: i64) -> Self {
        Self::new(AnyDataValue::new_integer_from_i64(value))
    }
}

impl From<u64> for GroundTerm {
    fn from(value: u64) -> Self {
        Self::new(AnyDataValue::new_integer_from_u64(value))
    }
}

impl From<String> for GroundTerm {
    fn from(value: String) -> Self {
        Self::new(AnyDataValue::new_plain_string(value))
    }
}

impl From<&str> for GroundTerm {
    fn from(value: &str) -> Self {
        Self::new(AnyDataValue::new_plain_string(value.to_string()))
    }
}

impl From<IriDataValue> for GroundTerm {
    fn from(value: IriDataValue) -> Self {
        Self::new(AnyDataValue::new_iri(value.to_string()))
    }
}

impl Display for GroundTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl PartialEq for GroundTerm {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for GroundTerm {}

impl PartialOrd for GroundTerm {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl Hash for GroundTerm {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl ComponentBehavior for GroundTerm {
    fn kind(&self) -> ProgramComponentKind {
        match self.value.value_domain() {
            ValueDomain::PlainString => ProgramComponentKind::PlainString,
            ValueDomain::LanguageTaggedString => ProgramComponentKind::LanguageTaggedString,
            ValueDomain::Iri => ProgramComponentKind::Iri,
            ValueDomain::Float => ProgramComponentKind::Float,
            ValueDomain::Double => ProgramComponentKind::Double,
            ValueDomain::UnsignedLong
            | ValueDomain::NonNegativeLong
            | ValueDomain::UnsignedInt
            | ValueDomain::NonNegativeInt
            | ValueDomain::Int
            | ValueDomain::Long => ProgramComponentKind::Integer,
            ValueDomain::Tuple => ProgramComponentKind::Tuple,
            ValueDomain::Map => ProgramComponentKind::Map,
            ValueDomain::Boolean => ProgramComponentKind::Boolean,
            ValueDomain::Null => ProgramComponentKind::Null,
            ValueDomain::Other => ProgramComponentKind::Other,
        }
    }

    fn validate(&self, _builder: &mut ValidationErrorBuilder) -> Option<()> {
        Some(())
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for GroundTerm {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin
    }
}

impl IterableComponent for GroundTerm {}

impl GroundTerm {
    pub fn parse(input: &str) -> Result<Self, ComponentParseError>
    where
        Self: Sized,
    {
        let term = Term::parse(input)?;
        term.try_into_ground(&HashMap::default())
            .map_err(|_| ComponentParseError::ParseError)
    }
}

#[cfg(test)]
mod test {
    use super::GroundTerm;

    #[test]
    fn parse_ground_term() {
        let integer = GroundTerm::parse("2").unwrap();
        let string = GroundTerm::parse("\"abc\"").unwrap();

        assert_eq!(GroundTerm::from(2), integer);
        assert_eq!(GroundTerm::from("abc"), string);
    }
}
