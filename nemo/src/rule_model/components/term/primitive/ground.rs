//! This module defines [GroundTerm].

use std::{fmt::Display, hash::Hash};

use nemo_physical::datavalues::{AnyDataValue, DataValue, IriDataValue, ValueDomain};

use crate::rule_model::{
    components::{
        term::{value_type::ValueType, Term},
        ComponentBehavior, ComponentIdentity, IterableComponent, ProgramComponent,
        ProgramComponentKind,
    },
    error::ValidationReport,
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

use super::Primitive;

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

    /// Create an IRI term.
    pub fn constant(iri: &str) -> Self {
        Self::new(AnyDataValue::new_iri(iri.to_owned()))
    }

    /// Create a language tagged string term.
    pub fn language_tagged(value: &str, tag: &str) -> Self {
        Self::new(AnyDataValue::new_language_tagged_string(
            value.to_owned(),
            tag.to_owned(),
        ))
    }

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        ValueType::from(self.value.value_domain())
    }

    /// Return the [AnyDataValue] of this term.
    pub fn value(&self) -> AnyDataValue {
        self.value.clone()
    }

    /// Replace the value.
    pub fn set_value(&mut self, value: AnyDataValue) {
        self.value = value;
    }
}

impl TryFrom<Term> for GroundTerm {
    type Error = Term;

    fn try_from(value: Term) -> Result<Self, Self::Error> {
        if !value.is_ground() {
            return Err(value);
        }

        let reduced = value.reduce().ok_or(value)?;

        // TODO: AnyDataValue at some point should support complex values
        match reduced {
            Term::Primitive(primitive) => {
                if let Primitive::Ground(ground) = primitive {
                    return Ok(ground);
                } else {
                    unreachable!("value is ground");
                }
            }
            Term::Aggregate(term) => Err(Term::Aggregate(term)),
            Term::FunctionTerm(term) => Err(Term::FunctionTerm(term)),
            Term::Map(term) => Err(Term::Map(term)),
            Term::Operation(term) => Err(Term::Operation(term)),
            Term::Tuple(term) => Err(Term::Tuple(term)),
        }
    }
}

impl From<AnyDataValue> for GroundTerm {
    fn from(value: AnyDataValue) -> Self {
        Self::new(value)
    }
}

impl From<bool> for GroundTerm {
    fn from(value: bool) -> Self {
        Self::new(AnyDataValue::new_boolean(value))
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

    fn validate(&self) -> Result<(), ValidationReport> {
        ValidationReport::default().result()
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
    // TODO:

    pub fn parse(_input: &str) -> Result<Self, ()>
    where
        Self: Sized,
    {
        // let term = Term::parse(input)?;
        // term.try_into_ground(&HashMap::default())
        //     .map_err(|_| ComponentParseError::ParseError)
        todo!()
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
