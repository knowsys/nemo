//! This module defines [GroundTerm].

use std::{fmt::Display, hash::Hash};

use nemo_physical::datavalues::{AnyDataValue, DataValue, IriDataValue, ValueDomain};

use crate::rule_model::{
    components::{term::value_type::ValueType, ProgramComponent, ProgramComponentKind},
    error::ValidationErrorBuilder,
    origin::Origin,
};

/// Primitive ground term
///
/// Represents a basic, indivisible constant value like integers, or strings.
/// Such terms are the atomic values used in the construction of more complex expressions.
#[derive(Debug, Clone, Eq)]
pub struct GroundTerm {
    /// Origin of this component
    origin: Origin,
    /// Value of this term
    value: AnyDataValue,
}

impl GroundTerm {
    /// Create a new [GroundTerm].
    pub fn new(value: AnyDataValue) -> Self {
        Self {
            origin: Origin::Created,
            value,
        }
    }

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        match self.value.value_domain() {
            ValueDomain::Float
            | ValueDomain::Double
            | ValueDomain::UnsignedLong
            | ValueDomain::NonNegativeLong
            | ValueDomain::UnsignedInt
            | ValueDomain::NonNegativeInt
            | ValueDomain::Long
            | ValueDomain::Int => ValueType::Number,
            ValueDomain::PlainString => ValueType::String,
            ValueDomain::LanguageTaggedString => ValueType::LanguageString,
            ValueDomain::Iri => ValueType::Constant,
            ValueDomain::Tuple => ValueType::Tuple,
            ValueDomain::Map => ValueType::Map,
            ValueDomain::Boolean => ValueType::Boolean,
            ValueDomain::Null => ValueType::Null,
            ValueDomain::Other => ValueType::Other,
        }
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

impl ProgramComponent for GroundTerm {
    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self, _builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        Some(())
    }

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
}

#[cfg(test)]
mod test {
    use crate::rule_model::{
        components::{
            parse::ComponentParseError,
            term::{primitive::Primitive, Term},
        },
        translation::TranslationComponent,
    };

    use super::GroundTerm;

    impl GroundTerm {
        fn parse(input: &str) -> Result<Self, ComponentParseError>
        where
            Self: Sized,
        {
            let term = Term::parse(input)?;

            if let Term::Primitive(Primitive::Ground(ground)) = term {
                return Ok(ground);
            }

            Err(ComponentParseError::ParseError)
        }
    }

    #[test]
    fn parse_ground_term() {
        let integer = GroundTerm::parse("2").unwrap();
        let string = GroundTerm::parse("\"abc\"").unwrap();

        assert_eq!(GroundTerm::from(2), integer);
        assert_eq!(GroundTerm::from("abc"), string);
    }
}
