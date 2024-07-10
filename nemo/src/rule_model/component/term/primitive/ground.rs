//! This module defines [GroundTerm].

use std::{fmt::Display, hash::Hash};

use nemo_physical::datavalues::{AnyDataValue, IriDataValue};

use crate::rule_model::{
    component::ProgramComponent, error::ProgramConstructionError, origin::Origin,
};

/// Primitive ground term
///
/// Represents a basic, indivisble constant value like integers, or strings.
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
    fn parse(_string: &str) -> Result<Self, ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }

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

    fn validate(&self) -> Result<(), ProgramConstructionError>
    where
        Self: Sized,
    {
        Ok(())
    }
}
