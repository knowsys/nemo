//! This module defines [Tuple].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{component::ProgramComponent, origin::Origin};

use super::Term;

/// An ordered list of terms
#[derive(Debug, Clone, Eq)]
pub struct Tuple {
    /// Origin of this component
    origin: Origin,

    /// Ordered list of terms contained in this tuple
    terms: Vec<Term>,
}

impl Display for Tuple {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        self.terms == other.terms
    }
}

impl Hash for Tuple {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.terms.hash(state);
    }
}

impl ProgramComponent for Tuple {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        todo!()
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self) -> Result<(), crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }
}
