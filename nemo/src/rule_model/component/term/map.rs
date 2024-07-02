//! This module defines [Map]

use std::{collections::BTreeMap, fmt::Display, hash::Hash};

use crate::rule_model::{component::ProgramComponent, origin::Origin};

use super::Term;

/// Map term
#[derive(Debug, Clone, Eq)]
pub struct Map {
    /// Origin of this component
    origin: Origin,

    /// Map associating [Term]s with [Term]s
    map: BTreeMap<Term, Term>,
}

impl Display for Map {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl PartialEq for Map {
    fn eq(&self, other: &Self) -> bool {
        self.map == other.map
    }
}

impl PartialOrd for Map {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.map.partial_cmp(&other.map)
    }
}

impl Hash for Map {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.map.hash(state);
    }
}

impl ProgramComponent for Map {
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
