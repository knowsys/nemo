//! This module defines [Map]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    components::{IterableVariables, ProgramComponent, Tag},
    error::ValidationErrorBuilder,
    origin::Origin,
};

use super::{primitive::variable::Variable, Term};

/// Map
///
/// A collection of key-value pairs,
/// associating [Term]s with each other.
#[derive(Debug, Clone, Eq)]
pub struct Map {
    /// Origin of this component
    origin: Origin,

    /// Name of the map
    tag: Option<Tag>,

    /// List of tuples associating [Term]s with [Term]s
    map: Vec<(Term, Term)>,
}

impl Map {
    /// Create a new [Map].
    pub fn new<Pairs: IntoIterator<Item = (Term, Term)>>(name: &str, map: Pairs) -> Self {
        Self {
            origin: Origin::Created,
            tag: Some(Tag::new(name.to_string())),
            map: map.into_iter().collect(),
        }
    }

    /// Create a new [Map].
    pub fn new_unnamed<Pairs: IntoIterator<Item = (Term, Term)>>(map: Pairs) -> Self {
        Self {
            origin: Origin::Created,
            tag: None,
            map: map.into_iter().collect(),
        }
    }
}

impl Display for Map {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}{{",
            self.tag.as_ref().map_or("", |tag| &tag.0)
        ))?;

        for (term_index, (key, value)) in self.map.iter().enumerate() {
            f.write_fmt(format_args!("{} = {}", key, value))?;

            if term_index < self.map.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str("}")
    }
}

impl PartialEq for Map {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag && self.map == other.map
    }
}

impl PartialOrd for Map {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.map.partial_cmp(&other.map)
    }
}

impl Hash for Map {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.tag.hash(state);
        self.map.hash(state);
    }
}

impl ProgramComponent for Map {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ValidationError>
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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl IterableVariables for Map {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(
            self.map
                .iter()
                .flat_map(|(key, value)| key.variables().chain(value.variables())),
        )
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(
            self.map
                .iter_mut()
                .flat_map(|(key, value)| key.variables_mut().chain(value.variables_mut())),
        )
    }
}
