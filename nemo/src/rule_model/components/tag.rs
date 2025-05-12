//! This module defines [Tag].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::origin::Origin;

/// Name of a term or predicate
#[derive(Debug, Clone)]
pub struct Tag {
    /// Origin of this component.
    origin: Origin,

    /// Content of this tag
    tag: String,
}

impl Tag {
    /// Create a new [Tag].
    pub fn new(name: String) -> Self {
        Self {
            origin: Origin::Created,
            tag: name,
        }
    }

    /// Return the [Origin] associated with this tag.
    pub fn origin(&self) -> &Origin {
        &self.origin
    }

    /// Set the [Origin].
    pub fn set_origin(mut self, origin: Origin) {
        self.origin = origin;
    }

    /// Return the name of [Tag].
    pub fn name(&self) -> &str {
        &self.tag
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.tag)
    }
}

impl PartialEq for Tag {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag
    }
}

impl Eq for Tag {}

impl PartialOrd for Tag {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.tag.partial_cmp(&other.tag)
    }
}

impl Hash for Tag {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.tag.hash(state);
    }
}

impl From<String> for Tag {
    fn from(value: String) -> Self {
        Self {
            origin: Origin::Created,
            tag: value,
        }
    }
}

impl From<&str> for Tag {
    fn from(value: &str) -> Self {
        Self {
            origin: Origin::Created,
            tag: value.to_owned(),
        }
    }
}
