//! This module defines [ImportExportSpec].

use crate::rule_model::{
    components::{
        tag::Tag,
        term::{
            primitive::{variable::Variable, Primitive},
            Term,
        },
        IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind,
    },
    error::ValidationErrorBuilder,
    origin::Origin,
};

use super::attribute::ImportExportAttribute;

/// Attribute value pairs defining import or export parameters
#[derive(Debug, Clone, Eq)]
pub struct ImportExportSpec {
    /// Origin of this component
    origin: Origin,

    /// File format
    format: Tag,

    /// List of tuples containing attribute-value-pairs
    map: Vec<(ImportExportAttribute, Term)>,
}

impl ImportExportSpec {
    /// Create a new [ImportExportSpec].
    pub fn new<Pairs: IntoIterator<Item = (ImportExportAttribute, Term)>>(
        format: &str,
        map: Pairs,
    ) -> Self {
        Self {
            origin: Origin::Created,
            format: Tag::new(format.to_string()),
            map: map.into_iter().collect(),
        }
    }

    /// Create a new empty [ImportExportSpec].
    pub fn empty(format: &str) -> Self {
        Self {
            origin: Origin::Created,
            format: Tag::new(format.to_string()),
            map: Vec::default(),
        }
    }

    /// Return the tag of this map.
    pub fn format(&self) -> Tag {
        self.format.clone()
    }

    /// Return an iterator over the key value pairs in this map.
    pub fn key_value(&self) -> impl Iterator<Item = &(ImportExportAttribute, Term)> {
        self.map.iter()
    }

    /// Return an iterator over the values in this map.
    pub fn values(&self) -> impl Iterator<Item = &Term> {
        self.map.iter().map(|(_key, value)| value)
    }

    /// Return a mutable iterator over the values in this map.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut Term> {
        self.map.iter_mut().map(|(_key, value)| value)
    }

    /// Return the number of entries in this map.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Return whether this map is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return whether this term is ground,
    /// i.e. if it does not contain any variables.
    pub fn is_ground(&self) -> bool {
        self.key_value().all(|(_key, value)| value.is_ground())
    }

    /// Reduce the [Term]s in each key-value pair returning a copy.
    pub fn reduce(&self) -> Self {
        Self {
            origin: self.origin,
            format: self.format.clone(),
            map: self
                .key_value()
                .map(|(key, value)| (key.clone(), value.reduce()))
                .collect(),
        }
    }
}

impl std::fmt::Display for ImportExportSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}{{", self.format))?;

        for (term_index, (key, value)) in self.map.iter().enumerate() {
            f.write_fmt(format_args!("{}: {}", key, value))?;

            if term_index < self.map.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str("}")
    }
}

impl PartialEq for ImportExportSpec {
    fn eq(&self, other: &Self) -> bool {
        self.format == other.format && self.map == other.map
    }
}

impl PartialOrd for ImportExportSpec {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.map.partial_cmp(&other.map)
    }
}

impl std::hash::Hash for ImportExportSpec {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.format.hash(state);
        self.map.hash(state);
    }
}

impl ProgramComponent for ImportExportSpec {
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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        for (key, value) in self.key_value() {
            key.validate(builder)?;
            value.validate(builder)?;
        }

        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Map
    }
}

impl IterableVariables for ImportExportSpec {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.map.iter().flat_map(|(_key, value)| value.variables()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(
            self.map
                .iter_mut()
                .flat_map(|(_key, value)| value.variables_mut()),
        )
    }
}

impl IterablePrimitives for ImportExportSpec {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        Box::new(
            self.map
                .iter()
                .flat_map(|(_key, value)| value.primitive_terms()),
        )
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
        Box::new(
            self.map
                .iter_mut()
                .flat_map(|(_key, value)| value.primitive_terms_mut()),
        )
    }
}
