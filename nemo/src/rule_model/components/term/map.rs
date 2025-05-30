//! This module defines [Map].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    components::{
        tag::Tag, IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind,
    },
    error::ValidationErrorBuilder,
    origin::Origin,
    substitution::Substitution,
};

use super::{
    primitive::{variable::Variable, Primitive},
    value_type::ValueType,
    Term,
};

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

    /// Create a new [Map] without a name.
    pub fn new_unnamed<Pairs: IntoIterator<Item = (Term, Term)>>(map: Pairs) -> Self {
        Self {
            origin: Origin::Created,
            tag: None,
            map: map.into_iter().collect(),
        }
    }

    /// Create a new [Map] with a given (optional) [Tag].
    pub fn new_tagged<Pairs: IntoIterator<Item = (Term, Term)>>(
        tag: Option<Tag>,
        map: Pairs,
    ) -> Self {
        Self {
            origin: Origin::Created,
            tag,
            map: map.into_iter().collect(),
        }
    }

    /// Create a new empty [Map].
    pub fn empty(name: &str) -> Self {
        Self {
            origin: Origin::Created,
            tag: Some(Tag::new(name.to_string())),
            map: Vec::default(),
        }
    }

    /// Create a new empty [Map].
    pub fn empty_unnamed() -> Self {
        Self {
            origin: Origin::Created,
            tag: None,
            map: Vec::default(),
        }
    }

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        ValueType::Map
    }

    /// Return the tag of this map.
    pub fn tag(&self) -> Option<Tag> {
        self.tag.clone()
    }

    /// Return an iterator over the key value pairs in this map.
    pub fn key_value(&self) -> impl Iterator<Item = &(Term, Term)> {
        self.map.iter()
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
        self.key_value()
            .all(|(key, value)| key.is_ground() && value.is_ground())
    }

    /// Reduce the [Term]s in each key-value pair returning a copy.
    pub fn reduce_with_substitution(&self, bindings: &Substitution) -> Self {
        Self {
            origin: self.origin,
            tag: self.tag.clone(),
            map: self
                .key_value()
                .map(|(key, value)| {
                    (
                        key.reduce_with_substitution(bindings),
                        value.reduce_with_substitution(bindings),
                    )
                })
                .collect(),
        }
    }
}

impl Display for Map {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}{{",
            self.tag
                .as_ref()
                .map_or(String::default(), |tag| tag.to_string())
        ))?;

        for (term_index, (key, value)) in self.map.iter().enumerate() {
            f.write_fmt(format_args!("{key}: {value}"))?;

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

impl IterablePrimitives for Map {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        Box::new(
            self.map
                .iter()
                .flat_map(|(key, value)| key.primitive_terms().chain(value.primitive_terms())),
        )
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
        Box::new(
            self.map.iter_mut().flat_map(|(key, value)| {
                key.primitive_terms_mut().chain(value.primitive_terms_mut())
            }),
        )
    }
}

impl IntoIterator for Map {
    type Item = (Term, Term);
    type IntoIter = std::vec::IntoIter<(Term, Term)>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

#[cfg(test)]
mod test {
    use crate::rule_model::{
        components::term::{primitive::variable::Variable, Term},
        translation::TranslationComponent,
    };

    use super::Map;

    #[test]
    fn parse_map() {
        let map = Map::parse("m { ?x = 5 }").unwrap();

        assert_eq!(
            Map::new(
                "m",
                vec![(Term::from(Variable::universal("x")), Term::from(5)),]
            ),
            map
        );
    }
}
