//! This module defines [Map].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    components::{
        component_iterator, component_iterator_mut, tag::Tag, ComponentBehavior, ComponentIdentity,
        IterableComponent, IterablePrimitives, IterableVariables, ProgramComponent,
        ProgramComponentKind,
    },
    error::ValidationErrorBuilder,
    origin::Origin,
    pipeline::id::ProgramComponentId,
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
#[derive(Debug, Clone)]
pub struct Map {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Name of the map
    tag: Option<Tag>,

    /// List of tuples associating [Term]s with [Term]s
    map: Vec<(Term, Term)>,
}

impl Map {
    /// Create a new [Map].
    pub fn new<Pairs: IntoIterator<Item = (Term, Term)>>(name: &str, map: Pairs) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            tag: Some(Tag::new(name.to_string())),
            map: map.into_iter().collect(),
        }
    }

    /// Create a new [Map] without a name.
    pub fn new_unnamed<Pairs: IntoIterator<Item = (Term, Term)>>(map: Pairs) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
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
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            tag,
            map: map.into_iter().collect(),
        }
    }

    /// Create a new empty [Map].
    pub fn empty(name: &str) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            tag: Some(Tag::new(name.to_string())),
            map: Vec::default(),
        }
    }

    /// Create a new empty [Map].
    pub fn empty_tagged(tag: Option<Tag>) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            tag,
            map: Vec::default(),
        }
    }

    /// Create a new empty [Map].
    pub fn empty_unnamed() -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
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

    /// Return an iterator over the key value pairs in this map.
    pub fn key_value_mut(&mut self) -> impl Iterator<Item = &mut (Term, Term)> {
        self.map.iter_mut()
    }

    /// Return the number of entries in this map.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Return whether this map is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Find the given key and return a reference to the corresponding value,
    /// if it exists.
    pub fn get(&self, key: &Term) -> Option<&Term> {
        for (map_key, value) in self.key_value() {
            if map_key == key {
                return Some(value);
            }
        }

        None
    }

    /// Find the given key and return a mutable reference to the corresponding value,
    /// if it exists.
    pub fn get_mut(&mut self, key: &Term) -> Option<&mut Term> {
        for (map_key, value) in self.key_value_mut() {
            if map_key == key {
                return Some(value);
            }
        }

        None
    }

    /// Insert a new key-value-pair into the map.
    pub fn insert(&mut self, key: Term, value: Term) {
        self.map.push((key, value));
    }

    /// Remove all occurrences of the given key from the map.
    pub fn remove(&mut self, key: &Term) {
        self.map.retain(|(key_map, _)| key_map != key);
    }

    /// Check whether the given key is contained in this map.
    pub fn contains_key(&self, key: &Term) -> bool {
        self.map
            .iter()
            .find(|(key_map, _)| key_map == key)
            .is_some()
    }

    /// Return whether this term is ground,
    /// i.e. if it does not contain any variables.
    pub fn is_ground(&self) -> bool {
        self.key_value()
            .all(|(key, value)| key.is_ground() && value.is_ground())
    }

    /// Reduce this term by evaluating all contained expressions,
    /// and return a new [Map] with the same [Origin] as `self`.
    ///
    /// This function does nothing if `self` is not ground.
    pub fn reduce(&self) -> Self {
        Self {
            origin: self.origin.clone(),
            id: ProgramComponentId::default(),
            tag: self.tag.clone(),
            map: self
                .key_value()
                .map(|(key, value)| (key.reduce(), value.reduce()))
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

impl Eq for Map {}

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

impl ComponentBehavior for Map {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Map
    }

     fn validate(&self) -> Result<(), ValidationReport> {
        for (key, value) in self.key_value() {
            key.validate(builder)?;
            value.validate(builder)?;
        }

        Some(())
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for Map {
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

impl IterableComponent for Map {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let key_value_iterator =
            component_iterator(self.key_value().flat_map(|(a, b)| [a, b].into_iter()));

        Box::new(key_value_iterator)
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let key_value_iterator =
            component_iterator_mut(self.map.iter_mut().flat_map(|(a, b)| [a, b].into_iter()));

        Box::new(key_value_iterator)
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
