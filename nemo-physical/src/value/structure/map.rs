//! Maps, owned and borrowed.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::Range,
    ptr, slice,
};

use crate::{
    dictionary::{Dictionary, StoredStructure},
    value::{DataValue, ValueRef},
};

/// An owned map from data values to data values with an optional label,
/// written `{host = "localhost"}` or `server{host = "localhost"}` in term syntax.
/// Its borrowed view is [MapRef].
///
/// Keys are unique, and the pairs are in key order.
///
/// The constructors sort the pairs,
/// so maps with the same pairs are equal however they were built.
#[derive(Debug, Clone)]
pub struct MapValue {
    /// The map's label, if any.
    label: Option<Box<str>>,
    /// The key-value pairs, sorted by key.
    pairs: Box<[(DataValue, DataValue)]>,
}

impl MapValue {
    /// Creates an unlabeled map from key-value pairs,
    /// or `None` if two pairs share a key.
    pub fn new(pairs: impl IntoIterator<Item = (DataValue, DataValue)>) -> Option<Self> {
        Self::build(None, pairs)
    }

    /// Creates a labeled map from key-value pairs,
    /// or `None` if two pairs share a key.
    pub fn labeled(
        label: impl Into<Box<str>>,
        pairs: impl IntoIterator<Item = (DataValue, DataValue)>,
    ) -> Option<Self> {
        Self::build(Some(label.into()), pairs)
    }

    /// Sorts the pairs by key and rejects duplicate keys.
    fn build(
        label: Option<Box<str>>,
        pairs: impl IntoIterator<Item = (DataValue, DataValue)>,
    ) -> Option<Self> {
        let mut pairs: Vec<_> = pairs.into_iter().collect();
        pairs.sort_by(|left, right| left.0.cmp(&right.0));
        if pairs.windows(2).any(|window| window[0].0 == window[1].0) {
            return None;
        }

        Some(Self {
            label,
            pairs: pairs.into(),
        })
    }

    /// Returns the map's label, if any.
    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    /// Returns the key-value pairs, sorted by key in term order.
    pub fn pairs(&self) -> &[(DataValue, DataValue)] {
        &self.pairs
    }

    /// Returns the value at the key, if the map holds it.
    pub fn get(&self, key: ValueRef<'_>) -> Option<&DataValue> {
        self.pairs
            .binary_search_by(|pair| pair.0.as_ref().cmp(&key))
            .ok()
            .map(|index| &self.pairs[index].1)
    }

    /// Returns the number of key-value pairs.
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    /// Returns whether the map has no pairs.
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// Returns the borrowed view of this map.
    pub fn as_ref(&self) -> MapRef<'_> {
        MapRef(Backing::Owned(self))
    }
}

impl PartialEq for MapValue {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for MapValue {}

impl Ord for MapValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(&other.as_ref())
    }
}

impl PartialOrd for MapValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for MapValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl fmt::Display for MapValue {
    /// Writes the map in term syntax, its keys and values in canonical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

/// A borrowed map from data values to data values with an optional label,
/// written `{host = "localhost"}` or `server{host = "localhost"}` in term syntax.
/// Its owned form is [MapValue].
///
/// Keys are unique, and the pairs are in key order.
#[derive(Clone, Copy)]
pub struct MapRef<'a>(Backing<'a>);

/// Where a [MapRef] reads its map.
#[derive(Clone, Copy)]
enum Backing<'a> {
    /// An owned map.
    Owned(&'a MapValue),
    /// An entry of a dictionary's map store, read in place:
    /// a key or a value is decoded when it is read.
    Stored {
        /// The dictionary that stores the map.
        dictionary: &'a Dictionary,
        /// The index of the entry.
        index: u64,
    },
}

impl<'a> MapRef<'a> {
    /// Creates the view of an entry of a dictionary's map store.
    ///
    /// The store must hold the entry.
    pub(crate) fn stored(dictionary: &'a Dictionary, index: u64) -> Self {
        Self(Backing::Stored { dictionary, index })
    }

    /// Returns the map's label, if any.
    pub fn label(self) -> Option<&'a str> {
        match self.0 {
            Backing::Owned(map) => map.label(),
            Backing::Stored { dictionary, index } => dictionary.stored_map(index).label(),
        }
    }

    /// Returns the number of key-value pairs.
    pub fn len(self) -> usize {
        match self.0 {
            Backing::Owned(map) => map.len(),
            // A stored map's elements are its keys and values, alternating.
            Backing::Stored { dictionary, index } => dictionary.stored_map(index).len() / 2,
        }
    }

    /// Returns whether the map has no pairs.
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Returns the value at the key, if the map holds it.
    pub fn get(self, key: ValueRef<'_>) -> Option<ValueRef<'a>> {
        match self.0 {
            Backing::Owned(map) => map.get(key).map(DataValue::as_ref),
            Backing::Stored { dictionary, index } => {
                let structure = dictionary.stored_map(index);
                let (mut low, mut high) = (0, structure.len() / 2);

                while low < high {
                    let middle = low + (high - low) / 2;
                    let probed = structure
                        .element(2 * middle)
                        .expect("the pair lies within the map");
                    match probed.cmp(&key) {
                        Ordering::Less => low = middle + 1,
                        Ordering::Greater => high = middle,
                        Ordering::Equal => return structure.element(2 * middle + 1),
                    }
                }

                None
            }
        }
    }

    /// Returns the key-value pairs, sorted by key in term order.
    pub fn pairs(self) -> impl ExactSizeIterator<Item = (ValueRef<'a>, ValueRef<'a>)> {
        match self.0 {
            Backing::Owned(map) => Pairs::Owned(map.pairs.iter()),
            Backing::Stored { dictionary, index } => {
                let structure = dictionary.stored_map(index);
                Pairs::Stored {
                    structure,
                    pairs: 0..structure.len() / 2,
                }
            }
        }
    }

    /// Clones the viewed map into an owned [MapValue].
    pub fn to_owned(self) -> MapValue {
        match self.0 {
            Backing::Owned(map) => map.clone(),
            // The stored pairs are in key order and their keys unique already.
            Backing::Stored { .. } => MapValue {
                label: self.label().map(Box::from),
                pairs: self
                    .pairs()
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect(),
            },
        }
    }

    /// Returns the index of the viewed map in a dictionary's map store,
    /// if the view reads it from there.
    pub(crate) fn index_in(self, dictionary: &Dictionary) -> Option<u64> {
        match self.0 {
            Backing::Stored {
                dictionary: own,
                index,
            } if ptr::eq(own, dictionary) => Some(index),
            _ => None,
        }
    }
}

impl PartialEq for MapRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        // One dictionary stores one map once.
        if let (
            Backing::Stored { dictionary, index },
            Backing::Stored {
                dictionary: other_dictionary,
                index: other_index,
            },
        ) = (self.0, other.0)
            && ptr::eq(dictionary, other_dictionary)
        {
            return index == other_index;
        }

        self.label() == other.label() && self.len() == other.len() && self.pairs().eq(other.pairs())
    }
}

impl Eq for MapRef<'_> {}

impl Ord for MapRef<'_> {
    /// Orders by label, an unlabeled map first, then by the pairs in key order.
    fn cmp(&self, other: &Self) -> Ordering {
        self.label()
            .cmp(&other.label())
            .then_with(|| self.pairs().cmp(other.pairs()))
    }
}

impl PartialOrd for MapRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for MapRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label().hash(state);
        state.write_usize(self.len());
        for pair in self.pairs() {
            pair.hash(state);
        }
    }
}

impl fmt::Debug for MapRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MapRef({self})")
    }
}

impl fmt::Display for MapRef<'_> {
    /// Writes the map in term syntax, its keys and values in canonical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write_term(f, true)
    }
}

impl MapRef<'_> {
    /// Writes the map in term syntax, the IRIs that read back bare written bare if `bare`.
    pub(crate) fn write_term(&self, f: &mut fmt::Formatter<'_>, bare: bool) -> fmt::Result {
        if let Some(label) = self.label() {
            super::write_label(f, label, bare)?;
        }
        f.write_str("{")?;
        for (index, (key, value)) in self.pairs().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            key.write_term(f, bare)?;
            f.write_str(" = ")?;
            value.write_term(f, bare)?;
        }
        f.write_str("}")
    }
}

/// The key-value pairs of a [MapRef], in key order.
enum Pairs<'a> {
    /// The pairs of an owned map.
    Owned(slice::Iter<'a, (DataValue, DataValue)>),
    /// The pairs of a stored map, by position.
    Stored {
        /// The stored map.
        structure: StoredStructure<'a>,
        /// The positions of the pairs not yet read.
        pairs: Range<usize>,
    },
}

impl<'a> Iterator for Pairs<'a> {
    type Item = (ValueRef<'a>, ValueRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Pairs::Owned(pairs) => pairs
                .next()
                .map(|(key, value)| (key.as_ref(), value.as_ref())),
            Pairs::Stored { structure, pairs } => pairs.next().map(|pair| {
                let element = |position| {
                    structure
                        .element(position)
                        .expect("the pairs lie within the map")
                };
                (element(2 * pair), element(2 * pair + 1))
            }),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Pairs::Owned(pairs) => pairs.size_hint(),
            Pairs::Stored { pairs, .. } => pairs.size_hint(),
        }
    }
}

impl ExactSizeIterator for Pairs<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_canonicalize_pair_order() {
        let key_a = DataValue::string("a");
        let key_b = DataValue::string("b");
        let one = DataValue::integer(1);
        let two = DataValue::integer(2);

        let forward = MapValue::new([(key_a.clone(), one.clone()), (key_b.clone(), two.clone())])
            .expect("keys are unique");
        let backward = MapValue::new([(key_b, two.clone()), (key_a.clone(), one.clone())])
            .expect("keys are unique");
        assert_eq!(forward, backward);
        assert_eq!(MapValue::new([(key_a.clone(), one), (key_a, two)]), None);
    }
}
