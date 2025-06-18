//! This module defines [Bag].

use std::{collections::HashMap, hash::Hash};

/// Mapping from a key to a list of values
#[derive(Debug)]
pub(crate) struct Bag<K, V>(HashMap<K, Vec<V>>);

impl<K, V> Default for Bag<K, V> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K, V> Bag<K, V>
where
    K: Eq + Hash,
{
    /// Returns the first entry for a given key.
    pub fn get_unique(&self, key: &K) -> Option<&V> {
        self.0.get(key).map(|v| &v[0])
    }

    /// Return a reference to the list of values
    /// asociated with the given key.
    #[allow(unused)]
    pub fn get(&self, key: &K) -> &[V] {
        self.0.get(key).map(|v| &**v).unwrap_or(&[])
    }

    /// Return a mutable reference to the list of values
    /// associated with the fiven key.
    pub fn get_mut(&mut self, key: K) -> &mut Vec<V> {
        self.0.entry(key).or_default()
    }

    /// Delete all entries.
    pub fn clear(&mut self) {
        self.0.clear();
    }
}
