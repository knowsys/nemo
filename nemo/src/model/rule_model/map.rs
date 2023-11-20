use std::collections::BTreeMap;

use super::{Constant, Identifier};

/// A key in a [Map].
#[derive(Debug, Eq, PartialEq, Clone, Hash, PartialOrd, Ord)]
pub enum Key {
    /// A string key.
    String(String),
    /// An identifier key.
    Identifier(Identifier),
}

impl Key {
    /// Construct a new [Key] from a [String].
    pub fn string(s: String) -> Self {
        Self::String(s)
    }

    /// Construct a new [Key] from an [Identifier].
    pub fn identifier(i: Identifier) -> Self {
        Self::Identifier(i)
    }

    /// Construct a new [Identifier] [Key] from a [&str].
    pub(crate) fn identifier_from_str(s: &str) -> Self {
        Self::Identifier(Identifier(s.into()))
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::String(s) => write!(f, "{s}"),
            Key::Identifier(i) => write!(f, "{i}"),
        }
    }
}

/// A Map: a [Constant] assigning values (which can be arbitrary
/// [Constants][Constant]) to [Keys][Key].
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord, Default)]
pub struct Map {
    pub(crate) pairs: BTreeMap<Key, Constant>,
}

impl Map {
    /// Construct an empty [Map].
    pub fn new() -> Self {
        Default::default()
    }

    /// Construct a [Map] containg a single [Key] and [Constant].
    pub fn singleton(key: Key, value: Constant) -> Self {
        Self::from_iter(Some((key, value)))
    }

    /// An iterator over the pairs in the map
    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Constant)> {
        self.pairs.iter()
    }
}

impl std::fmt::Display for Map {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ ")?;

        for (key, value) in &self.pairs {
            write!(f, "{key}: {value}")?;
        }

        write!(f, " }}")
    }
}

impl FromIterator<(Key, Constant)> for Map {
    fn from_iter<T: IntoIterator<Item = (Key, Constant)>>(iter: T) -> Self {
        Self {
            pairs: iter.into_iter().collect(),
        }
    }
}

impl From<Vec<(Key, Constant)>> for Map {
    fn from(pairs: Vec<(Key, Constant)>) -> Self {
        pairs.into_iter().collect()
    }
}
