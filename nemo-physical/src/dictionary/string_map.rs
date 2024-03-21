//! This module implements a string based hash map.

use hashbrown::HashMap;

use super::bytes_buffer::{BytesBuffer, BytesRef, GlobalBytesBuffer};
use crate::datavalues::NullDataValue;

use delegate::delegate;

/// A struct that implements a mapping that uses Strings as keys.
///
/// String data is stored in a compact buffer to reduce memory overhead and fragmentation.
#[derive(Debug)]
pub(crate) struct StringMap<B: GlobalBytesBuffer, I> {
    mapping: HashMap<BytesRef<B>, I>,
    buffer_id: usize,
}

impl<B: GlobalBytesBuffer, I> StringMap<B, I> {
    /// Insert the given value at the given key.
    pub(crate) fn insert(&mut self, k: &str, v: I) {
        // Be careful not to insert the key again if it was already buffered:
        if !self.mapping.contains_key(k.as_bytes()) {
            let bref = B::push_bytes(self.buffer_id, k.as_bytes());
            self.mapping.insert(bref, v);
        } else {
            *self
                .mapping
                .get_mut(k.as_bytes())
                .expect("key was just checked to be contained") = v;
        }
    }

    /// Returns a reference to the current value for the given key,
    /// or None if there is no value for this key.
    pub(crate) fn get(&self, k: &str) -> Option<&I> {
        self.mapping.get(k.as_bytes())
    }

    /// Returns a mutable reference to the current value for the given key,
    /// or None if there is no value for this key.
    pub(crate) fn get_mut(&mut self, k: &str) -> Option<&mut I> {
        self.mapping.get_mut(k.as_bytes())
    }

    /// Returns the number of entries in the map.
    pub(crate) fn len(&self) -> usize {
        self.mapping.len()
    }

    /// Return whether the dictionary is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<B: GlobalBytesBuffer, I> Default for StringMap<B, I> {
    fn default() -> Self {
        StringMap {
            mapping: HashMap::new(),
            buffer_id: B::init_buffer(),
        }
    }
}

impl<B: GlobalBytesBuffer, I> Drop for StringMap<B, I> {
    fn drop(&mut self) {
        B::drop_buffer(self.buffer_id);
    }
}

crate::dictionary::bytes_buffer::declare_bytes_buffer!(NullMapBytesBuffer, NULL_MAP_BYTES_BUFFER);
/// A memory-efficient map from string identifiers to [NullDataValue]s.
/// This can be used in various contexts, e.g., to translate the local
/// string IDs of blank nodes in input files into data values.
#[derive(Debug, Default)]
pub struct NullMap(StringMap<NullMapBytesBuffer, NullDataValue>);
impl NullMap {
    delegate! {
        to self.0 {
            /// Insert the given value at the given key.
            pub fn insert(&mut self, k: &str, v: NullDataValue);
            /// Returns a reference to the current value for the given key,
            /// or None if there is no value for this key.
            pub fn get(&self, k: &str) -> Option<&NullDataValue>;
            /// Returns a mutable reference to the current value for the given key,
            /// or None if there is no value for this key.
            pub fn get_mut(&mut self, k: &str) -> Option<&mut NullDataValue>;
            /// Returns the number of entries in the map.
            pub fn len(&self) -> usize;
            /// Return whether the dictionary is empty.
            pub fn is_empty(&self) -> bool;
        }
    }
}

#[cfg(test)]
mod test {
    use crate::dictionary::bytes_buffer::{BytesBuffer, GlobalBytesBuffer};

    use super::StringMap;

    crate::dictionary::bytes_buffer::declare_bytes_buffer!(
        StringMapTestBytesBuffer,
        STRING_MAP_TEST_BUFFER
    );
    type TestStringMap<I> = StringMap<StringMapTestBytesBuffer, I>;

    #[test]
    fn add_and_get() {
        let mut map: TestStringMap<u64> = TestStringMap::default();

        map.insert("A", 1);
        map.insert("B", 2);
        assert_eq!(map.get("A"), Some(1).as_ref());
        assert_eq!(map.get("B"), Some(2).as_ref());
        map.insert("A", 5);
        assert_eq!(map.get("A"), Some(5).as_ref());

        let val_b = map.get_mut("B").expect("should be set");
        *val_b += 1;
        assert_eq!(map.get("B"), Some(3).as_ref());

        assert_eq!(map.len(), 2);
    }
}
