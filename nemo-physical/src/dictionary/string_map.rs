use hashbrown::HashMap;

use super::bytes_buffer::{BytesRef, GlobalBytesBuffer};

/// A struct that implements a mapping that uses Strings as keys.
///
/// String data is stored in a compact buffer to reduce memory overhead and fragmentation.
#[derive(Debug)]
pub(crate) struct StringMap<B: GlobalBytesBuffer, I> {
    mapping: HashMap<BytesRef<B>, I>,
    buffer_id: usize,
}

impl<B: GlobalBytesBuffer, I> StringMap<B, I> {
    /// Construct a new and empty map.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Insert the given value at the given key. The previous entry is
    /// returned: either None if the key was not set, or Some(v) for the
    /// prior value v.
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
        let mut map: TestStringMap<u64> = TestStringMap::new();

        map.insert("A", 1);
        map.insert("B", 2);
        assert_eq!(map.get("A"), Some(1).as_ref());
        assert_eq!(map.get("B"), Some(2).as_ref());
        map.insert("A", 5);
        assert_eq!(map.get("A"), Some(5).as_ref());

        let val_b = map.get_mut("B").expect("should be set");
        *val_b += 1;
        assert_eq!(map.get("B"), Some(3).as_ref());
    }
}
