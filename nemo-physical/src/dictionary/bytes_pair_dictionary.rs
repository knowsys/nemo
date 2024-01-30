use super::{
    bytes_buffer::{BytesRef, GlobalBytesBuffer},
    AddResult,
};

use hashbrown::{HashMap,HashSet};


/// A struct that implements a bijection between a pair of bytes and integers.
/// Integers are automatically assigned upon insertion.
///
/// TODO
/// It assumes that the first component is unfrequent, and the second component
/// is frequent. Two use cases are language tagged strings, and datatypes, e.g. "Deutschland"@de,
/// and "2010539060"^^http://www.w3.org/2001/XMLSchema#integer.
#[derive(Debug)]
pub(crate) struct BytesPairDictionary<B: GlobalBytesBuffer> {
    set: HashSet<BytesRef<B>>,
    map: HashMap<[BytesRef<B>;2], usize>,
    store: Vec<[BytesRef<B>;2]>,
    buffer_id: usize,
}


impl<B: GlobalBytesBuffer> BytesPairDictionary<B> {
    /// Construct a new and empty string dictionary.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Adds a new string pair to the dictionary when the pair is not stored yet.
    /// Ids are assigned in consecutive order.
    ///
    /// The result is an [AddResult] that indicates if the string pair was newly added,
    /// or previoulsy present.
    pub(crate) fn add_bytes_pair(&mut self, first: &[u8], second: &[u8]) -> AddResult {
        let first_bref = self.get_or_insert_bref(first);
        let second_bref = self.get_or_insert_bref(second);
        let pair = [first_bref, second_bref];
        match self.map.get(&pair) {
            Some(id) => AddResult::Known(*id),
            None => {
                let new_id = self.store.len();
                self.map.insert(pair, new_id);
                self.store.push(pair);
                AddResult::Fresh(new_id)
            }
        }
    }

    /// Looks for a given [&str] slice pair and returns `Some(id)` if the pair is in the dictionary,
    /// and `None` otherwise.
    pub(crate) fn str_pair_to_id(&self, first: &[u8], second: &[u8]) -> Option<usize> {
        match (self.set.get(first), self.set.get(second)) {
            (Some(fbref), Some(sbref)) => {
                let pair = [*fbref, *sbref];
                self.map.get(&pair).copied()
            },
            (_,_) => None,
        }
    }

    /// Returns the string pair associated with the `id`, or `None` if `id` doesn't
    /// exists in the dictioanry.
    pub(crate) fn id_to_bytes_pair(&self, id: usize) -> Option<(Vec<u8>, Vec<u8>)> {
        match self.store.get(id) {
            Some([first_bref, second_bref]) => Some((first_bref.to_vec(), second_bref.to_vec())),
            None => None,
        }
    }

    /// Returns the number of elements in the dictionary
    pub(crate) fn len(&self) -> usize {
        self.store.len()
    }

        /// Helper function to get a BytesRef from a string using the BytesBuffer
    fn get_or_insert_bref(&mut self, bytes: &[u8]) -> BytesRef<B> {
        match self.set.get(bytes) {
            Some(bref) => *bref,
            None => {
                let bref = B::push_bytes(self.buffer_id, bytes);
                self.set.insert(bref);
                bref
            }
        }
    }
}

impl <B: GlobalBytesBuffer>  Default for BytesPairDictionary<B> {
    fn default() -> Self {
        BytesPairDictionary {
            set: HashSet::new(),
            map: HashMap::new(),
            store: Vec::new(),
            buffer_id: B::init_buffer(),
        }
    }
}
