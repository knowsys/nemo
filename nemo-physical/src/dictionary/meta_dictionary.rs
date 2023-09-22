use super::DictionaryString;
use super::HashMapDictionary;
use super::InfixDictionary;
use super::AddResult;
use super::Dictionary;

use lru::LruCache;
use std::collections::HashMap;
use std::hash::{Hash,Hasher};
use std::num::NonZeroUsize;
use std::borrow::Borrow;

/// Number of recent occurrences of a string pattern required for creating a bespoke dictionary
const DICT_THRESHOLD: u32 = 500;

/// Bits in the size of address blocks allocated to sub-dictionaries.
/// For example 24bit blocks each contain 2^24 addresses, and there are
/// 2^8=256 such blocks available within the u32 address range (and
/// 2^40 in 64bits).
const BLOCKSIZE: u32 = 24;

// The code for [StringPair] and [StringPairKey] is inspired by
// https://stackoverflow.com/a/50478038 ("How to avoid temporary allocations when using a complex key for a HashMap?").
// The goal is just that, since we have very frequent hashmap lookups here.
#[derive(Debug, Eq, Hash, PartialEq)]
struct StringPair {
    first: String,
    second: String,
}
impl StringPair {
    fn new(first: impl Into<String>, second: impl Into<String>) -> Self {
        StringPair { first: first.into(), second: second.into() }
    }
}

trait StringPairKey {
    fn to_key(&self) -> (&str, &str);
}
impl Hash for dyn StringPairKey + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_key().hash(state)
    }
}
impl PartialEq for dyn StringPairKey + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.to_key() == other.to_key()
    }
}
impl Eq for dyn StringPairKey + '_ {}

impl StringPairKey for StringPair {
    fn to_key(&self) -> (&str, &str) {
        (&self.first, &self.second)
    }
}
impl<'a> StringPairKey for (&'a str, &'a str) {
    fn to_key(&self) -> (&str, &str) {
        (self.0, self.1)
    }
}

impl<'a> Borrow<dyn StringPairKey + 'a> for StringPair {
    fn borrow(&self) -> &(dyn StringPairKey + 'a) {
        self
    }
}
impl<'a> Borrow<dyn StringPairKey + 'a> for (&'a str, &'a str) {
    fn borrow(&self) -> &(dyn StringPairKey + 'a) {
        self
    }
}
// End of code for [StringPair].

/// Enum to specify what kind of data a dictionary supports.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum DictionaryType {
    /// Plain string dictionary
    String,
    /// Dictionary for long strings (blobs)
    Blob,
    /// Dictionary for strings with a fixed prefix and suffix
    Infix { prefix: String, suffix: String },
    // /// Dictionary for numeric strings with a fixed prefix and suffix
    //NumInfix { prefix: String, suffix: String },
    // /// Dictionary for named (actually: "numbered") nulls
    // NULL,
}
impl DictionaryType {
    /// Returns true if the given string is supported by a dictinoary of this type.
    fn supports(&self, ds: &DictionaryString) -> bool {
        match self {
            DictionaryType::String => !ds.is_long(),
            DictionaryType::Blob => ds.is_long(),
            DictionaryType::Infix { prefix, suffix } => !ds.is_long() && ds.has_infix(prefix,suffix),
            //DictionaryType::NumInfix { prefix, suffix } => false, // TODO
        }
    }
}

/// Struct to hold relevant information about a sub-dictionary.
#[derive(Debug)]
pub struct DictRecord {
    /// Pointer to the actual dictionary object
    dict: Box<dyn Dictionary>,
    /// Type of the dictionary
    dict_type: DictionaryType,
    /// Vector to associate local address block numbers to global block numbers
    gblocks: Vec<usize>,
}

/// Iterator-like struct for cycling over suitable dictionaries for some [DictionaryString].
/// It is mostly a device for reusing some iteration code, and requires all calls to provide
/// the data to work on.
struct DictIterator {
    /// Internal encoding of a "position" in a single value.
    /// It is interpreted as follows: 0 is the initial state ("before" any value),
    /// 1 is the "fitting infix dictionary" (if any)
    position: usize,
}
impl DictIterator {
    /// Constructor.
    fn new() -> Self {
        DictIterator { position: 0 }
    }

    /// Advance iterator, and return the id of the next dictionary.
    fn next(&mut self, ds: &DictionaryString, md: &MetaDictionary) -> usize {
        // First look for infix dictionary:
        if self.position == 0 {
            self.position = 1;
            if ds.infixable() {
                #[allow(trivial_casts)]
                if let Some(dict_idx) = md.infix_dicts.get( (ds.prefix(),ds.suffix()).borrow() as &dyn StringPairKey ) {
                    return *dict_idx;
                }
            }
        }

        // Finally, interpret self.position-1 as an index in md.generic_dicts:
        while self.position <= md.generic_dicts.len() {
            self.position += 1;
            if md.dicts[md.generic_dicts[self.position-2]].dict_type.supports(&ds) {
                return md.generic_dicts[self.position-2];
            }
        }

        usize::MAX // No further dictionaries left
    }

}

/// A dictionary that combines several other dictionaries.
#[derive(Debug)]
pub struct MetaDictionary {
    /// Vector to map global block numbers to pairs (sub-dictionary, local block number)
    dictblocks: Vec<(usize, usize)>,
    /// Vector of all sub-dictionaries, indexed by their sub-dictionary number
    dicts: Vec<DictRecord>,
    /// Data structure to hold counters for recently encountered dictionary types that we might
    /// want to make a dictionary for.
    dict_candidates: LruCache<StringPair,u32>,
    /// Auxiliary datastructure for finding fitting infix dictionaries.
    infix_dicts: HashMap<StringPair,usize>,
    /// Auxiliary datastructure for finding fitting general purpose dictionaries.
    generic_dicts: Vec<usize>,
}

impl Default for MetaDictionary {
    /// Initialise a [MetaDictionary].
    /// Sets up relevant default dictionaries for basic blocks.
    fn default() -> Self {
        let mut result = Self {
            dictblocks: Vec::new(),//vec![(1, 0)],
            dicts: Vec::new(), //vec![default_dict_record, blob_dict_record],
            dict_candidates: LruCache::new(NonZeroUsize::new(100).unwrap()),
            infix_dicts: HashMap::new(),
            generic_dicts: Vec::new(),
        };

        result.add_dictionary(DictionaryType::Blob);
        result.add_dictionary(DictionaryType::String);

        result
    }
}

impl MetaDictionary {
    /// Construct a new and empty dictionary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Convert the local ID of a given dictionary to a global ID.
    /// The function assumes that the given local id exists, and will crash
    /// otherwise. It can safely be used for conversion of previously stored data.
    fn local_to_global_unchecked(&self, dict: usize, local_id: usize) -> usize {
        let lblock = local_id >> BLOCKSIZE;
        let offset = local_id % (1 << BLOCKSIZE);
        let gblock = self.dicts[dict].gblocks[lblock]; // Could fail if: (1) dictionary does not exist, or (2) block not used by dict

        (gblock << BLOCKSIZE) + offset
    }

    /// Convert the local ID of a given dictionary to a global ID.
    /// The function will check if the local id is supported by a previously
    /// reserved address block, and will reserve a new block for this
    /// dictionary otherwise. This is used when converting newly created ids.
    fn local_to_global(&mut self, dict: usize, local_id: usize) -> usize {
        let lblock = local_id >> BLOCKSIZE;
        let offset = local_id % (1 << BLOCKSIZE);
        let gblock = self.allocate_block(dict, lblock);

        (gblock << BLOCKSIZE) + offset
    }

    /// Find a global block that is allocated for the given dictionary and local block. If not
    /// allocated yet, a new block is reserved for this purpose. Allocation tries to preserve relative
    /// order and distance, and to keep some distance from other dictionary's blocks.
    fn allocate_block(&mut self, dict: usize, local_block: usize) -> usize {
        if self.dicts[dict].gblocks.len() <= local_block {
            // make space for block records up to required length
            self.dicts[dict].gblocks.resize(local_block + 1, usize::MAX);
        }
        if self.dicts[dict].gblocks[local_block] == usize::MAX {
            // allocate necessary new block
            let mut btl_index = local_block + 1; // index of first allocated block to the left, +1 (so 0 means "no block to left")
            while btl_index > 0 && self.dicts[dict].gblocks[btl_index - 1] == usize::MAX {
                btl_index -= 1;
            }

            let mut new_block: usize;
            if btl_index > 0 {
                // extrapolate where global block should be relative to last allocated local block
                new_block = self.dicts[dict].gblocks[btl_index - 1] + btl_index - 1;
            } else {
                // TODO determine "good" initial block for this dictionary
                new_block = 0;
            }
            // Find first empty block right of the chosen new block
            while new_block < self.dictblocks.len() && self.dictblocks[new_block].1 != usize::MAX {
                new_block += 1;
            }
            if new_block >= self.dictblocks.len() {
                self.dictblocks
                    .resize(new_block + 1, (usize::MAX, usize::MAX));
            }
            self.dicts[dict].gblocks[local_block] = new_block;
            self.dictblocks[new_block] = (dict, local_block);
            new_block
        } else {
            self.dicts[dict].gblocks[local_block]
        }
    }

    /// Creates and adds a new (sub)dictionary of the given type.
    /// No blocks are allocated yet. It is not checked if a similar dictionary is already there.
    fn add_dictionary(&mut self, dt: DictionaryType) {
        let dict: Box<dyn Dictionary>;
        match dt {
            DictionaryType::String => {
                dict = Box::new(HashMapDictionary::new());
                self.generic_dicts.push(self.dicts.len());
            },
            DictionaryType::Blob => {
                dict = Box::new(HashMapDictionary::new());
                self.generic_dicts.push(self.dicts.len());
            },
            DictionaryType::Infix { ref prefix, ref suffix } => {
                dict = Box::new(InfixDictionary::new(prefix.to_string(), suffix.to_string()));
                self.infix_dicts.insert(StringPair::new(prefix.to_string(),suffix.to_string()), self.dicts.len());
            },
        }
        let dr = DictRecord {
            dict: dict,
            dict_type: dt,
            gblocks: Vec::new(),
        };
        self.dicts.push(dr);
    }
}

impl Dictionary for MetaDictionary {
    fn add_string(&mut self, string: String) -> AddResult {
        self.add_dictionary_string(DictionaryString::from_string(string))
    }

    fn add_str(&mut self, string: &str) -> AddResult {
        self.add_dictionary_string(DictionaryString::new(string))
    }

    fn add_dictionary_string(&mut self, ds: DictionaryString) -> AddResult {
        let mut best_dict_idx = usize::MAX;

        // Look up new entry in all applicable dictionaries.
        let mut d_it = DictIterator::new();
        let mut dict_idx: usize;
        while {dict_idx=d_it.next(&ds, self); dict_idx} != usize::MAX {
            if best_dict_idx == usize::MAX {
                best_dict_idx = dict_idx;
            }
            if let Some(idx) = self.dicts[dict_idx].dict.fetch_id_for_dictionary_string(&ds) {
                return AddResult::Known(self.local_to_global_unchecked(dict_idx, idx));
            }
        }
        // Performance note: The remaining code is only executed once per unique string (i.e., typically much fewer times than the above).
        assert!(best_dict_idx<self.dicts.len());

        // Consider creating a new dictionary for the new entry:
        if ds.infixable() {
            if let DictionaryType::String = self.dicts[best_dict_idx].dict_type {
                #[allow(trivial_casts)]
                if let Some(count) = self.dict_candidates.get_mut( (ds.prefix(),ds.suffix()).borrow() as &dyn StringPairKey ) {
                    *count += 1;
                    if *count > DICT_THRESHOLD { // Performance note: The following code is very rarely executed.
                        self.dict_candidates.pop(&StringPair::new(ds.prefix().to_string(),ds.suffix().to_string() ));
                        best_dict_idx = self.dicts.len();
                        self.add_dictionary(DictionaryType::Infix { prefix: ds.prefix().to_string(), suffix: ds.suffix().to_string() });
                        log::info!("Initialized new infix dictionary (#{}) for '{}...{}'.",best_dict_idx,ds.prefix(),ds.suffix());
                    }
                } else {
                    self.dict_candidates.put(StringPair::new(ds.prefix().to_string(),ds.suffix().to_string() ), 1);
                }
            }
        }

        // Add entry to preferred dictionary
        let local_id = self.dicts[best_dict_idx].dict.add_dictionary_string(ds).value();
        // compute global id based on block and local id, possibly allocating new block in the process
        AddResult::Fresh(self.local_to_global(best_dict_idx, local_id))
    }

    fn fetch_id(&self, string: &str) -> Option<usize> {
        let ds = DictionaryString::new(string);

        // Look up new entry in all applicable dictionaries.
        let mut d_it = DictIterator::new();
        let mut dict_idx: usize;
        while {dict_idx=d_it.next(&ds, self); dict_idx} != usize::MAX {
            if let Some(idx) = self.dicts[dict_idx].dict.fetch_id(string) {
                return Some(self.local_to_global_unchecked(dict_idx, idx));
            }
        }
        None
    }

    fn get(&self, id: usize) -> Option<String> {
        let gblock = id >> BLOCKSIZE;
        let offset = id % (1 << BLOCKSIZE);
        if self.dictblocks.len() <= gblock || self.dictblocks[gblock] == (usize::MAX, usize::MAX) {
            return None;
        }
        let (dict_id, lblock) = self.dictblocks[gblock];

        self.dicts[dict_id].dict.get((lblock >> BLOCKSIZE) + offset)
    }

    fn len(&self) -> usize {
        let mut len = 0;
        log::info!("Computing total meta dict length ...");
        for dr in self.dicts.iter() {
            log::info!("+ {} entries in dict {:?}", dr.dict.len(), dr.dict_type);
            len += dr.dict.len();
        }
        len
    }
}

#[cfg(test)]
mod test {
    use crate::dictionary::AddResult;
    use crate::dictionary::Dictionary;

    use super::MetaDictionary;
    use super::DICT_THRESHOLD;
    use crate::dictionary::dictionary_string::LONG_STRING_THRESHOLD;

    /// Pads a string to make it longer than the threshold applied to distinguish blobs.
    fn long_string(s: &str) -> String {
        "#".to_string().repeat(LONG_STRING_THRESHOLD + 1) + s
    }

    #[test]
    fn add_and_get() {
        let mut dict = MetaDictionary::default();

        let res1 = dict.add_string("entry0".to_string());
        let res2 = dict.add_string("entry1".to_string());
        let res3 = dict.add_string("entry0".to_string());
        let res4 = dict.add_string(long_string("long1"));
        let res5 = dict.add_string("entry2".to_string());
        let res6 = dict.add_string(long_string("long2"));
        let res7 = dict.add_string(long_string("long1"));

        let get1 = dict.get(res1.value());
        let get2 = dict.get(res2.value());
        let get4 = dict.get(res4.value());
        let getnone1 = dict.get(res6.value() + 1); // unused but in an allocated block
        let getnone2 = dict.get(1 << 30); // out of any allocated block

        assert_eq!(res1, AddResult::Fresh(res1.value()));
        assert_eq!(res2, AddResult::Fresh(res2.value()));
        assert_eq!(res3, AddResult::Known(res1.value()));
        assert_eq!(res4, AddResult::Fresh(res4.value()));
        assert_eq!(res5, AddResult::Fresh(res5.value()));
        assert_eq!(res6, AddResult::Fresh(res6.value()));
        assert_eq!(res7, AddResult::Known(res4.value()));

        assert_eq!(dict.fetch_id("entry0"), Some(res1.value()));
        assert_eq!(dict.fetch_id("entry1"), Some(res2.value()));
        assert_eq!(
            dict.fetch_id(long_string("long1").as_str()),
            Some(res4.value())
        );

        assert_eq!(get1.unwrap(), "entry0".to_string());
        assert_eq!(get2.unwrap(), "entry1".to_string());
        assert_eq!(get4.unwrap(), long_string("long1"));

        assert_eq!(getnone1, None);
        assert_eq!(getnone2, None);
    }

    #[test]
    fn add_and_get_prefix() {
        let mut dict = MetaDictionary::default();

        let res1 = dict.add_string("entry0".to_string());

        for i in 0..DICT_THRESHOLD+2 {
            dict.add_string("<http://www.wikidata.org/entity/Q".to_string() + i.to_string().as_str() + ">");
            dict.add_string("\"". to_string() + i.to_string().as_str() + "\"^^<http://www.w3.org/2001/XMLSchema#decimal>");
        }

        let res2 = dict.add_string("<http://www.wikidata.org/entity/Another>".to_string());
        let res3 = dict.add_string("\"42.3\"^^<http://www.w3.org/2001/XMLSchema#decimal>".to_string());

        let res1known = dict.add_string("entry0".to_string());
        let res2known = dict.add_string("<http://www.wikidata.org/entity/Another>".to_string());
        let res3known = dict.add_string("\"42.3\"^^<http://www.w3.org/2001/XMLSchema#decimal>".to_string());

        assert_eq!(res1, AddResult::Fresh(res1.value()));
        assert_eq!(res2, AddResult::Fresh(res2.value()));
        assert_eq!(res3, AddResult::Fresh(res3.value()));

        assert_eq!(res1known, AddResult::Known(res1.value()));
        assert_eq!(res2known, AddResult::Known(res2.value()));
        assert_eq!(res3known, AddResult::Known(res3.value()));
    }
}
