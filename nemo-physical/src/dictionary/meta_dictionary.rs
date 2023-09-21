use super::DictionaryString;
use super::HashMapDictionary;
use super::InfixDictionary;
use super::AddResult;
use super::Dictionary;

/// Bits in the size of address blocks allocated to sub-dictionaries
const BLOCKSIZE: u32 = 24;

/// Enum to specify what kind of data a dictionary supports.
#[derive(Clone, Debug)]
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

/// A dictionary that combines several other dictionaries.
#[derive(Debug)]
pub struct MetaDictionary {
    /// Vector to map global block numbers to pairs (sub-dictionary, local block number)
    dictblocks: Vec<(usize, usize)>,
    /// Vector of all sub-dictionaries, indexed by their sub-dictionary number
    dicts: Vec<DictRecord>,
}

impl Default for MetaDictionary {
    /// Initialise a [MetaDictionary].
    /// Sets up relevant default dictionaries for basic blocks.
    fn default() -> Self {
        // Initialize default dictionary and give it no blocks:
        let default_dict = Box::new(HashMapDictionary::new());
        let default_dict_record = DictRecord {
            dict: default_dict,
            dict_type: DictionaryType::String,
            gblocks: Vec::new(), //vec![0],
        };
        // Initialize blob dictionary and give it no blocks:
        let blob_dict = Box::new(HashMapDictionary::new());
        let blob_dict_record = DictRecord {
            dict: blob_dict,
            dict_type: DictionaryType::Blob,
            gblocks: Vec::new(),
        };

        // For testing, we hard-code some infix dictionaries:
        Self {
            dictblocks: Vec::new(),//vec![(1, 0)],
            dicts: vec![default_dict_record, blob_dict_record,
                Self::prepare_infix_dict_record("<http://www.wikidata.org/entity/",">"), 
                Self::prepare_infix_dict_record("<http://www.wikidata.org/entity/statement/",">"),
                Self::prepare_infix_dict_record("<http://www.wikidata.org/reference/",">"),
                Self::prepare_infix_dict_record("\"","\"^^<http://www.w3.org/2001/XMLSchema#dateTime>")],
        }
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

    /// Creates a new empty infix dictionary for the given prefix and suffix.
    fn prepare_infix_dict_record(prefix: &str, suffix: &str) -> DictRecord {
        let infix_dict = Box::new(InfixDictionary::new(prefix.to_string(), suffix.to_string()));
        DictRecord {
            dict: infix_dict,
            dict_type: DictionaryType::Infix { prefix: prefix.to_string(), suffix: suffix.to_string() },
            gblocks: Vec::new(),
        }
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
                // determine "good" initial block for this dictionary (TODO)
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
        let mut dict_idx = self.dicts.len();
        // for all (relevant) dictionaries; most suitable dicts are to the left, hence rev()
        for dr in self.dicts.iter().rev() {
            dict_idx -= 1;
            if dr.dict_type.supports(&ds) {
                if best_dict_idx == usize::MAX {
                    best_dict_idx = dict_idx;
                }
                //   check if string has a (local) id
                match dr.dict.fetch_id(ds.as_str()) {
                    Some(idx) => { //   and, if so, map it to a global id
                        return AddResult::Known(self.local_to_global_unchecked(dict_idx, idx));
                    }
                    _ => {}
                }
            }
        }
        assert!(best_dict_idx<self.dicts.len());

        // else add string to preferred dictionary
        let local_id = self.dicts[best_dict_idx].dict.add_dictionary_string(ds).value();
        // compute global id based on block and local id, possibly allocating new block in the process
        AddResult::Fresh(self.local_to_global(best_dict_idx, local_id))
    }

    fn fetch_id(&self, string: &str) -> Option<usize> {
        let ds = DictionaryString::new(string);
        let mut dict_idx = self.dicts.len();
        // for all (relevant) dictionaries; most suitable dicts are to the left, hence rev()
        for dr in self.dicts.iter().rev() {
            dict_idx -= 1;
            //   check if string has a (local) id
            if dr.dict_type.supports(&ds) {
                let result = dr.dict.fetch_id(string);
                //   and, if so, map it to a global id
                if result.is_some() {
                    return Some(self.local_to_global_unchecked(dict_idx, result.unwrap()));
                }
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
        println!("Computing total meta dict length ...");
        for dr in self.dicts.iter() {
            println!("+ dict with {} entries.", dr.dict.len());
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
        let res2 = dict.add_string("<http://www.wikidata.org/entity/Q1>".to_string());
        let res3 = dict.add_string("<http://www.wikidata.org/entity/Q2>".to_string());
        let res4 = dict.add_string("<http://www.wikidata.org/entity/Q3>".to_string()); 
        let res5 = dict.add_string("<https://www.wikidata.org/wiki/Special:EntityData/Q31>".to_string());

        let res1known = dict.add_string("entry0".to_string());
        let res2known = dict.add_string("<http://www.wikidata.org/entity/Q1>".to_string());
        let res3known = dict.add_string("<http://www.wikidata.org/entity/Q2>".to_string());
        let res4known = dict.add_string("<http://www.wikidata.org/entity/Q3>".to_string()); 
        let res5known = dict.add_string("<https://www.wikidata.org/wiki/Special:EntityData/Q31>".to_string());

        assert_eq!(res1, AddResult::Fresh(res1.value()));
        assert_eq!(res2, AddResult::Fresh(res2.value()));
        assert_eq!(res3, AddResult::Fresh(res3.value()));
        assert_eq!(res4, AddResult::Fresh(res4.value()));
        assert_eq!(res5, AddResult::Fresh(res5.value()));

        assert_eq!(res1known, AddResult::Known(res1.value()));
        assert_eq!(res2known, AddResult::Known(res2.value()));
        assert_eq!(res3known, AddResult::Known(res3.value()));
        assert_eq!(res4known, AddResult::Known(res4.value()));
        assert_eq!(res5known, AddResult::Known(res5.value()));
    }
}
