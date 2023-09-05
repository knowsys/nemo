use super::Dictionary;
use super::EntryStatus;
use super::hash_map_dictionary::HashMapDictionary;
use super::dictionary_string::DictionaryString;

/// Bits in the size of address blocks allocated to sub-dictionaries
const BLOCKSIZE: u32 = 24;

/// Enum to specify what kind of data a dictionary supports.
#[derive(Clone, Debug)]
enum DictionaryType {
    /// Plain string dictionary
    String,
    /// Dictionary for long strings (blobs)
    Blob,
    /// Dictionary for strings with a fixed prefix and postfix
    Infix { prefix: String, postfix: String },
    /// Dictionary for numeric strings with a fixed prefix and postfix
    NumInfix { prefix: String, postfix: String },
    // /// Dictionary for named (actually: "numbered") nulls
    // NULL,
}

impl DictionaryType {
    /// Returns true if the given string is supported by a dictinoary of this type.
    fn supports(&self, ds: &mut DictionaryString) -> bool {
        match self {
            DictionaryType::String => !ds.is_long(),
            DictionaryType::Blob => ds.is_long(),
            DictionaryType::Infix{prefix,postfix} => false, // TODO
            DictionaryType::NumInfix{prefix,postfix} => false, // TODO
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
    dictblocks: Vec<(usize,usize)>,
    /// Vector of all sub-dictionaries, indexed by their sub-dictionary number
    dicts: Vec<DictRecord>,
}

impl Default for MetaDictionary {
    /// Initialise a [MetaDictionary].
    /// Sets up relevant default dictionaries for basic blocks.
    fn default() -> Self {
        // Initialize default dictionary and give it one block:
        let default_dict = Box::new(HashMapDictionary::new());
        let default_dict_record = DictRecord{dict: default_dict, dict_type: DictionaryType::String, gblocks: vec![0]};
        // Initialize blob dictionary and give it no blocks:
        let blob_dict = Box::new(HashMapDictionary::new());
        let blob_dict_record = DictRecord{dict: blob_dict, dict_type: DictionaryType::Blob, gblocks: Vec::new()};

        Self {
            dictblocks: vec![(0,0)],
            dicts: vec![default_dict_record, blob_dict_record],
        }
    }
}

impl MetaDictionary {

    /// Convert the local ID of a given dictionary to a global ID.
    /// The function assumes that the given local id exists, and will crash
    /// otherwise. It can safely be used for conversion of previously stored data.
    fn local_to_global_unchecked(&self, dict: usize, local_id: usize) -> usize {
        let lblock = local_id >> BLOCKSIZE;
        let offset = local_id % (1<<BLOCKSIZE);
        let gblock = self.dicts[dict].gblocks[lblock]; // Could fail if: (1) dictionary does not exist, or (2) block not used by dict

        (gblock << BLOCKSIZE) + offset
    }

    /// Convert the local ID of a given dictionary to a global ID.
    /// The function will check if the local id is supported by a previously
    /// reserved address block, and will reserve a new block for this
    /// dictionary otherwise. This is used when converting newly created ids.
    fn local_to_global(&mut self, dict: usize, local_id: usize) -> usize {
        let lblock = local_id >> BLOCKSIZE;
        let offset = local_id % (1<<BLOCKSIZE);
        let gblock = self.allocate_block(dict, lblock);

        (gblock << BLOCKSIZE) + offset
    }

    /// Find a global block that is allocated for the given dictionary and local block. If not
    /// allocated yet, a new block is reserved for this purpose. Allocation tries to preserve relative
    /// order and distance, and to keep some distance from other dictionary's blocks.
    fn allocate_block(&mut self, dict: usize, local_block: usize) -> usize {
        if self.dicts[dict].gblocks.len() <= local_block { // make space for block records up to required length
            self.dicts[dict].gblocks.resize(local_block+1, usize::MAX);
        } 
        if self.dicts[dict].gblocks[local_block] == usize::MAX { // allocate necessary new block
            let mut btl_index = local_block+1; // index of first allocated block to the left, +1 (so 0 means "no block to left")
            while btl_index > 0 && self.dicts[dict].gblocks[btl_index-1] == usize::MAX {
                btl_index -= 1;
            }

            let mut new_block: usize;
            if btl_index > 0 { // extrapolate where global block should be relative to last allocated local block
                new_block = self.dicts[dict].gblocks[btl_index-1] + btl_index - 1;
            } else { // determine "good" initial block for this dictionary (TODO)
                new_block = 0;
            }
            // Find first empty block right of the chosen new block
            while new_block < self.dictblocks.len() && self.dictblocks[new_block].1 != usize::MAX {
                new_block += 1;
            }
            if new_block >= self.dictblocks.len() {
                self.dictblocks.resize(new_block+1, (usize::MAX,usize::MAX));
            }
            self.dicts[dict].gblocks[local_block] = new_block;
            self.dictblocks[new_block] = (dict,local_block);
            new_block
        } else {
            self.dicts[dict].gblocks[local_block]
        }
    }

}

impl Dictionary for MetaDictionary {
    fn new() -> Self {
        Default::default()
    }

    fn add(&mut self, entry: String) -> EntryStatus {
        let mut ds = DictionaryString::new(entry.as_str());
        // for all (relevant) dictionaries
        //   check if string has a (local) id
        //   and, if so, map it to a global id        
        for (index,dr) in self.dicts.iter().enumerate() {
            if dr.dict_type.supports(&mut ds) {
                match dr.dict.index_of(entry.as_str()) {
                    Some(idx) => {return EntryStatus::Known(self.local_to_global_unchecked(index, idx)); },
                    _ => {}
                }
            }
        }
        // else add string to preferred dictionary
        let local_id: usize;
        let dict_idx: usize;
        if ds.is_long() {
            dict_idx = 1;
            local_id = self.dicts[1].dict.add(entry).value();
        } else {
            dict_idx = 0;
            local_id = self.dicts[0].dict.add(entry).value();
        }
        // compute global id based on block and local id, possibly allocating new block in the process
        EntryStatus::Fresh(self.local_to_global(dict_idx,local_id))
    }

    fn index_of(&self, entry: &str) -> Option<usize> {
        let mut ds = DictionaryString::new(entry);
        // for all (relevant) dictionaries
        //   check if string has a (local) id
        //   and, if so, map it to a global id        
        for (dict_index,dr) in self.dicts.iter().enumerate() {
            if dr.dict_type.supports(&mut ds) {
                let result = dr.dict.index_of(entry);
                if result.is_some() {
                    return Some(self.local_to_global_unchecked(dict_index, result.unwrap()));
                }
            }
        }
        None
    }

    fn entry(&self, index: usize) -> Option<String> {
        let gblock = index >> BLOCKSIZE;
        let offset = index % (1<<BLOCKSIZE);
        if self.dictblocks.len() <= gblock || self.dictblocks[gblock] == (usize::MAX,usize::MAX) {
            return None;
        }
        let (dict_id,lblock) = self.dictblocks[gblock];

        self.dicts[dict_id].dict.entry((lblock >> BLOCKSIZE) + offset)
    }

    fn len(&self) -> usize {
        let mut len = 0;
        for dr in self.dicts.iter() {
            len += dr.dict.len();
        }
        len
    }

    fn is_empty(&self) -> bool {
        for dr in self.dicts.iter() {
            if !dr.dict.is_empty() {
                return false;
            }
        }
        true
    }
}


#[cfg(test)]
mod test {
    use crate::dictionary::Dictionary;
    use crate::dictionary::EntryStatus;

    use crate::dictionary::dictionary_string::LONG_STRING_THRESHOLD;
    use super::MetaDictionary;

    /// Pads a string to make it longer than the threshold applied to distinguish blobs.
    fn long_string(s: &str) -> String {
        "#".to_string().repeat(LONG_STRING_THRESHOLD+1) + s
    }

    #[test]
    fn add_and_get() {
        let mut dict = MetaDictionary::default();

        let res1 = dict.add("entry0".to_string());
        let res2 = dict.add("entry1".to_string());
        let res3 = dict.add("entry0".to_string());
        let res4 = dict.add(long_string("long1"));
        let res5 = dict.add("entry2".to_string());
        let res6 = dict.add(long_string("long2"));
        let res7 = dict.add(long_string("long1"));

        let get1 = dict.entry(res1.value());
        let get2 = dict.entry(res2.value());
        let get4 = dict.entry(res4.value());
        let getnone1 = dict.entry(res6.value()+1); // unused but in an allocated block
        let getnone2 = dict.entry(1<< 30); // out of any allocated block

        assert_eq!(res1, EntryStatus::Fresh(res1.value()));
        assert_eq!(res2, EntryStatus::Fresh(res2.value()));
        assert_eq!(res3, EntryStatus::Known(res1.value()));
        assert_eq!(res4, EntryStatus::Fresh(res4.value()));
        assert_eq!(res5, EntryStatus::Fresh(res5.value()));
        assert_eq!(res6, EntryStatus::Fresh(res6.value()));
        assert_eq!(res7, EntryStatus::Known(res4.value()));

        assert_eq!(dict.index_of("entry0"), Some(res1.value()));
        assert_eq!(dict.index_of("entry1"), Some(res2.value()));
        assert_eq!(dict.index_of(long_string("long1").as_str()), Some(res4.value()));

        assert_eq!(get1.unwrap(), "entry0".to_string());
        assert_eq!(get2.unwrap(), "entry1".to_string());
        assert_eq!(get4.unwrap(), long_string("long1"));

        assert_eq!(getnone1, None);
        assert_eq!(getnone2, None);
    }
}
