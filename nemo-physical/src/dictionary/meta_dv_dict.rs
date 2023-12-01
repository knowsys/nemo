use crate::datavalues::ValueDomain;
use crate::datavalues::{DataValue,AnyDataValue};

use super::AddResult;
use super::DvDict;
use super::IriDvDictionary;
use super::LangStringDvDictionary;
use super::OtherDvDictionary;
use super::StringDvDictionary;

// use lru::LruCache;
// use std::borrow::Borrow;
// use std::collections::HashMap;
// use std::hash::{Hash, Hasher};
// use std::num::NonZeroUsize;

// /// Number of recent occurrences of a string pattern required for creating a bespoke dictionary
// const DICT_THRESHOLD: u32 = 500;

/// Bits in the size of address blocks allocated to sub-dictionaries.
/// For example 24bit blocks each contain 2^24 addresses, and there are
/// 2^8=256 such blocks available within the u32 address range (and
/// 2^40 in 64bits).
const BLOCKSIZE: u32 = 24;

// The code for [StringPair] and [StringPairKey] is inspired by
// https://stackoverflow.com/a/50478038 ("How to avoid temporary allocations when using a complex key for a HashMap?").
// The goal is just that, since we have very frequent hashmap lookups here.
// #[derive(Debug, Eq, Hash, PartialEq)]
// struct StringPair {
//     first: String,
//     second: String,
// }

// impl StringPair {
//     fn new(first: impl Into<String>, second: impl Into<String>) -> Self {
//         StringPair {
//             first: first.into(),
//             second: second.into(),
//         }
//     }
// }

// trait StringPairKey {
//     fn to_key(&self) -> (&str, &str);
// }

// impl Hash for dyn StringPairKey + '_ {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         self.to_key().hash(state)
//     }
// }

// impl PartialEq for dyn StringPairKey + '_ {
//     fn eq(&self, other: &Self) -> bool {
//         self.to_key() == other.to_key()
//     }
// }

// impl Eq for dyn StringPairKey + '_ {}

// impl StringPairKey for StringPair {
//     fn to_key(&self) -> (&str, &str) {
//         (&self.first, &self.second)
//     }
// }

// impl<'a> StringPairKey for (&'a str, &'a str) {
//     fn to_key(&self) -> (&str, &str) {
//         (self.0, self.1)
//     }
// }

// impl<'a> Borrow<dyn StringPairKey + 'a> for StringPair {
//     fn borrow(&self) -> &(dyn StringPairKey + 'a) {
//         self
//     }
// }
// impl<'a> Borrow<dyn StringPairKey + 'a> for (&'a str, &'a str) {
//     fn borrow(&self) -> &(dyn StringPairKey + 'a) {
//         self
//     }
// }
// End of code for [StringPair].

/// Enum to specify what kind of data a dictionary supports.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum DictionaryType {
    /// Dictionary for string datavalues
    StringDv,
    /// Dictionary for language-tagged string datavalues
    LangStringDv,
    /// Dictionary for IRI datavalues
    IriDv,
    /// Dictionary for other datavalues
    OtherDv,
    // /// Dictionary for long strings (blobs)
    // Blob,
    // /// Dictionary for strings with a fixed prefix and suffix
    // Infix { prefix: String, suffix: String },
    // /// Dictionary for numeric strings with a fixed prefix and suffix
    //NumInfix { prefix: String, suffix: String },
    // /// Dictionary for named (actually: "numbered") nulls
    // NULL,
}

impl DictionaryType {
    /// Returns true if the given value is supported by a dictionary of this type.
    fn supports(&self, dv: &AnyDataValue) -> bool {
        match (self,dv.value_domain()) {
            (DictionaryType::IriDv, ValueDomain::Iri) => true,
            (DictionaryType::StringDv, ValueDomain::String) => true,
            (DictionaryType::LangStringDv, ValueDomain::LanguageTaggedString) => true,
            (DictionaryType::OtherDv, ValueDomain::Other) => true,
            _ => false,
        }
    }
}

/// Struct to hold relevant information about a sub-dictionary.
#[derive(Debug)]
pub struct DictRecord {
    /// Pointer to the actual dictionary object
    dict: Box<dyn DvDict>,
    /// Type of the dictionary
    dict_type: DictionaryType,
    /// Vector to associate local address block numbers to global block numbers
    gblocks: Vec<usize>,
}

/// Iterator-like struct for cycling over suitable dictionaries for some datavalue.
/// This defines the preferred order in which dictionaries are considered in cases
/// where multiple sub-dictionaries could be used for a value.
/// 
/// TODO: This iterator might also become the place where we cache information about possible
/// prefix/infix/suffix splits for further processing.
struct DictIterator {
    /// Internal encoding of a "position" in a single value.
    /// 
    /// It is interpreted as follows: 0 is the initial state ("before" any value),
    /// 1 is the "fitting infix dictionary" (if any, only relevant for IRIs),
    /// 2 is the "default dictionary" (if any, only relevant for some types),
    /// numbers from 2 on refer to generic dictionaries in their order (i.e., the index is number-3). 
    position: usize,
}

impl DictIterator {
    /// Constructor.
    fn new() -> Self {
        DictIterator { position: 0 }
    }

    /// Advance iterator, and return the id of the next dictionary, or
    /// [`NO_DICT`] if no further matching dictionaries exist. 
    fn next(&mut self, dv: &AnyDataValue, md: &MetaDictionary) -> usize {
        // First look for infix dictionary:
        if self.position == 0 {
            self.position = 1; // move on, whatever happens
            // TODO: disabled, currently no infix dictionary support
            // if ds.infixable() {
            //     if let Some(dict_idx) = md
            //         .infix_dicts
            //         .get::<dyn StringPairKey>(&(ds.prefix(), ds.suffix()))
            //     {
            //         return *dict_idx;
            //     }
            // }
        }

        if self.position == 1 {
            self.position = 2; // move on, whatever happens
            match dv {
                AnyDataValue::String(_) => return md.string_dict,
                AnyDataValue::LanguageTaggedString(_) => return md.langstring_dict,
                AnyDataValue::Iri(_) => return md.iri_dict,
                AnyDataValue::Other(_) => return md.other_dict,
                _ => {}
            }
        }

        // Finally, interpret self.position-1 as an index in md.generic_dicts:
        while self.position + 2 < md.generic_dicts.len() {
            self.position += 1; // move on, whatever happens
            if md.dicts[md.generic_dicts[self.position - 3]]
                .dict_type
                .supports(dv)
            {
                return md.generic_dicts[self.position - 3];
            }
        }

        NO_DICT // No further dictionaries left
    }
}

type DictId = usize;
/// Constant to indicate the absence of a dictionary (an invalid dictionary ID)
const NO_DICT: DictId = usize::MAX;

/// A dictionary that combines several other dictionaries.
/// The design integrates specific optimized dictoinaries that support specific content types,
/// but it is also designed with the possibility of external read-only disctionaries in mind,
/// as they might occur when loading pre-indexed data from external sources.
#[derive(Debug)]
pub struct MetaDictionary {
    /// Vector to map global block numbers to pairs (sub-dictionary, local block number)
    dictblocks: Vec<(DictId, usize)>,
    /// Vector of all sub-dictionaries, indexed by their sub-dictionary number
    dicts: Vec<DictRecord>,

    /// If of the main string datavalue dictionary, if any (otherwise [NO_DICT])
    string_dict: DictId,
    /// If of the main language-tagged string datavalue dictionary, if any (otherwise [NO_DICT])
    langstring_dict: DictId,
    /// If of the main IRI datavalue dictionary, if any (otherwise [NO_DICT])
    iri_dict: DictId,
    /// If of the main other datavalue dictionary, if any (otherwise [NO_DICT])
    other_dict: DictId,
    /// Ids of further general-purpose dictionaries,
    /// which might be used for any kind of datavalue.
    generic_dicts: Vec<DictId>,
    // /// Auxiliary datastructure for finding fitting infix dictionaries, mapping
    // /// (prefix,suffix) pairs to dictionary ids.
    // infix_dicts: HashMap<StringPair, DictId>,
    // /// Data structure to hold counters for recently encountered dictionary types that we might
    // /// want to make a dictionary for.
    // dict_candidates: LruCache<StringPair, u32>,
    /// Keep track of total number of entries for faster checks
    size: usize,
}

impl Default for MetaDictionary {
    /// Initialise a [MetaDictionary].
    /// Sets up relevant default dictionaries for basic blocks.
    fn default() -> Self {
        let mut result = Self {
            dictblocks: Vec::new(),
            dicts: Vec::new(),
            string_dict: NO_DICT,
            langstring_dict: NO_DICT,
            iri_dict: NO_DICT,
            other_dict: NO_DICT,
            // dict_candidates: LruCache::new(NonZeroUsize::new(150).unwrap()),
            //infix_dicts: HashMap::new(),
            generic_dicts: Vec::new(),
            size: 0,
        };

        result.add_dictionary(DictionaryType::IriDv);
        result.add_dictionary(DictionaryType::StringDv);
        result.add_dictionary(DictionaryType::LangStringDv);
        result.add_dictionary(DictionaryType::OtherDv);

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
    fn local_to_global_unchecked(&self, dict: DictId, local_id: usize) -> usize {
        let lblock = local_id >> BLOCKSIZE;
        let offset = local_id % (1 << BLOCKSIZE);
        let gblock = self.dicts[dict].gblocks[lblock]; // Could fail if: (1) dictionary does not exist, or (2) block not used by dict

        (gblock << BLOCKSIZE) + offset
    }

    /// Convert the local ID of a given dictionary to a global ID.
    /// The function will check if the local id is supported by a previously
    /// reserved address block, and will reserve a new block for this
    /// dictionary otherwise. This is used when converting newly created ids.
    fn local_to_global(&mut self, dict: DictId, local_id: usize) -> usize {
        let lblock = local_id >> BLOCKSIZE;
        let offset = local_id % (1 << BLOCKSIZE);
        let gblock = self.allocate_block(dict, lblock);

        (gblock << BLOCKSIZE) + offset
    }

    /// Find a global block that is allocated for the given dictionary and local block. If not
    /// allocated yet, a new block is reserved for this purpose. Allocation tries to preserve relative
    /// order and distance, and to keep some distance from other dictionary's blocks.
    fn allocate_block(&mut self, dict: DictId, local_block: usize) -> usize {
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

            let mut new_block = if btl_index > 0 {
                // extrapolate where global block should be relative to last allocated local block
                self.dicts[dict].gblocks[btl_index - 1] + btl_index - 1
            } else {
                // TODO determine "good" initial block for this dictionary
                0
            };
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
    /// No blocks are allocated yet. Basic dictionaries are not created
    /// multiple times.
    fn add_dictionary(&mut self, dt: DictionaryType) {
        let dict: Box<dyn DvDict>;
        match dt {
            DictionaryType::StringDv => {
                if self.string_dict != NO_DICT { return; }
                dict = Box::new(StringDvDictionary::new());
                self.string_dict = self.dicts.len();
            }
            DictionaryType::LangStringDv => {
                if self.langstring_dict != NO_DICT { return; }
                dict = Box::new(LangStringDvDictionary::new());
                self.langstring_dict = self.dicts.len();
            }
            DictionaryType::IriDv => {
                if self.iri_dict != NO_DICT { return; }
                dict = Box::new(IriDvDictionary::new());
                self.iri_dict = self.dicts.len();
            }
            DictionaryType::OtherDv => {
                if self.other_dict != NO_DICT { return; }
                dict = Box::new(OtherDvDictionary::new());
                self.other_dict = self.dicts.len();
            }
            // DictionaryType::Infix {
            //     ref prefix,
            //     ref suffix,
            // } => {
            //     dict = Box::new(InfixDictionary::new(prefix.to_string(), suffix.to_string()));
            //     self.infix_dicts.insert(
            //         StringPair::new(prefix.to_string(), suffix.to_string()),
            //         self.dicts.len(),
            //     );
            // }
        }
        let dr = DictRecord {
            dict,
            dict_type: dt,
            gblocks: Vec::new(),
        };
        self.dicts.push(dr);
    }

    #[inline(always)]
    fn add_datavalue_inline(&mut self, dv: AnyDataValue) -> AddResult {
        let mut best_dict_idx = usize::MAX;

        // Look up new entry in all applicable dictionaries.
        let mut d_it = DictIterator::new();
        let mut dict_idx: usize;
        while {
            dict_idx = d_it.next(&dv, self);
            dict_idx
        } != usize::MAX
        {
            if best_dict_idx == usize::MAX {
                best_dict_idx = dict_idx;
            }
            if let Some(idx) = self.dicts[dict_idx]
                .dict
                .datavalue_to_id(&dv)
            {
                if idx != super::KNOWN_ID_MARK {
                    return AddResult::Known(self.local_to_global_unchecked(dict_idx, idx));
                } // else: marked, continue search for real id
            } else if self.dicts[dict_idx].dict.has_marked() {
                // neither found nor marked in marked dict -> give up search
                break;
            }
        }
        // Performance note: The remaining code is only executed once per unique string (i.e., typically much fewer times than the above).
        assert!(best_dict_idx < self.dicts.len());

        // Consider creating a new dictionary for the new entry:
        // if dv.infixable() {
        //     if let DictionaryType::String = self.dicts[best_dict_idx].dict_type {
        //         #[allow(trivial_casts)]
        //         if let Some(count) = self
        //             .dict_candidates
        //             .get_mut((dv.prefix(), dv.suffix()).borrow() as &dyn StringPairKey)
        //         {
        //             *count += 1;
        //             if *count > DICT_THRESHOLD {
        //                 // Performance note: The following code is very rarely executed.
        //                 self.dict_candidates.pop(&StringPair::new(
        //                     dv.prefix().to_string(),
        //                     dv.suffix().to_string(),
        //                 ));
        //                 best_dict_idx = self.dicts.len();
        //                 self.add_dictionary(DictionaryType::Infix {
        //                     prefix: dv.prefix().to_string(),
        //                     suffix: dv.suffix().to_string(),
        //                 });
        //                 log::info!(
        //                     "Initialized new infix dictionary (#{}) for '{}...{}'.",
        //                     best_dict_idx,
        //                     dv.prefix(),
        //                     dv.suffix()
        //                 );
        //                 // Mark previously added strings to enable one-shot misses when looking up elements:
        //                 if self.size < 50000 {
        //                     let mut i: usize = 0;
        //                     let mut c: usize = 0;
        //                     let min_len = dv.prefix().len() + dv.suffix().len(); // presumably lets us discard many strings more quickly
        //                     while let Some(string) = self.dicts[1].dict.get(i) {
        //                         i += 1;
        //                         if string.len() >= min_len
        //                             && string.starts_with(dv.prefix())
        //                             && string.ends_with(dv.suffix())
        //                         {
        //                             c += 1;
        //                             self.dicts[best_dict_idx].dict.mark_str(string.as_str());
        //                         }
        //                     }
        //                     log::info!("Marked {} older strings of that type, iterating {} strings overall.",c,i);
        //                 }
        //             }
        //         } else {
        //             self.dict_candidates.put(
        //                 StringPair::new(dv.prefix().to_string(), dv.suffix().to_string()),
        //                 1,
        //             );
        //         }
        //     }
        // }

        // Add entry to preferred dictionary
        self.size += 1;
        let local_id = self.dicts[best_dict_idx]
            .dict
            .add_datavalue(dv)
            .value();
        // Compute global id based on block and local id, possibly allocating new block in the process
        AddResult::Fresh(self.local_to_global(best_dict_idx, local_id))
    }
}

impl DvDict for MetaDictionary {
    fn add_datavalue(&mut self, dv: AnyDataValue) -> AddResult {
        self.add_datavalue_inline(dv)
    }

    fn datavalue_to_id(&self, dv: &AnyDataValue) -> Option<usize> {
        // Look up new entry in all applicable dictionaries.
        let mut d_it = DictIterator::new();
        let mut dict_idx: usize;
        while {
            dict_idx = d_it.next(&dv, self);
            dict_idx
        } != usize::MAX
        {
            if let Some(idx) = self.dicts[dict_idx].dict.datavalue_to_id(dv) {
                return Some(self.local_to_global_unchecked(dict_idx, idx));
            }
        }
        None
    }

    fn id_to_datavalue(&self, id: usize) -> Option<AnyDataValue> {
        let gblock = id >> BLOCKSIZE;
        let offset = id % (1 << BLOCKSIZE);
        if self.dictblocks.len() <= gblock || self.dictblocks[gblock] == (usize::MAX, usize::MAX) {
            return None;
        }
        let (dict_id, lblock) = self.dictblocks[gblock];

        self.dicts[dict_id].dict.id_to_datavalue((lblock >> BLOCKSIZE) + offset)
    }

    fn len(&self) -> usize {
        let mut len = 0;
        log::info!("Computing total meta dict length ...");
        for dr in self.dicts.iter() {
            log::info!("+ {} entries in dict {:?}", dr.dict.len(), dr.dict_type);
            len += dr.dict.len();
        }
        log::info!("Total len {}", len);
        len
    }

    fn mark_dv(&mut self, _dv: AnyDataValue) -> AddResult {
        AddResult::Rejected
    }

    fn has_marked(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::{datavalues::AnyDataValue, dictionary::{DvDict, AddResult}};

    use super::MetaDictionary;

    #[test]
    fn add_and_get() {
        let mut dict = MetaDictionary::new();

        let mut dvs = Vec::new();
        dvs.push(AnyDataValue::new_string("http://example.org".to_string()));
        dvs.push(AnyDataValue::new_string("another string".to_string()));
        dvs.push(AnyDataValue::new_iri("http://example.org".to_string()));
        dvs.push(AnyDataValue::new_language_tagged_string(
            "Hallo".to_string(),
            "de".to_string(),
        ));
        dvs.push(AnyDataValue::new_other(
            "abc".to_string(),
            "http://example.org/mydatatype".to_string(),
        ));

        let mut ids = Vec::new();
        for dv in &dvs {
            let AddResult::Fresh(dv_id) = dict.add_datavalue(dv.clone()) else { panic!("add failed") };
            ids.push(dv_id);
        }

        for i in 0..dvs.len() {
            assert_eq!(dict.datavalue_to_id(&dvs[i]), Some(ids[i]));
            assert_eq!(dict.add_datavalue(dvs[i].clone()), AddResult::Known(ids[i]));
            assert_eq!(dict.id_to_datavalue(ids[i]), Some(dvs[i].clone()));
        }

        assert_eq!(dict.len(), dvs.len());
    }
}