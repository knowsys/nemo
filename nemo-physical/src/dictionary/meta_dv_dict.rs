//! This module defines [MetaDvDictionary].

use crate::datavalues::ValueDomain;
use crate::datavalues::{AnyDataValue, DataValue};
use crate::dictionary::NONEXISTING_ID_MARK;
use crate::management::bytesized::{size_inner_vec_flat, ByteSized};

use super::ranked_pair_iri_dv_dict::IriSplittingDvDictionary;
use super::ranked_pair_other_dv_dict::OtherSplittingDvDictionary;
use super::string_langstring_dv_dict::LangStringDvDictionary;
use super::tuple_dv_dict::TupleDvDict;
use super::DvDict;
use super::StringDvDictionary;
use super::{AddResult, NullDvDictionary};

// /// Number of recent occurrences of a string pattern required for creating a bespoke dictionary
// const DICT_THRESHOLD: u32 = 500;

/// Bits in the size of address blocks allocated to sub-dictionaries.
/// For example 24bit blocks each contain 2^24 addresses, and there are
/// 2^8=256 such blocks available within the u32 address range (and
/// 2^40 in 64bits).
const BLOCKSIZE: u32 = 24;

/// Enum to specify what kind of data a dictionary supports.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum DictionaryType {
    /// Dictionary for string datavalues
    String,
    /// Dictionary for language-tagged string datavalues
    LangString,
    /// Dictionary for IRI datavalues
    Iri,
    /// Dictionary for other datavalues
    Other,
    /// Dictionary for null datavalues
    Null,
    /// Dictionary for (variable-free) tuples
    Tuple,
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
        matches!(
            (self, dv.value_domain()),
            (DictionaryType::Iri, ValueDomain::Iri)
                | (DictionaryType::String, ValueDomain::PlainString)
                | (
                    DictionaryType::LangString,
                    ValueDomain::LanguageTaggedString
                )
                | (DictionaryType::Other, ValueDomain::Other)
                | (DictionaryType::Null, ValueDomain::Null)
        )
    }
}

/// Struct to hold relevant information about a sub-dictionary.
#[derive(Debug)]
pub(crate) struct DictRecord {
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
    /// [NO_DICT] if no further matching dictionaries exist.
    fn next(&mut self, dv: &AnyDataValue, md: &MetaDvDictionary) -> usize {
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
            match dv.value_domain() {
                ValueDomain::PlainString => return md.string_dict,
                ValueDomain::LanguageTaggedString => return md.langstring_dict,
                ValueDomain::Iri => return md.iri_dict,
                ValueDomain::Other => return md.other_dict,
                ValueDomain::Null => return md.null_dict,
                ValueDomain::Tuple => return md.tuple_dict,
                ValueDomain::Boolean => return md.other_dict, // TODO: maybe not the best place, using a whole page for two values if there is not much "other"
                ValueDomain::UnsignedLong => return md.other_dict, // TODO: maybe not the best place either
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
///
/// The design integrates specific optimized dictionaries that support specific content types,
/// but it is also designed with the possibility of external read-only dictionaries in mind,
/// as they might occur when loading pre-indexed data from external sources.
#[derive(Debug)]
pub struct MetaDvDictionary {
    /// Vector to map global block numbers to pairs (sub-dictionary, local block number)
    dictblocks: Vec<(DictId, usize)>,
    /// Vector of all sub-dictionaries, indexed by their sub-dictionary number
    dicts: Vec<DictRecord>,

    /// Id of the main string datavalue dictionary, if any (otherwise [NO_DICT])
    string_dict: DictId,
    /// Id of the main language-tagged string datavalue dictionary, if any (otherwise [NO_DICT])
    langstring_dict: DictId,
    /// Id of the main IRI datavalue dictionary, if any (otherwise [NO_DICT])
    iri_dict: DictId,
    /// Id of the main other datavalue dictionary, if any (otherwise [NO_DICT])
    other_dict: DictId,
    /// Id of the null dictionary, if any (otherwise [NO_DICT])
    null_dict: DictId,
    /// Id of the tuple dictionary, if any (otherwise [NO_DICT])
    tuple_dict: DictId,
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

impl Default for MetaDvDictionary {
    /// Initialise a [MetaDvDictionary].
    /// Sets up relevant default dictionaries for basic blocks.
    fn default() -> Self {
        let mut result = Self {
            dictblocks: Vec::new(),
            dicts: Vec::new(),
            string_dict: NO_DICT,
            langstring_dict: NO_DICT,
            iri_dict: NO_DICT,
            other_dict: NO_DICT,
            null_dict: NO_DICT,
            tuple_dict: NO_DICT,
            // dict_candidates: LruCache::new(NonZeroUsize::new(150).unwrap()),
            //infix_dicts: HashMap::new(),
            generic_dicts: Vec::new(),
            size: 0,
        };

        result.add_dictionary(DictionaryType::Iri);
        result.add_dictionary(DictionaryType::String);
        result.add_dictionary(DictionaryType::LangString);
        result.add_dictionary(DictionaryType::Other);
        result.add_dictionary(DictionaryType::Null);
        result.add_dictionary(DictionaryType::Tuple);

        result
    }
}

impl MetaDvDictionary {
    /// Construct a new and empty dictionary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a pointer to the [DvDict] that is identified by the given id. This function
    /// must only be called with ids that are given out by [MetaDvDictionary], and will panic
    /// if invalid IDs are used.
    pub(crate) fn sub_dictionary_mut_unchecked(&mut self, dict_id: usize) -> &mut Box<dyn DvDict> {
        &mut self.dicts[dict_id].dict
    }

    /// Returns a pointer to the [DvDict] that is identified by the given id. This function
    /// must only be called with ids that are given out by [MetaDvDictionary], and will panic
    /// if invalid IDs are used.
    pub(crate) fn sub_dictionary_unchecked(&self, dict_id: usize) -> &dyn DvDict {
        &*self.dicts[dict_id].dict
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

    /// Find the dictionary ID and local ID for a given global id. The function
    /// returns ([NO_DICT],0) if the global id is not in any dictionary.
    fn global_to_local(&self, global_id: usize) -> (usize, usize) {
        let gblock = global_id >> BLOCKSIZE;
        let offset = global_id % (1 << BLOCKSIZE);
        if self.dictblocks.len() <= gblock || self.dictblocks[gblock] == (usize::MAX, usize::MAX) {
            return (NO_DICT, 0);
        }
        let (dict_id, lblock) = self.dictblocks[gblock];
        (dict_id, (lblock << BLOCKSIZE) + offset)
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
            DictionaryType::String => {
                if self.string_dict != NO_DICT {
                    return;
                }
                dict = Box::new(StringDvDictionary::new());
                self.string_dict = self.dicts.len();
            }
            DictionaryType::LangString => {
                if self.langstring_dict != NO_DICT {
                    return;
                }
                dict = Box::new(LangStringDvDictionary::new());
                //dict = Box::new(LangStringSplittingDvDictionary::new());
                self.langstring_dict = self.dicts.len();
            }
            DictionaryType::Iri => {
                if self.iri_dict != NO_DICT {
                    return;
                }
                //dict = Box::new(IriDvDictionary::new());
                dict = Box::new(IriSplittingDvDictionary::new());
                self.iri_dict = self.dicts.len();
            }
            DictionaryType::Other => {
                if self.other_dict != NO_DICT {
                    return;
                }
                //dict = Box::new(OtherDvDictionary::new());
                dict = Box::new(OtherSplittingDvDictionary::new());
                self.other_dict = self.dicts.len();
            }
            DictionaryType::Null => {
                if self.null_dict != NO_DICT {
                    return;
                }
                dict = Box::new(NullDvDictionary::new());
                self.null_dict = self.dicts.len();
            }
            DictionaryType::Tuple => {
                if self.tuple_dict != NO_DICT {
                    return;
                }
                dict = Box::new(TupleDvDict::new());
                self.tuple_dict = self.dicts.len();
            } // DictionaryType::Infix {
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
            let dti_fn = self.dicts[dict_idx].dict.datavalue_to_id_with_parent_fn();
            if let Some(idx) = dti_fn(self, dict_idx, &dv) {
                if idx != super::KNOWN_ID_MARK {
                    return AddResult::Known(self.local_to_global_unchecked(dict_idx, idx));
                } // else: marked, continue search for real id
            } else if self.dicts[dict_idx].dict.has_marked() {
                // neither found nor marked in marked dict -> give up search
                break;
            }
        }
        // Performance note: The remaining code is only executed once per unique string (i.e., typically much fewer times than the above).

        // No dictionary was found for this type of value. Maybe a number, which we never make dictionary entries for.
        if best_dict_idx >= self.dicts.len() {
            return AddResult::Rejected;
        }

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
        let add_fn = self.dicts[best_dict_idx]
            .dict
            .add_datavalue_with_parent_fn();
        let local_id = add_fn(self, best_dict_idx, dv).value();
        // let local_id = self.dicts[best_dict_idx].dict.add_datavalue(dv).value();
        if local_id != NONEXISTING_ID_MARK {
            // Compute global id based on block and local id, possibly allocating new block in the process
            AddResult::Fresh(self.local_to_global(best_dict_idx, local_id))
        } else {
            AddResult::Rejected
        }
    }
}

impl DvDict for MetaDvDictionary {
    fn add_datavalue(&mut self, dv: AnyDataValue) -> AddResult {
        self.add_datavalue_inline(dv)
    }

    fn fresh_null(&mut self) -> (AnyDataValue, usize) {
        let (nv, local_id) = self.dicts[self.null_dict].dict.fresh_null();
        (nv, self.local_to_global(self.null_dict, local_id))
    }

    fn fresh_null_id(&mut self) -> usize {
        let local_id = self.dicts[self.null_dict].dict.fresh_null_id();
        self.local_to_global(self.null_dict, local_id)
    }

    fn datavalue_to_id(&self, dv: &AnyDataValue) -> Option<usize> {
        // Look up new entry in all applicable dictionaries.
        let mut d_it = DictIterator::new();
        let mut dict_idx: usize;
        while {
            dict_idx = d_it.next(dv, self);
            dict_idx
        } != usize::MAX
        {
            let dti_fn = self.dicts[dict_idx].dict.datavalue_to_id_with_parent_fn();
            if let Some(idx) = dti_fn(self, dict_idx, dv) {
                return Some(self.local_to_global_unchecked(dict_idx, idx));
            }
        }
        None
    }

    fn id_to_datavalue(&self, id: usize) -> Option<AnyDataValue> {
        let (dict_id, local_id) = self.global_to_local(id);
        if dict_id == NO_DICT {
            None
        } else {
            let itd_fn = self.dicts[dict_id].dict.id_to_datavalue_with_parent_fn();
            itd_fn(self, dict_id, local_id)
        }
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

    fn is_iri(&self, id: usize) -> bool {
        let (dict_id, local_id) = self.global_to_local(id);
        if dict_id == NO_DICT {
            false
        } else {
            self.dicts[dict_id].dict.is_iri(local_id)
        }
    }

    fn is_plain_string(&self, id: usize) -> bool {
        let (dict_id, local_id) = self.global_to_local(id);
        if dict_id == NO_DICT {
            false
        } else {
            self.dicts[dict_id].dict.is_plain_string(local_id)
        }
    }

    fn is_lang_string(&self, id: usize) -> bool {
        let (dict_id, local_id) = self.global_to_local(id);
        if dict_id == NO_DICT {
            false
        } else {
            self.dicts[dict_id].dict.is_lang_string(local_id)
        }
    }

    fn is_null(&self, id: usize) -> bool {
        let (dict_id, local_id) = self.global_to_local(id);
        if dict_id == NO_DICT {
            false
        } else {
            self.dicts[dict_id].dict.is_null(local_id)
        }
    }

    fn mark_dv(&mut self, _dv: AnyDataValue) -> AddResult {
        AddResult::Rejected
    }

    fn has_marked(&self) -> bool {
        false
    }
}

impl ByteSized for MetaDvDictionary {
    fn size_bytes(&self) -> u64 {
        size_of::<Self>() as u64
            + size_inner_vec_flat(&self.dictblocks)
            + size_inner_vec_flat(&self.dicts)
            + size_inner_vec_flat(&self.generic_dicts)
            + self
                .dicts
                .iter()
                .map(|dr| dr.dict.size_bytes() + size_inner_vec_flat(&dr.gblocks))
                .sum::<u64>()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        datavalues::{
            syntax::XSD_PREFIX, AnyDataValue, IriDataValue, NullDataValue, TupleDataValue,
        },
        dictionary::{AddResult, DvDict},
    };

    use super::MetaDvDictionary;

    #[test]
    fn add_and_get() {
        let mut dict = MetaDvDictionary::new();

        let dvs = vec![
            AnyDataValue::new_plain_string("http://example.org".to_string()),
            AnyDataValue::new_plain_string("another string".to_string()),
            AnyDataValue::new_iri("http://example.org".to_string()),
            AnyDataValue::new_language_tagged_string("Hallo".to_string(), "de".to_string()),
            AnyDataValue::new_other(
                "abc".to_string(),
                "http://example.org/mydatatype".to_string(),
            ),
        ];

        let mut ids = Vec::new();
        for dv in &dvs {
            let AddResult::Fresh(dv_id) = dict.add_datavalue(dv.clone()) else {
                panic!("add failed")
            };
            ids.push(dv_id);
        }

        for i in 0..dvs.len() {
            assert_eq!(dict.datavalue_to_id(&dvs[i]), Some(ids[i]));
            assert_eq!(dict.add_datavalue(dvs[i].clone()), AddResult::Known(ids[i]));
            assert_eq!(dict.id_to_datavalue(ids[i]), Some(dvs[i].clone()));
        }

        assert!(!dict.is_iri(ids[0]));
        assert!(!dict.is_iri(ids[1]));
        assert!(dict.is_iri(ids[2]));
        assert!(!dict.is_iri(ids[3]));
        assert!(!dict.is_iri(ids[3]));
        assert!(!dict.is_iri(ids[4] * 500));

        assert!(dict.is_plain_string(ids[0]));
        assert!(dict.is_plain_string(ids[1]));
        assert!(!dict.is_plain_string(ids[2]));
        assert!(!dict.is_plain_string(ids[3]));
        assert!(!dict.is_plain_string(ids[4]));
        assert!(!dict.is_plain_string(ids[4] * 500));

        assert!(!dict.is_lang_string(ids[0]));
        assert!(!dict.is_lang_string(ids[1]));
        assert!(!dict.is_lang_string(ids[2]));
        assert!(dict.is_lang_string(ids[3]));
        assert!(!dict.is_lang_string(ids[4]));
        assert!(!dict.is_lang_string(ids[4] * 500));

        assert!(!dict.is_null(ids[0]));
        assert!(!dict.is_null(ids[1]));
        assert!(!dict.is_null(ids[2]));
        assert!(!dict.is_null(ids[3]));
        assert!(!dict.is_null(ids[4]));
        assert!(!dict.is_null(ids[4] * 500));

        assert_eq!(dict.len(), dvs.len());
    }

    #[test]
    fn add_and_get_nulls() {
        let mut dict = MetaDvDictionary::new();

        let n1_id = dict.fresh_null_id();
        let nv1 = dict.id_to_datavalue(n1_id).unwrap();
        let (nv2, n2_id) = dict.fresh_null();
        let nv3: AnyDataValue = NullDataValue::new(42).into();

        assert_eq!(dict.datavalue_to_id(&nv1), Some(n1_id));
        assert_eq!(dict.datavalue_to_id(&nv2), Some(n2_id));
        assert_eq!(dict.add_datavalue(nv1.clone()), AddResult::Known(n1_id));
        assert_eq!(dict.add_datavalue(nv2.clone()), AddResult::Known(n2_id));

        assert_eq!(dict.add_datavalue(nv3.clone()), AddResult::Rejected);

        assert!(dict.is_null(n1_id));
        assert!(dict.is_null(n2_id));
        assert!(!dict.is_null(n2_id + 10));

        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn add_unsupported_dv() {
        let mut dict = MetaDvDictionary::new();
        let dv = AnyDataValue::new_integer_from_i64(42);

        assert_eq!(dict.add_datavalue(dv), AddResult::Rejected);
    }

    #[test]
    fn add_unsigned_long() {
        let mut dict = MetaDvDictionary::new();
        let dv = AnyDataValue::new_from_typed_literal(
            "+13000000000000000000.0".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        )
        .expect("Failed to create unsigned long");

        assert_eq!(dict.add_datavalue(dv), AddResult::Fresh(0));
    }

    #[test]
    fn add_and_get_tuple() {
        let mut dict = MetaDvDictionary::new();

        let dv_label = IriDataValue::new("f".to_string());
        let null_id = dict.fresh_null_id();
        let dv_null = dict.id_to_datavalue(null_id).unwrap();
        let dvs = vec![
            AnyDataValue::new_plain_string("http://example.org".to_string()),
            AnyDataValue::new_integer_from_i64(42),
            AnyDataValue::new_integer_from_u64(u64::MAX - 3),
            AnyDataValue::new_plain_string("another string".to_string()),
            AnyDataValue::new_boolean(true),
            dv_null,
            AnyDataValue::new_float_from_f32(3.9914).expect("must work"),
            AnyDataValue::new_double_from_f64(1.2345).expect("must work"),
            AnyDataValue::new_iri("http://example.org".to_string()),
            AnyDataValue::new_language_tagged_string("Hallo".to_string(), "de".to_string()),
            AnyDataValue::new_other(
                "abc".to_string(),
                "http://example.org/mydatatype".to_string(),
            ),
        ];

        let dv_tuple: AnyDataValue = TupleDataValue::new(Some(dv_label), dvs).into();

        let ar = dict.add_datavalue(dv_tuple.clone());
        let AddResult::Fresh(dv_tuple_id) = ar else {
            panic!("add failed: {:?}", ar);
        };

        assert_eq!(dict.datavalue_to_id(&dv_tuple), Some(dv_tuple_id));
        assert_eq!(
            dict.add_datavalue(dv_tuple.clone()),
            AddResult::Known(dv_tuple_id)
        );
        assert_eq!(dict.id_to_datavalue(dv_tuple_id), Some(dv_tuple.clone()));
    }

    #[test]
    fn add_and_get_tuple_nolabel() {
        let mut dict = MetaDvDictionary::new();

        let dvs = vec![
            AnyDataValue::new_integer_from_i64(42),
            AnyDataValue::new_integer_from_i64(43),
            AnyDataValue::new_integer_from_i64(44),
        ];

        let dv_tuple: AnyDataValue = TupleDataValue::new(None, dvs).into();

        let ar = dict.add_datavalue(dv_tuple.clone());
        let AddResult::Fresh(dv_tuple_id) = ar else {
            panic!("add failed: {:?}", ar);
        };

        assert_eq!(dict.datavalue_to_id(&dv_tuple), Some(dv_tuple_id));
        assert_eq!(
            dict.add_datavalue(dv_tuple.clone()),
            AddResult::Known(dv_tuple_id)
        );
        assert_eq!(dict.id_to_datavalue(dv_tuple_id), Some(dv_tuple.clone()));
    }

    #[test]
    fn add_and_get_tuple_empty() {
        let mut dict = MetaDvDictionary::new();

        let dvs = vec![];

        let dv_tuple: AnyDataValue = TupleDataValue::new(None, dvs).into();

        let ar = dict.add_datavalue(dv_tuple.clone());
        let AddResult::Fresh(dv_tuple_id) = ar else {
            panic!("add failed: {:?}", ar);
        };

        assert_eq!(dict.datavalue_to_id(&dv_tuple), Some(dv_tuple_id));
        assert_eq!(
            dict.add_datavalue(dv_tuple.clone()),
            AddResult::Known(dv_tuple_id)
        );
        assert_eq!(dict.id_to_datavalue(dv_tuple_id), Some(dv_tuple.clone()));
    }

    #[test]
    fn add_and_get_tuple_nested() {
        let mut dict = MetaDvDictionary::new();

        let dvs1 = vec![
            AnyDataValue::new_integer_from_i64(42),
            AnyDataValue::new_integer_from_i64(43),
            AnyDataValue::new_integer_from_i64(44),
        ];
        let dv_tuple1: AnyDataValue = TupleDataValue::new(None, dvs1).into();

        let dvs2 = vec![
            dv_tuple1.clone(),
            AnyDataValue::new_integer_from_i64(4),
            dv_tuple1.clone(),
            dv_tuple1.clone(),
        ];
        let dv_tuple2: AnyDataValue = TupleDataValue::new(None, dvs2).into();

        let ar = dict.add_datavalue(dv_tuple2.clone());
        let AddResult::Fresh(dv_tuple_id) = ar else {
            panic!("add failed: {:?}", ar);
        };

        assert_eq!(dict.datavalue_to_id(&dv_tuple2), Some(dv_tuple_id));
        assert_eq!(
            dict.add_datavalue(dv_tuple2.clone()),
            AddResult::Known(dv_tuple_id)
        );
        assert_eq!(dict.id_to_datavalue(dv_tuple_id), Some(dv_tuple2.clone()));
    }
}
