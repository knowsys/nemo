//! A [DvDict] implementation based on converting data values to strings.
//! The dictionary implementations in this module are typically restricted to certain
//! types of datavalues (e.g., strings or IRIs), which allows them to use more direct
//! string representations without any risk of confusion.

use super::{ranked_pair_dictionary::StringPairDictionary, AddResult, DvDict};
use crate::{
    datavalues::{AnyDataValue, ValueDomain},
    management::bytesized::ByteSized,
};
use std::{fmt::Debug, marker::PhantomData};

/// Trait to encapsulate (static) functions for converting datavalues to pairs of
/// strings and vice versa. The mapping must therefore be invertible, but otherwise it
/// can be arbitrary. Implementations may choose which datavalues to support.
pub(crate) trait DvPairConverter: Debug {
    /// Converts a datavalue to a string, if supported.
    fn dict_pair(dv: &AnyDataValue) -> Option<(String, String)>;
    /// Converts a pair of strings to a datavalue, if supported.
    fn pair_to_datavalue(frequent: &str, rare: &str) -> Option<AnyDataValue>;
    /// Each converter supports exactly one domain, returned by this function.
    fn supported_value_domain() -> ValueDomain;
}

/// A generic [DvDict] dictionary based on converting datavalues to strings. The
/// type parameter defines how the conversion is to be done, making sure that we have
/// compile-time knowledge about this.
#[derive(Debug)]
pub(crate) struct RankedPairBasedDvDictionary<C: DvPairConverter> {
    string_pair_dict: StringPairDictionary,
    _phantom: PhantomData<C>,
}

impl<C: DvPairConverter> RankedPairBasedDvDictionary<C> {
    /// Construct a new and empty dictionary.
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl<C: DvPairConverter> Default for RankedPairBasedDvDictionary<C> {
    fn default() -> Self {
        RankedPairBasedDvDictionary {
            string_pair_dict: StringPairDictionary::default(),
            _phantom: PhantomData,
        }
    }
}

impl<C: DvPairConverter> DvDict for RankedPairBasedDvDictionary<C> {
    fn add_datavalue(&mut self, dv: AnyDataValue) -> AddResult {
        if let Some((frequent, rare)) = C::dict_pair(&dv) {
            self.string_pair_dict
                .add_pair(frequent.as_str(), rare.as_str())
        } else {
            AddResult::Rejected
        }
    }

    fn fresh_null(&mut self) -> (AnyDataValue, usize) {
        panic!("string-pair dictionaries cannot make fresh nulls");
    }

    fn fresh_null_id(&mut self) -> usize {
        panic!("string-pair dictionaries cannot make fresh nulls");
    }

    fn datavalue_to_id(&self, dv: &AnyDataValue) -> Option<usize> {
        if let Some((frequent, rare)) = C::dict_pair(dv) {
            self.string_pair_dict
                .pair_to_id(frequent.as_str(), rare.as_str())
        } else {
            None
        }
    }

    fn id_to_datavalue(&self, id: usize) -> Option<AnyDataValue> {
        if let Some((frequent, rare)) = self.string_pair_dict.id_to_pair(id) {
            C::pair_to_datavalue(frequent.as_str(), rare.as_str())
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.string_pair_dict.len()
    }

    fn is_iri(&self, id: usize) -> bool {
        C::supported_value_domain() == ValueDomain::Iri && self.string_pair_dict.knows_id(id)
    }

    fn is_plain_string(&self, id: usize) -> bool {
        C::supported_value_domain() == ValueDomain::PlainString
            && self.string_pair_dict.knows_id(id)
    }

    fn is_lang_string(&self, id: usize) -> bool {
        C::supported_value_domain() == ValueDomain::LanguageTaggedString
            && self.string_pair_dict.knows_id(id)
    }

    fn is_null(&self, _id: usize) -> bool {
        false
    }

    fn mark_dv(&mut self, dv: AnyDataValue) -> AddResult {
        if let Some((frequent, rare)) = C::dict_pair(&dv) {
            self.string_pair_dict
                .mark_pair(frequent.as_str(), rare.as_str())
        } else {
            AddResult::Rejected
        }
    }

    fn has_marked(&self) -> bool {
        self.string_pair_dict.has_marked()
    }
}

impl<C: DvPairConverter> ByteSized for RankedPairBasedDvDictionary<C> {
    fn size_bytes(&self) -> u64 {
        // Code for debugging/profiling dictionary
        // let result = self.string_pair_dict.size_bytes();
        // println!(
        //     "Ranked Pair DV Dict for {:?}: size {}",
        //     C::supported_value_domain(),
        //     result
        // );
        // result
        self.string_pair_dict.size_bytes()
    }
}
