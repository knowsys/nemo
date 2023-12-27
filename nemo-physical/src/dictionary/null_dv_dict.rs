//! A [`DvDict`] implementation for nulls (and nulls only).

use hashbrown::hash_table::AbsentEntry;

use crate::datavalues::{AnyDataValue, NullDataValue};

use super::{AddResult, DvDict};

/// A [`DvDict`] dictionary for datavalues that are nulls.
///
/// No datavalues (nulls or otherwise) can be directly added to the dictionary,
/// but it can generate new nulls, and retrieve the id of such nulls later on.
/// The dictionary does not support marking of values (whether nulls
/// or not).
#[derive(Clone, Debug)]
pub(crate) struct NullDvDictionary {
    unused_ids: std::ops::RangeFrom<usize>,
}

impl NullDvDictionary {
    /// Construct a new and empty dictionary.
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl Default for NullDvDictionary {
    fn default() -> Self {
        NullDvDictionary { unused_ids: 0.. }
    }
}

impl DvDict for NullDvDictionary {
    fn add_datavalue(&mut self, dv: AnyDataValue) -> AddResult {
        if let AnyDataValue::Null(nv) = dv {
            if nv.id() < self.unused_ids.start {
                AddResult::Known(nv.id())
            } else {
                AddResult::Rejected
            }
        } else {
            AddResult::Rejected
        }
    }

    fn fresh_null(&mut self) -> (AnyDataValue, usize) {
        let nv = NullDataValue::new(self.fresh_null_id());
        (AnyDataValue::Null(nv), nv.id())
    }

    fn fresh_null_id(&mut self) -> usize {
        self.unused_ids.next().unwrap()
    }

    fn datavalue_to_id(&self, dv: &AnyDataValue) -> Option<usize> {
        if let AnyDataValue::Null(nv) = dv {
            if nv.id() < self.unused_ids.start {
                Some(nv.id())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn id_to_datavalue(&self, id: usize) -> Option<AnyDataValue> {
        if id < self.unused_ids.start {
            Some(AnyDataValue::Null(NullDataValue::new(id)))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.unused_ids.start
    }

    fn mark_dv(&mut self, _dv: AnyDataValue) -> AddResult {
        AddResult::Rejected
    }

    fn has_marked(&self) -> bool {
        false
    }
}
