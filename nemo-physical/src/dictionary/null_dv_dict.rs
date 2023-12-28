//! A [`DvDict`] implementation for nulls (and nulls only).

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

#[cfg(test)]
mod test {
    use crate::{
        datavalues::{AnyDataValue, NullDataValue},
        dictionary::{AddResult, DvDict, NullDvDictionary},
    };

    #[test]
    fn add_and_get_nulls() {
        let mut dict = NullDvDictionary::new();

        let n1_id = dict.fresh_null_id();
        let nv1 = dict.id_to_datavalue(n1_id).unwrap();
        let (nv2, n2_id) = dict.fresh_null();
        let nv3 = AnyDataValue::Null(NullDataValue::new(42));
        let dv = AnyDataValue::new_integer_from_i64(42);

        assert_ne!(nv1, nv2);
        assert_ne!(n1_id, n2_id);

        assert_eq!(dict.datavalue_to_id(&nv1), Some(n1_id));
        assert_eq!(dict.datavalue_to_id(&nv2), Some(n2_id));
        assert_eq!(dict.add_datavalue(nv1.clone()), AddResult::Known(n1_id));
        assert_eq!(dict.add_datavalue(nv2.clone()), AddResult::Known(n2_id));

        assert_eq!(dict.add_datavalue(nv3), AddResult::Rejected);
        assert_eq!(dict.add_datavalue(dv), AddResult::Rejected);

        assert_eq!(dict.len(), 2);
    }
}
