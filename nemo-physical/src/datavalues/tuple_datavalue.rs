//! This module provides implementations [`super::DataValue`]s that represent tuples of
//! data values. The tupes have a fixed length, which can also be zero.

use std::sync::Arc;

use super::{AnyDataValue, DataValue, ValueDomain};

/// Physical representation of a fixed-length tuple of [`DataValue`]s.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleDataValue {
    values: Arc<[AnyDataValue]>,
}

impl TupleDataValue {
    /// Constructor.
    pub fn new(values: Vec<AnyDataValue>) -> Self {
        values.into_iter().collect()
    }
}

impl FromIterator<AnyDataValue> for TupleDataValue {
    fn from_iter<T: IntoIterator<Item = AnyDataValue>>(iter: T) -> Self {
        Self {
            values: iter.into_iter().collect(),
        }
    }
}

impl DataValue for TupleDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        self.values
            .iter()
            .map(|v| DataValue::canonical_string(v))
            //.by_ref()
            .intersperse(",".to_string())
            .collect::<String>()
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Tuple
    }

    fn canonical_string(&self) -> String {
        super::datavalue::quote_string(self.lexical_value())
            + "^^"
            + &super::datavalue::quote_iri(self.datatype_iri().as_str())
    }

    fn tuple_element<'a>(&self, index: usize) -> Option<&(dyn DataValue + 'a)> {
        #[allow(trivial_casts)]
        self.values.get(index).map(|v| v as &dyn DataValue)
    }

    fn tuple_len_unchecked(&self) -> usize {
        self.values.len()
    }

    fn tuple_element_unchecked<'a>(&self, index: usize) -> &(dyn DataValue + 'a) {
        self.values.get(index).expect("this method is unchecked")
    }
}

#[cfg(test)]
mod test {
    use crate::datavalues::{AnyDataValue, DataValue, TupleDataValue};

    #[test]
    fn test_tuple() {
        let dv1 = AnyDataValue::new_integer_from_i64(42);
        let dv2 = AnyDataValue::new_string("test".to_string());
        let dv3 = AnyDataValue::new_boolean(true);

        let dv_tuple = TupleDataValue::new(vec![dv1.clone(), dv2.clone(), dv3.clone()]);

        assert_eq!(dv_tuple.tuple_len(), Some(3));
        assert_eq!(dv_tuple.tuple_len_unchecked(), 3);
        #[allow(trivial_casts)]
        let dv1_dyn = &dv1 as &dyn DataValue;
        assert_eq!(dv_tuple.tuple_element(0), Some(dv1_dyn));
        assert_eq!(dv_tuple.tuple_element_unchecked(0), dv1_dyn);
        #[allow(trivial_casts)]
        let dv2_dyn = &dv2 as &dyn DataValue;
        assert_eq!(dv_tuple.tuple_element(1), Some(dv2_dyn));
        assert_eq!(dv_tuple.tuple_element_unchecked(1), dv2_dyn);
        #[allow(trivial_casts)]
        let dv3_dyn = &dv3 as &dyn DataValue;
        assert_eq!(dv_tuple.tuple_element(2), Some(dv3_dyn));
        assert_eq!(dv_tuple.tuple_element_unchecked(2), dv3_dyn);
        assert_eq!(dv_tuple.tuple_element(3), None);

        assert_eq!(dv_tuple.datatype_iri(), "nemo:tuple".to_string());
        assert_eq!(
            dv_tuple.lexical_value(),
            dv1.canonical_string()
                + ","
                + dv2.canonical_string().as_str()
                + ","
                + dv3.canonical_string().as_str()
        );
    }

    #[test]
    fn test_empty_tuple() {
        let dv_tuple = TupleDataValue::new(vec![]);

        assert_eq!(dv_tuple.tuple_len(), Some(0));
        assert_eq!(dv_tuple.tuple_len_unchecked(), 0);
        assert_eq!(dv_tuple.tuple_element(0), None);
        assert_eq!(dv_tuple.lexical_value(), "".to_string());
        assert_eq!(
            dv_tuple.canonical_string(),
            "\"\"^^<nemo:tuple>".to_string()
        );
    }
}
