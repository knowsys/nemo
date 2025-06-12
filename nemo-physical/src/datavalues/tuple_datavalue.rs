//! This module provides implementations [DataValue]s that represent tuples of
//! data values. The tupes have a fixed length, which can also be zero.

use std::sync::Arc;

use super::{syntax::tuple, AnyDataValue, DataValue, IriDataValue, ValueDomain};

/// Physical representation of a fixed-length tuple of [DataValue]s.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TupleDataValue {
    label: Option<IriDataValue>,
    values: Arc<[AnyDataValue]>,
}

impl TupleDataValue {
    /// Constructor.
    #[allow(dead_code)]
    pub fn new<T: IntoIterator<Item = AnyDataValue>>(
        label: Option<IriDataValue>,
        values: T,
    ) -> Self {
        Self {
            label,
            values: values.into_iter().collect(),
        }
    }
}

impl FromIterator<AnyDataValue> for TupleDataValue {
    fn from_iter<T: IntoIterator<Item = AnyDataValue>>(iter: T) -> Self {
        Self {
            label: None,
            values: iter.into_iter().collect(),
        }
    }
}

impl DataValue for TupleDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        let values = self
            .values
            .iter()
            .map(DataValue::canonical_string)
            //.by_ref()
            .intersperse(tuple::SEPARATOR.to_string())
            .collect::<String>();

        if let Some(iri) = self.label() {
            iri.canonical_string() + tuple::OPEN + values.as_str() + tuple::CLOSE
        } else {
            tuple::OPEN.to_string() + values.as_str() + tuple::CLOSE
        }
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Tuple
    }

    fn canonical_string(&self) -> String {
        super::datavalue::quote_string(self.lexical_value().as_str())
            + "^^"
            + &super::datavalue::quote_iri(self.datatype_iri().as_str())
    }

    fn tuple_element(&self, index: usize) -> Option<&AnyDataValue> {
        self.values.get(index)
    }

    fn label(&self) -> Option<&IriDataValue> {
        self.label.as_ref()
    }

    fn len_unchecked(&self) -> usize {
        self.values.len()
    }

    fn tuple_element_unchecked(&self, index: usize) -> &AnyDataValue {
        self.values
            .get(index)
            .expect("unchecked access to tuple element requires index to be valid")
    }
}

impl std::hash::Hash for TupleDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.values.hash(state);
    }
}

impl std::fmt::Display for TupleDataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(iri) = self.label() {
            iri.fmt(f)?;
        }
        f.write_str(tuple::OPEN)?;
        let mut first = true;
        for v in self.values.iter() {
            if first {
                first = false;
            } else {
                f.write_str(tuple::SEPARATOR)?;
            }
            v.fmt(f)?;
        }
        f.write_str(tuple::CLOSE)
    }
}

#[cfg(test)]
mod test {
    use crate::datavalues::{AnyDataValue, DataValue, IriDataValue, TupleDataValue, ValueDomain};

    #[test]
    fn test_tuple() {
        let dv1 = AnyDataValue::new_integer_from_i64(42);
        let dv2 = AnyDataValue::new_plain_string("test".to_string());
        let dv3 = AnyDataValue::new_boolean(true);
        let label = IriDataValue::new("http://example.org/label".to_string());

        let dv_tuple = TupleDataValue::new(
            Some(label.clone()),
            vec![dv1.clone(), dv2.clone(), dv3.clone()],
        );

        assert_eq!(dv_tuple.label(), Some(&label));
        assert_eq!(dv_tuple.length(), Some(3));
        assert_eq!(dv_tuple.len_unchecked(), 3);
        assert_eq!(dv_tuple.tuple_element(0), Some(&dv1));
        assert_eq!(dv_tuple.tuple_element_unchecked(0), &dv1);
        assert_eq!(dv_tuple.tuple_element(1), Some(&dv2));
        assert_eq!(dv_tuple.tuple_element_unchecked(1), &dv2);
        assert_eq!(dv_tuple.tuple_element(2), Some(&dv3));
        assert_eq!(dv_tuple.tuple_element_unchecked(2), &dv3);
        assert_eq!(dv_tuple.tuple_element(3), None);

        assert_eq!(dv_tuple.value_domain(), ValueDomain::Tuple);
        assert_eq!(dv_tuple.datatype_iri(), "nemo:tuple".to_string());
        assert_eq!(
            dv_tuple.lexical_value(),
            label.canonical_string()
                + "("
                + dv1.canonical_string().as_str()
                + ","
                + dv2.canonical_string().as_str()
                + ","
                + dv3.canonical_string().as_str()
                + ")"
        );
    }

    #[test]
    fn test_tuple_eq() {
        let dv1 = AnyDataValue::new_integer_from_i64(42);
        let dv2 = AnyDataValue::new_plain_string("test".to_string());
        let dv3 = AnyDataValue::new_boolean(true);
        let label1 = IriDataValue::new("http://example.org/label1".to_string());
        let label2 = IriDataValue::new("http://example.org/label2".to_string());

        let dv_tuple1 = TupleDataValue::new(
            Some(label1.clone()),
            vec![dv1.clone(), dv2.clone(), dv3.clone()],
        );
        let dv_tuple2 = TupleDataValue::new(
            Some(label1.clone()),
            vec![dv1.clone(), dv2.clone(), dv3.clone()],
        );
        let dv_tuple3 = TupleDataValue::new(
            Some(label1.clone()),
            vec![dv1.clone(), dv2.clone(), dv3.clone(), dv3.clone()],
        );
        let dv_tuple4 =
            TupleDataValue::new(Some(label2), vec![dv1.clone(), dv2.clone(), dv3.clone()]);

        assert_eq!(dv_tuple1, dv_tuple2);
        assert_ne!(dv_tuple1, dv_tuple3);
        assert_ne!(dv_tuple1, dv_tuple4);
    }

    #[test]
    fn test_empty_tuple() {
        let dv_tuple = TupleDataValue::new(None, vec![]);

        assert_eq!(dv_tuple.length(), Some(0));
        assert_eq!(dv_tuple.len_unchecked(), 0);
        assert_eq!(dv_tuple.tuple_element(0), None);
        assert_eq!(dv_tuple.lexical_value(), "()".to_string());
        assert_eq!(
            dv_tuple.canonical_string(),
            "\"()\"^^<nemo:tuple>".to_string()
        );
    }
}
