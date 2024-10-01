//! This module provides implementations [DataValue]s that represent unordered maps of
//! data values to data values. The maps can be empty.

use std::collections::BTreeMap;

use super::{syntax::map, AnyDataValue, DataValue, IriDataValue, ValueDomain};

/// Physical representation of a finite map on [DataValue]s.
///
/// Maps in Nemo are unordered, and maps with the same key-value pairs
/// provided (upn creation) in different order will be equal and behave
/// indistinguishably.
///
/// In particular, all string representations provided by the implementation
/// are canonical, i.e., equal maps will lead to equal string serializations
/// that list keys according to their natural order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MapDataValue {
    label: Option<IriDataValue>,
    pairs: BTreeMap<AnyDataValue, AnyDataValue>,
}

impl MapDataValue {
    /// Constructor.
    #[allow(dead_code)]
    pub(crate) fn new<T: IntoIterator<Item = (AnyDataValue, AnyDataValue)>>(
        label: Option<IriDataValue>,
        pairs_iter: T,
    ) -> Self {
        Self {
            label,
            pairs: pairs_iter.into_iter().collect(),
        }
    }
}

impl FromIterator<(AnyDataValue, AnyDataValue)> for MapDataValue {
    fn from_iter<T: IntoIterator<Item = (AnyDataValue, AnyDataValue)>>(iter: T) -> Self {
        Self {
            label: None,
            pairs: iter.into_iter().collect(),
        }
    }
}

impl DataValue for MapDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        let pairs = self
            .pairs
            .iter()
            .map(|v| {
                DataValue::canonical_string(v.0)
                    + map::KEY_VALUE_ASSIGN
                    + DataValue::canonical_string(v.1).as_str()
            })
            .intersperse(map::SEPARATOR.to_string())
            .collect::<String>();

        if let Some(iri) = self.label() {
            iri.canonical_string() + map::OPEN + pairs.as_str() + map::CLOSE
        } else {
            map::OPEN.to_string() + pairs.as_str() + map::CLOSE
        }
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Map
    }

    fn canonical_string(&self) -> String {
        super::datavalue::quote_string(self.lexical_value().as_str())
            + "^^"
            + &super::datavalue::quote_iri(self.datatype_iri().as_str())
    }

    fn label(&self) -> Option<&IriDataValue> {
        self.label.as_ref()
    }

    fn len_unchecked(&self) -> usize {
        self.pairs.len()
    }

    fn map_keys(&self) -> Option<Box<dyn Iterator<Item = &AnyDataValue> + '_>> {
        Some(Box::new(self.pairs.keys()))
    }

    fn contains(&self, key: &AnyDataValue) -> bool {
        self.pairs.contains_key(key)
    }

    fn map_element(&self, key: &AnyDataValue) -> Option<&AnyDataValue> {
        self.pairs.get(key)
    }

    fn map_element_unchecked(&self, key: &AnyDataValue) -> &AnyDataValue {
        self.pairs.get(key).expect("unchecked method")
    }
}

impl std::hash::Hash for MapDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.pairs.hash(state);
    }
}

impl std::fmt::Display for MapDataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(iri) = self.label() {
            iri.fmt(f)?;
        }
        f.write_str(map::OPEN)?;
        let mut first = true;
        for (key, value) in self.pairs.iter() {
            if first {
                first = false;
            } else {
                f.write_str(map::SEPARATOR)?;
            }
            key.fmt(f)?;
            f.write_str(map::KEY_VALUE_ASSIGN)?;
            value.fmt(f)?;
        }
        f.write_str(map::CLOSE)
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashSet;

    use crate::datavalues::{AnyDataValue, DataValue, IriDataValue, MapDataValue, ValueDomain};

    #[test]
    fn test_map() {
        let dv1 = AnyDataValue::new_integer_from_i64(42);
        let dv2 = AnyDataValue::new_plain_string("test".to_string());
        let dv3 = AnyDataValue::new_boolean(true);
        let dv4 = AnyDataValue::new_plain_string("test2".to_string());
        let label = IriDataValue::new("http://example.org/label".to_string());

        let map = MapDataValue::new(
            Some(label.clone()),
            vec![
                (dv1.clone(), dv2.clone()),
                (dv1.clone(), dv3.clone()),
                (dv2.clone(), dv4.clone()),
            ],
        );

        assert_eq!(map.value_domain(), ValueDomain::Map);
        assert_eq!(map.datatype_iri(), "nemo:map".to_string());

        assert_eq!(map.label(), Some(&label));

        assert_eq!(map.length(), Some(2));
        assert_eq!(map.len_unchecked(), 2);

        let keys: HashSet<&AnyDataValue> = map.map_keys().expect("maps should have keys").collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&dv1.clone()));
        assert!(keys.contains(&dv2.clone()));

        assert_eq!(map.map_element(&dv1), Some(&dv3));
        assert_eq!(map.map_element_unchecked(&dv1), &dv3);
        assert_eq!(map.map_element(&dv2), Some(&dv4));
        assert_eq!(map.map_element_unchecked(&dv2), &dv4);
    }

    #[test]
    fn test_map_equality() {
        let dv1 = AnyDataValue::new_integer_from_i64(42);
        let dv2 = AnyDataValue::new_plain_string("test".to_string());
        let dv3 = AnyDataValue::new_boolean(true);
        let dv4 = AnyDataValue::new_plain_string("test2".to_string());

        let map1 = MapDataValue::from_iter(vec![
            (dv1.clone(), dv2.clone()),
            (dv1.clone(), dv3.clone()),
            (dv2.clone(), dv4.clone()),
        ]);

        let map2 =
            MapDataValue::from_iter(vec![(dv2.clone(), dv4.clone()), (dv1.clone(), dv3.clone())]);

        let map3 =
            MapDataValue::from_iter(vec![(dv1.clone(), dv2.clone()), (dv2.clone(), dv4.clone())]);

        assert_eq!(map1, map2);
        assert_eq!(map1.lexical_value(), map2.lexical_value());
        assert_eq!(map1.canonical_string(), map2.canonical_string());
        assert_ne!(map1, map3);
    }
}
