//! This module provides implementations [`super::DataValue`]s that represent maps of
//! data values to data values. The maps can be empty.

use std::collections::BTreeMap;

use super::{AnyDataValue, DataValue, ValueDomain};

// Physical representation of a finite map on [`DataValue`]s.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MapDataValue {
    pairs: BTreeMap<AnyDataValue, AnyDataValue>,
}

impl FromIterator<(AnyDataValue, AnyDataValue)> for MapDataValue {
    fn from_iter<T: IntoIterator<Item = (AnyDataValue, AnyDataValue)>>(iter: T) -> Self {
        Self {
            pairs: iter.into_iter().collect(),
        }
    }
}

impl DataValue for MapDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        self.pairs
            .iter()
            .map(|v| {
                DataValue::canonical_string(v.0) + "=" + DataValue::canonical_string(v.1).as_str()
            })
            .intersperse(",".to_string())
            .collect::<String>()
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Map
    }

    fn canonical_string(&self) -> String {
        super::datavalue::quote_string(self.lexical_value())
            + "^^"
            + &super::datavalue::quote_iri(self.datatype_iri().as_str())
    }
}

impl std::hash::Hash for MapDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.pairs.hash(state);
    }
}
