//! This module provides implementations [DataValue]s that represent boolean values.

use super::{
    DataValue, ValueDomain,
    syntax::{
        RDF_DATATYPE_INDICATOR,
        boolean::{FALSE, TRUE},
    },
};

/// Physical representation of a boolean value
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BooleanDataValue(bool);

impl BooleanDataValue {
    /// Create a new [BooleanDataValue].
    pub(crate) fn new(value: bool) -> Self {
        Self(value)
    }
}

impl DataValue for BooleanDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        if self.0 {
            TRUE.to_string()
        } else {
            FALSE.to_string()
        }
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Boolean
    }

    fn to_boolean(&self) -> Option<bool> {
        Some(self.0)
    }

    fn to_boolean_unchecked(&self) -> bool {
        self.0
    }

    fn canonical_string(&self) -> String {
        if self.0 {
            format!(
                "\"{TRUE}\"{RDF_DATATYPE_INDICATOR}<{}>",
                &self.datatype_iri()
            )
        } else {
            format!(
                "\"{FALSE}\"{RDF_DATATYPE_INDICATOR}<{}>",
                &self.datatype_iri()
            )
        }
    }
}

impl std::hash::Hash for BooleanDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.0.hash(state);
    }
}

impl Ord for BooleanDataValue {
    /// This order corresponds to the
    /// [comparison of boolean values defined for XPath (and SPARQL)](https://www.w3.org/TR/xpath-functions/#op.boolean).
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for BooleanDataValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for BooleanDataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.canonical_string().as_str())
    }
}
