//! This module provides implementations [`super::DataValue`]s that represent boolean values.

use super::{DataValue, ValueDomain};

/// Physical representation of a boolean value
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BooleanDataValue(bool);

impl BooleanDataValue {
    /// Create a new [BooleanDataValue].
    pub fn new(value: bool) -> Self {
        Self(value)
    }
}

impl DataValue for BooleanDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        if self.0 {
            "true".to_string()
        } else {
            "false".to_string()
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
}

impl Eq for BooleanDataValue {}
