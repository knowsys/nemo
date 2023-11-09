//! This module provides implementations [`super::DataValue`]s that represent Unicode strings.

use super::{DataValue,ValueDomain};

/// Physical representation of a Unicode string using String.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringDataValue(String);

impl StringDataValue {
    /// Constructor.
    pub fn new(string: String) -> Self {
        StringDataValue(string)
    }
}

impl DataValue for StringDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        self.0.to_owned()
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::String
    }

    fn to_string_unchecked(&self) -> String {
        self.0.to_owned()
    }
}

#[cfg(test)]
mod test {
    use super::StringDataValue;
    use crate::datavalues::{DataValue,ValueDomain};

    #[test]
    fn test_string() {
        let value = "Hello world";
        let dv = StringDataValue::new(value.to_string());

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(dv.datatype_iri(), "http://www.w3.org/2001/XMLSchema#string".to_string());
        assert_eq!(dv.value_domain(), ValueDomain::String);

        assert_eq!(dv.to_string(), Some(value.to_string()));
        assert_eq!(dv.to_string_unchecked(), value.to_string());
    }
}