//! This module provides implementations [`super::DataValue`]s that represent datavalues for
//! which we have no specific handling.

use super::{DataValue, ValueDomain};

/// Physical representation of arbitrary datavalues using two Strings, one
/// for the lexical value and one for the datatype IRI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtherDataValue(String, String);

impl OtherDataValue {
    /// Constructor. We do not currently check if the datatype IRI refers to a
    /// known type that is not really in [`ValueDomain::Other`].
    pub fn new(lexical_value: String, datatype_iri: String) -> Self {
        OtherDataValue(lexical_value, datatype_iri)
    }
}

impl DataValue for OtherDataValue {
    fn datatype_iri(&self) -> String {
        self.1.to_owned()
    }

    fn lexical_value(&self) -> String {
        self.0.to_owned()
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Other
    }

    fn canonical_string(&self) -> String {
        super::datavalue::quote_string(self.0.to_owned())
            + "^^"
            + &super::datavalue::quote_iri(self.1.as_str())
    }
}

impl std::hash::Hash for OtherDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.0.hash(state);
        self.1.hash(state);
    }
}

#[cfg(test)]
mod test {
    use super::OtherDataValue;
    use crate::datavalues::{DataValue, ValueDomain};

    #[test]
    fn test_other() {
        let value = "0FB7";
        let datatype_iri = "http://www.w3.org/2001/XMLSchema#hexBinary";
        let dv = OtherDataValue::new(value.to_string(), datatype_iri.to_string());

        assert_eq!(dv.lexical_value(), "0FB7");
        assert_eq!(dv.datatype_iri(), datatype_iri.to_string());
        assert_eq!(dv.value_domain(), ValueDomain::Other);
        assert_eq!(
            dv.canonical_string(),
            "\"".to_string() + value + "\"^^<" + datatype_iri + ">"
        );

        assert_eq!(dv.to_string(), None);
        assert_eq!(dv.to_iri(), None);
        assert_eq!(dv.to_language_tagged_string(), None);
        // ...
    }
}
