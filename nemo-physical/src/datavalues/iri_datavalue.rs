//! This module provides implementations [`super::DataValue`]s that represent IRIs.
//! In essence, IRIs are represented by Unicode strings, but they are considered
//! distinct from elements of ['super::ValueDomain::String']. Moreover, some IRI-specific
//! requirements and normalizations might apply.

use super::{DataValue, ValueDomain};

/// Physical representation of a Unicode string using String.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IriDataValue(String);

impl IriDataValue {
    /// Constructor. It is currently not checked whether the IRI is valid according to applicable specifications
    /// and standards -- we just treat it like a string.
    pub fn new(iri: String) -> Self {
        IriDataValue(iri)
    }
}

impl DataValue for IriDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        self.0.to_owned()
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Iri
    }

    fn to_iri_unchecked(&self) -> String {
        self.0.to_owned()
    }

    fn canonical_string(&self) -> String {
        super::datavalue::quote_iri(self.0.as_str())
    }
}

#[cfg(test)]
mod test {
    use super::IriDataValue;
    use crate::datavalues::{DataValue, ValueDomain};

    #[test]
    fn test_iri() {
        let value = "http://example.org/nemo";
        let dv = IriDataValue::new(value.to_string());

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#anyURI".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::Iri);
        assert_eq!(dv.canonical_string(), "<".to_string() + value + ">");

        assert_eq!(dv.to_iri(), Some(value.to_string()));
        assert_eq!(dv.to_iri_unchecked(), value.to_string());
    }
}
