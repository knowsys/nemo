//! This module provides implementations [`super::DataValue`]s that represent Unicode strings.

use super::{DataValue, ValueDomain};

/// Physical representation of a Unicode string using String.
///
/// Note: As [specified in the rust documentation](https://doc.rust-lang.org/std/primitive.str.html#impl-Ord-for-str),
/// the derived order agrees with the [Unicode codepoint collation](https://www.w3.org/TR/xpath-functions/#dt-codepoint-collation),
/// which is the style of string ordering used in SPARQL and as a default in XPath.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StringDataValue(String);

impl StringDataValue {
    /// Constructor.
    pub(crate) fn new(string: String) -> Self {
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

    fn to_plain_string_unchecked(&self) -> String {
        self.0.to_owned()
    }

    fn canonical_string(&self) -> String {
        super::datavalue::quote_string(self.0.as_str())
    }
}

impl std::hash::Hash for StringDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.0.hash(state);
    }
}

impl std::fmt::Display for StringDataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&super::datavalue::quote_string(self.0.as_str()).as_str())
    }
}

#[cfg(test)]
mod test {
    use super::StringDataValue;
    use crate::datavalues::{DataValue, ValueDomain};

    #[test]
    fn test_string() {
        let value = "Hello world";
        let dv = StringDataValue::new(value.to_string());

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#string".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::String);
        assert_eq!(dv.canonical_string(), "\"".to_string() + value + "\"");

        assert_eq!(dv.to_plain_string(), Some(value.to_string()));
        assert_eq!(dv.to_plain_string_unchecked(), value.to_string());
    }

    #[test]
    fn test_string_escaping() {
        let value = "Hello!\n\"World\"!\tTest\\backslash";
        let dv = StringDataValue::new(value.to_string());

        // No escaping in actual value ...
        assert_eq!(dv.lexical_value(), value.to_string());
        // ... but in serialization as string
        assert_eq!(
            dv.canonical_string(),
            "\"Hello!\\n\\\"World\\\"!\tTest\\\\backslash\""
        );
    }
}
