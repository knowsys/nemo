//! This module provides implementations [DataValue]s that represent IRIs.
//! In essence, IRIs are represented by Unicode strings, but they are considered
//! distinct from elements of [ValueDomain::PlainString]. Moreover, some IRI-specific
//! requirements and normalizations might apply.

use super::{DataValue, ValueDomain};

/// Physical representation of a Unicode string using String.
///
/// SPARQL states: "Pairs of IRIs are ordered by comparing them as simple literals.",
/// which means that their string representations should be compared according to the
/// [Unicode codepoint collation](https://www.w3.org/TR/xpath-functions/#dt-codepoint-collation).
/// As [specified in the rust documentation](https://doc.rust-lang.org/std/primitive.str.html#impl-Ord-for-str),
/// this is also the default order for strings in rust.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IriDataValue(String);

impl IriDataValue {
    /// Constructor. It is currently not checked whether the IRI is valid according to applicable specifications
    /// and standards -- we just treat it like a string.
    pub(crate) fn new(iri: String) -> Self {
        IriDataValue(iri)
    }

    /// Nemo considers "plain" identifiers, such as a constant name `a` or a predicate name `edge`, as simple forms
    /// of relative IRIs. This function checks if the given IRI is guaranteed to be of this form.
    ///
    /// Not all relative IRIs are "names" in this sense, since, e.g., "/" and "#" is not allowed here. Moreover,
    /// names in Nemo cannot start with numbers.
    ///
    /// TODO: This should be aligned with how the parser works. Currently restricted to basic alpha-numeric names.
    /// Note: Failing to recognize an IRI as possible name should not have severe consequences (we just put some <>
    /// around such IRIs in formatting).
    pub(crate) fn is_name(&self) -> bool {
        let mut first = true;
        for c in self.0.chars() {
            match c {
                '0'..='9' | '_' | '-' => {
                    if first {
                        return false;
                    }
                }
                'a'..='z' | 'A'..='Z' => {}
                _ => {
                    return false;
                }
            }
            first = false;
        }
        true
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

impl std::hash::Hash for IriDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.0.hash(state);
    }
}

impl std::fmt::Display for IriDataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_name() {
            f.write_str(self.0.as_str())
        } else {
            f.write_str(self.canonical_string().as_str())
        }
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
