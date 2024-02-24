//! This module provides implementations [`super::DataValue`]s that represent language-tagged strings.
//! Strings can be arbitrary Unicode strings. Language-tags should normally have a specific format, as
//! defined in [RFC5646 Tags for Identifying Languages](https://datatracker.ietf.org/doc/html/rfc5646),
//! but our implementation does not perform any checks and we consider any pair of Unicode strings to be
//! a valid string-tag combination.

use super::{DataValue, ValueDomain};

/// Physical representation of a language-tagged string using two Strings.
///
/// Note: SPARQL does not define any order for language-tagged literals. We assume
/// that the default order should be ok.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LangStringDataValue(String, String);

impl LangStringDataValue {
    /// Constructor. We do not currently perform any checks regarding the structure
    /// of the language tag: any string is accepted there. In practice, however, it
    /// should conform to [RFC5646 Tags for Identifying Languages](https://datatracker.ietf.org/doc/html/rfc5646).
    ///
    /// The langauge tag is normalized to lower case, following RDF Schema 1.1, which states that
    /// "The value space of language tags is always in lower case."
    pub(crate) fn new(string: String, lang_tag: String) -> Self {
        LangStringDataValue(string, lang_tag.to_ascii_lowercase())
    }
}

impl DataValue for LangStringDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    /// Returns a "virtual" lexical value for language-tagged strings, which includes the language tag.
    /// This is *not* the standard interpretation of lexcial value for such literals, which would only
    /// include the value. We still use it to provide a bijection from (String,String) to String, avoiding
    /// the need for a separate language-tag in all data value representations.
    ///
    /// The method should not be used for output: use [Self::canonical_string] instead.
    fn lexical_value(&self) -> String {
        // We escape any existing @ in self.1 to avoid confusion with the final @ we inserted.
        // This should not occur, but it is cheaper to escape here than to validate all language tags.
        self.0.to_owned() + "@" + &self.1.replace('@', "@@")
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::LanguageTaggedString
    }

    fn to_language_tagged_string_unchecked(&self) -> (String, String) {
        (self.0.to_owned(), self.1.to_owned())
    }

    fn canonical_string(&self) -> String {
        super::datavalue::quote_string(self.0.as_str()) + "@" + &self.1
    }
}

impl std::hash::Hash for LangStringDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.0.hash(state);
        self.1.hash(state);
    }
}

impl std::fmt::Display for LangStringDataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.canonical_string().as_str())
    }
}

#[cfg(test)]
mod test {
    use super::LangStringDataValue;
    use crate::datavalues::{DataValue, ValueDomain};

    #[test]
    fn test_lang_string() {
        let value = "Hello world";
        let lang = "en-GB";
        let dv = LangStringDataValue::new(value.to_string(), lang.to_string());

        assert_eq!(dv.lexical_value(), "Hello world@en-gb");
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::LanguageTaggedString);
        assert_eq!(
            dv.canonical_string(),
            "\"".to_string() + value + "\"@" + lang.to_ascii_lowercase().as_str()
        );

        assert_eq!(
            dv.to_language_tagged_string(),
            Some((value.to_string(), lang.to_ascii_lowercase()))
        );
        assert_eq!(
            dv.to_language_tagged_string_unchecked(),
            (value.to_string(), lang.to_ascii_lowercase())
        );
    }
}
