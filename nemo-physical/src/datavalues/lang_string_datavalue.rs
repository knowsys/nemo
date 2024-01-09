//! This module provides implementations [`super::DataValue`]s that represent language-tagged strings.
//! Strings can be arbitrary Unicode strings. Language-tags should normally have a specific format, as
//! defined in [RFC5646 Tags for Identifying Languages](https://datatracker.ietf.org/doc/html/rfc5646),
//! but our implementation does not perform any checks and we consider any pair of Unicode strings to be
//! a valid string-tag combination.

use super::{DataValue, ValueDomain};

/// Physical representation of a language-tagged string using two Strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LangStringDataValue(String, String);

impl LangStringDataValue {
    /// Constructor. We do not currently perform any checks regarding the structure
    /// of the language tag: any string is accepted there. In practice, however, it
    /// should conform to [RFC5646 Tags for Identifying Languages](https://datatracker.ietf.org/doc/html/rfc5646).
    ///
    /// TODO: Should we normalize the lang_tag to lower case? This is allowed and might be needed to correct
    /// equality, but it does undo the (common?) formatting of things like `en-GB`.
    pub fn new(string: String, lang_tag: String) -> Self {
        LangStringDataValue(string, lang_tag)
    }
}

impl DataValue for LangStringDataValue {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    /// Returns a "virtual" lexical value for language-tagged strings. At the time of this writing,
    /// there is no standard for this encoding, and the method should not be used for output anyway
    /// for this type. Hence, the only use of this is to provide a bijection from (String,String) to String.
    fn lexical_value(&self) -> String {
        // We escape any existing @ in self.1 to avoid confusion with the final @ we inserted.
        // This should not occur, but it is cheaper to escape here than to validate all language tags.
        self.0.to_owned() + "@" + &self.1.replace("@", "@@")
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::LanguageTaggedString
    }

    fn to_language_tagged_string_unchecked(&self) -> (String, String) {
        (self.0.to_owned(), self.1.to_owned())
    }

    fn canonical_string(&self) -> String {
        super::datavalue::quote_string(self.0.to_owned()) + "@" + &self.1
    }
}

impl std::hash::Hash for LangStringDataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_domain().hash(state);
        self.0.hash(state);
        self.1.hash(state);
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

        assert_eq!(dv.lexical_value(), "Hello world@en-GB");
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::LanguageTaggedString);
        assert_eq!(
            dv.canonical_string(),
            "\"".to_string() + value + "\"@" + lang
        );

        assert_eq!(
            dv.to_language_tagged_string(),
            Some((value.to_string(), lang.to_string()))
        );
        assert_eq!(
            dv.to_language_tagged_string_unchecked(),
            (value.to_string(), lang.to_string())
        );
    }
}
