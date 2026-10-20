//! Language-tagged strings, owned and borrowed.

use std::{fmt, sync::Arc};

use crate::value::reference::write_quoted;

/// An owned [language-tagged string](https://www.w3.org/TR/rdf11-concepts/#dfn-language-tagged-string),
/// a text with a [BCP 47](https://www.rfc-editor.org/rfc/rfc5646) language tag:
/// the literals of `rdf:langString`.
/// Its borrowed view is [LanguageStringRef].
///
/// Tags are case-insensitive and kept lowercase:
/// `"a"@EN` and `"a"@en` are one value.
#[derive(Clone)]
pub struct LanguageString(Arc<Parts>);

/// The content of a language-tagged string.
struct Parts {
    /// The text content.
    text: Box<str>,
    /// The lowercase language tag.
    language: Box<str>,
}

impl LanguageString {
    /// Creates a language-tagged string, lowercasing the tag,
    /// or `None` for a tag that is not well-formed:
    /// subtags of one to eight ASCII letters or digits separated by single hyphens,
    /// the first of letters only.
    pub fn new(text: impl Into<Box<str>>, language: &str) -> Option<Self> {
        if !well_formed_tag(language) {
            return None;
        }

        let language = if language.bytes().any(|byte| byte.is_ascii_uppercase()) {
            language.to_ascii_lowercase().into()
        } else {
            language.into()
        };

        Some(Self(Arc::new(Parts {
            text: text.into(),
            language,
        })))
    }

    /// Returns the text content.
    pub fn text(&self) -> &str {
        &self.0.text
    }

    /// Returns the lowercase language tag.
    pub fn language(&self) -> &str {
        &self.0.language
    }

    /// Returns the borrowed view of this value.
    pub fn as_ref(&self) -> LanguageStringRef<'_> {
        LanguageStringRef {
            text: &self.0.text,
            language: &self.0.language,
        }
    }
}

impl PartialEq for LanguageString {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for LanguageString {}

impl Ord for LanguageString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(&other.as_ref())
    }
}

impl PartialOrd for LanguageString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for LanguageString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl fmt::Debug for LanguageString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LanguageString({self})")
    }
}

impl fmt::Display for LanguageString {
    /// Writes the term syntax `"text"@tag`,
    /// the text quoted with `\` and `"` escaped.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

/// A borrowed [language-tagged string](https://www.w3.org/TR/rdf11-concepts/#dfn-language-tagged-string),
/// a text with a [BCP 47](https://www.rfc-editor.org/rfc/rfc5646) language tag:
/// the literals of `rdf:langString`.
/// Its owned form is [LanguageString].
///
/// Tags are case-insensitive and kept lowercase:
/// `"a"@EN` and `"a"@en` are one value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LanguageStringRef<'a> {
    /// The text content.
    text: &'a str,
    /// The lowercase language tag.
    language: &'a str,
}

impl<'a> LanguageStringRef<'a> {
    /// Creates a view over a text and a tag in canonical form,
    /// or `None` for a tag that is not well-formed (see [LanguageString::new])
    /// or not lowercase.
    ///
    /// A view cannot lowercase without copying.
    /// [LanguageString::new] takes tags of any case.
    pub fn new(text: &'a str, language: &'a str) -> Option<Self> {
        let canonical =
            well_formed_tag(language) && !language.bytes().any(|byte| byte.is_ascii_uppercase());

        canonical.then_some(Self { text, language })
    }

    /// Creates a view over a text and a tag known to be canonical.
    pub(crate) fn from_parts(text: &'a str, language: &'a str) -> Self {
        debug_assert!(
            Self::new(text, language).is_some(),
            "{language:?} is not a canonical tag"
        );

        Self { text, language }
    }

    /// Returns the text content.
    pub fn text(self) -> &'a str {
        self.text
    }

    /// Returns the lowercase language tag.
    pub fn language(self) -> &'a str {
        self.language
    }

    /// Clones the viewed value into an owned [LanguageString].
    pub fn to_owned(self) -> LanguageString {
        LanguageString(Arc::new(Parts {
            text: self.text.into(),
            language: self.language.into(),
        }))
    }
}

impl fmt::Display for LanguageStringRef<'_> {
    /// Writes the term syntax `"text"@tag`,
    /// the text quoted with `\` and `"` escaped.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_quoted(f, self.text)?;
        write!(f, "@{}", self.language)
    }
}

/// Returns whether a language tag is well-formed enough to be a term:
/// subtags of one to eight ASCII letters or digits separated by single hyphens,
/// the first of letters only.
fn well_formed_tag(tag: &str) -> bool {
    let mut subtags = tag.split('-');
    let Some(primary) = subtags.next() else {
        return false;
    };

    let well_formed = |subtag: &str| {
        (1..=8).contains(&subtag.len()) && subtag.bytes().all(|byte| byte.is_ascii_alphanumeric())
    };

    well_formed(primary)
        && primary.bytes().all(|byte| byte.is_ascii_alphabetic())
        && subtags.all(well_formed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_are_lowercased_and_checked() {
        let upper = LanguageString::new("hello", "EN-US").expect("tag is well-formed");
        let lower = LanguageString::new("hello", "en-us").expect("tag is well-formed");

        assert_eq!(upper, lower);
        assert_eq!(upper.language(), "en-us");
        assert!(LanguageString::new("hello", "zh-hant-tw").is_some());

        for tag in [
            "",
            "en us",
            "en_US",
            "-en",
            "en-",
            "en--us",
            "1en",
            "abcdefghi",
            "en-é",
        ] {
            assert_eq!(LanguageString::new("hello", tag), None, "{tag:?}");
        }
    }

    #[test]
    fn views_take_canonical_tags_only() {
        assert!(LanguageStringRef::new("hello", "en-us").is_some());
        assert_eq!(LanguageStringRef::new("hello", "EN-US"), None);
        assert_eq!(LanguageStringRef::new("hello", "en--us"), None);
    }

    #[test]
    fn views_agree_with_their_owners() {
        let owned = LanguageString::new("say \"hi\"", "en").expect("tag is well-formed");
        let view = LanguageStringRef::new("say \"hi\"", "en").expect("tag is canonical");

        assert_eq!(owned.as_ref(), view);
        assert_eq!(view.to_owned(), owned);
        assert_eq!(owned.to_string(), r#""say \"hi\""@en"#);
        assert_eq!(view.to_string(), owned.to_string());
    }
}
