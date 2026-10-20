//! Literals of datatypes without native support, owned and borrowed.

use std::{fmt, sync::Arc};

use crate::value::{datatype::Datatype, reference::write_quoted};

/// An owned [literal](https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal)
/// of a datatype this crate has no native representation for,
/// kept as its lexical form and its datatype IRI.
/// Its borrowed view is [OtherLiteralRef].
///
/// The datatypes [DataValue::typed_literal](crate::value::DataValue::typed_literal) parses
/// have no such literals: their literals are the values they denote.
#[derive(Clone)]
pub struct OtherLiteral(Arc<Parts>);

/// The content of a literal.
struct Parts {
    /// The datatype IRI.
    datatype: Box<str>,
    /// The lexical form.
    lexical: Box<str>,
}

impl OtherLiteral {
    /// Creates a literal from its datatype IRI and lexical form,
    /// or `None` for a native datatype.
    pub fn new(datatype: impl Into<Box<str>>, lexical: impl Into<Box<str>>) -> Option<Self> {
        let datatype = datatype.into();

        if !Datatype::of(&datatype).is_other() {
            return None;
        }

        Some(Self(Arc::new(Parts {
            datatype,
            lexical: lexical.into(),
        })))
    }

    /// Returns the datatype IRI.
    pub fn datatype(&self) -> &str {
        &self.0.datatype
    }

    /// Returns the lexical form.
    pub fn lexical(&self) -> &str {
        &self.0.lexical
    }

    /// Returns the borrowed view of this value.
    pub fn as_ref(&self) -> OtherLiteralRef<'_> {
        OtherLiteralRef {
            datatype: &self.0.datatype,
            lexical: &self.0.lexical,
        }
    }
}

impl PartialEq for OtherLiteral {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for OtherLiteral {}

impl Ord for OtherLiteral {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(&other.as_ref())
    }
}

impl PartialOrd for OtherLiteral {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for OtherLiteral {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl fmt::Debug for OtherLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OtherLiteral({self})")
    }
}

impl fmt::Display for OtherLiteral {
    /// Writes the term syntax `"lexical"^^<datatype>`,
    /// the lexical form quoted with `\` and `"` escaped.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

/// A borrowed [literal](https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal)
/// of a datatype this crate has no native representation for,
/// kept as its lexical form and its datatype IRI.
/// Its owned form is [OtherLiteral].
///
/// The datatypes [DataValue::typed_literal](crate::value::DataValue::typed_literal) parses
/// have no such literals: their literals are the values they denote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OtherLiteralRef<'a> {
    /// The datatype IRI.
    datatype: &'a str,
    /// The lexical form.
    lexical: &'a str,
}

impl<'a> OtherLiteralRef<'a> {
    /// Creates a view over a datatype IRI and a lexical form,
    /// or `None` for a native datatype (see [OtherLiteral::new]).
    pub fn new(datatype: &'a str, lexical: &'a str) -> Option<Self> {
        Datatype::of(datatype)
            .is_other()
            .then_some(Self { datatype, lexical })
    }

    /// Creates a view over a datatype known not to be native.
    pub(crate) fn from_parts(datatype: &'a str, lexical: &'a str) -> Self {
        debug_assert!(
            Datatype::of(datatype).is_other(),
            "{datatype:?} has a dedicated value"
        );
        Self { datatype, lexical }
    }

    /// Returns the datatype IRI.
    pub fn datatype(self) -> &'a str {
        self.datatype
    }

    /// Returns the lexical form.
    pub fn lexical(self) -> &'a str {
        self.lexical
    }

    /// Clones the viewed value into an owned [OtherLiteral].
    pub fn to_owned(self) -> OtherLiteral {
        OtherLiteral(Arc::new(Parts {
            datatype: self.datatype.into(),
            lexical: self.lexical.into(),
        }))
    }
}

impl fmt::Display for OtherLiteralRef<'_> {
    /// Writes the term syntax `"lexical"^^<datatype>`,
    /// the lexical form quoted with `\` and `"` escaped.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_quoted(f, self.lexical)?;
        write!(f, "^^<{}>", self.datatype)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::value::datatype::{rdf, xsd};

    #[test]
    fn native_datatypes_are_refused() {
        let xsd = |local: &str| format!("{}{local}", xsd::NAMESPACE);

        assert!(OtherLiteral::new(xsd("integer"), "42").is_none());
        assert!(OtherLiteral::new(xsd("int"), "42").is_none());
        assert!(OtherLiteral::new(rdf::LANG_STRING, "hello").is_none());
        assert!(OtherLiteralRef::new(xsd::DATE_TIME, "2024-01-01T00:00:00Z").is_none());
        assert!(OtherLiteral::new(xsd::DATE, "2024-01-01").is_none());
        assert!(OtherLiteral::new(xsd::DAY_TIME_DURATION, "PT1H").is_none());
        assert!(OtherLiteral::new(xsd("gYear"), "2024").is_some());
        assert!(OtherLiteralRef::new("http://example.org/unit", "5cm").is_some());
    }

    #[test]
    fn views_agree_with_their_owners() {
        let owned = OtherLiteral::new("http://example.org/unit", "5cm").expect("not native");
        let view = OtherLiteralRef::new("http://example.org/unit", "5cm").expect("not native");

        assert_eq!(owned.as_ref(), view);
        assert_eq!(view.to_owned(), owned);
        assert_eq!(owned.to_string(), "\"5cm\"^^<http://example.org/unit>");
        assert_eq!(view.to_string(), owned.to_string());
    }
}
