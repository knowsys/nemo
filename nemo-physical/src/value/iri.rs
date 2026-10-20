//! IRIs, owned and borrowed.

use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
};

/// An owned [IRI](https://www.w3.org/TR/rdf11-concepts/#section-IRIs),
/// the name of an RDF resource, in the form of
/// [RFC 3987](https://www.rfc-editor.org/rfc/rfc3987).
/// Its borrowed view is [IriRef].
#[derive(Clone)]
pub struct Iri(Arc<str>);

impl Iri {
    /// Creates an IRI from its text.
    pub fn new(iri: impl Into<Arc<str>>) -> Self {
        Self(iri.into())
    }

    /// Returns whether a text is an IRI as SPARQL's grammar writes one in angle brackets,
    /// its [IRIREF](https://www.w3.org/TR/sparql11-query/#rIRIREF):
    /// no whitespace, no control character, and none of ``<>"{}|^`\``.
    ///
    /// Term syntax writes such an IRI, and reads it back.
    pub fn is_valid(text: &str) -> bool {
        !text.chars().any(|character| {
            character <= ' '
                || matches!(
                    character,
                    '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\'
                )
        })
    }

    /// Returns the text of the IRI.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the borrowed view of this IRI.
    pub fn as_ref(&self) -> IriRef<'_> {
        IriRef::new(&self.0)
    }
}

impl Deref for Iri {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Iri {
    fn from(iri: &str) -> Self {
        Self::new(iri)
    }
}

impl From<String> for Iri {
    fn from(iri: String) -> Self {
        Self::new(iri)
    }
}

impl From<Box<str>> for Iri {
    fn from(iri: Box<str>) -> Self {
        Self::new(iri)
    }
}

impl From<Arc<str>> for Iri {
    fn from(iri: Arc<str>) -> Self {
        Self::new(iri)
    }
}

impl PartialEq for Iri {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Iri {}

impl Ord for Iri {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Iri {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for Iri {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl fmt::Debug for Iri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Iri({:?})", &*self.0)
    }
}

impl fmt::Display for Iri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A borrowed [IRI](https://www.w3.org/TR/rdf11-concepts/#section-IRIs),
/// the name of an RDF resource, in the form of
/// [RFC 3987](https://www.rfc-editor.org/rfc/rfc3987).
/// Its owned form is [Iri].
///
/// The view holds the IRI in two segments:
/// the [namespace](Self::namespace), up to and including the last `/` or `#`,
/// and the [local part](Self::local) after it.
/// [to_cow](Self::to_cow) returns the whole text,
/// and equality and order are those of the whole text, not of the segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IriRef<'a> {
    /// The text up to and including the last `/` or `#`; possibly empty.
    namespace: &'a str,
    /// The text after the namespace; free of `/` and `#`.
    local: &'a str,
}

impl<'a> IriRef<'a> {
    /// Creates a view of an IRI, finding its split point.
    pub fn new(iri: &'a str) -> Self {
        let point = iri.rfind(['/', '#']).map_or(0, |position| position + 1);
        Self {
            namespace: &iri[..point],
            local: &iri[point..],
        }
    }

    /// Creates a view from segments already split at the canonical point.
    pub(crate) fn from_parts(namespace: &'a str, local: &'a str) -> Self {
        debug_assert!(
            !local.contains(['/', '#'])
                && (namespace.is_empty() || namespace.ends_with(['/', '#'])),
            "{namespace:?} | {local:?} is not the canonical split"
        );
        Self { namespace, local }
    }

    /// Returns the namespace segment, which is empty if the IRI has no `/` or `#`.
    pub fn namespace(self) -> &'a str {
        self.namespace
    }

    /// Returns the local segment following the namespace.
    pub fn local(self) -> &'a str {
        self.local
    }

    /// Returns the length of the whole IRI in bytes.
    pub fn len(self) -> usize {
        self.namespace.len() + self.local.len()
    }

    /// Returns whether the whole IRI is empty.
    pub fn is_empty(self) -> bool {
        self.namespace.is_empty() && self.local.is_empty()
    }

    /// Returns the text of the IRI,
    /// borrowed when it lies in one segment and joined otherwise.
    pub fn to_cow(self) -> Cow<'a, str> {
        if self.namespace.is_empty() {
            Cow::Borrowed(self.local)
        } else if self.local.is_empty() {
            Cow::Borrowed(self.namespace)
        } else {
            Cow::Owned(self.to_string())
        }
    }

    /// Clones the viewed IRI into an owned [Iri].
    pub fn to_owned(self) -> Iri {
        match self.to_cow() {
            Cow::Borrowed(text) => Iri::new(text),
            Cow::Owned(text) => Iri::new(text),
        }
    }

    /// Compares two IRIs by the bytes of their whole text,
    /// read across the seams of both without joining the segments.
    fn cmp_text(left: IriRef<'_>, right: IriRef<'_>) -> Ordering {
        let mut left = [left.namespace.as_bytes(), left.local.as_bytes()];
        let mut right = [right.namespace.as_bytes(), right.local.as_bytes()];

        loop {
            if left[0].is_empty() {
                left = [left[1], &[]];
            }
            if right[0].is_empty() {
                right = [right[1], &[]];
            }

            // An empty head is now an exhausted side.
            let length = left[0].len().min(right[0].len());
            if length == 0 {
                return left[0].len().cmp(&right[0].len());
            }

            match left[0][..length].cmp(&right[0][..length]) {
                Ordering::Equal => {}
                unequal => return unequal,
            }

            left[0] = &left[0][length..];
            right[0] = &right[0][length..];
        }
    }
}

impl Ord for IriRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.namespace == other.namespace {
            return self.local.cmp(other.local);
        }

        Self::cmp_text(*self, *other)
    }
}

impl PartialOrd for IriRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl IriRef<'_> {
    /// Returns whether term syntax writes this IRI without angle brackets:
    /// it is a bare name, which reads back as the IRI it spells,
    /// as `alice` or `item-1` do.
    ///
    /// A bare name is a letter or `_`, followed by letters, digits, `_` and `%`,
    /// with single `-` joining parts.
    /// `_`, `true` and `false` read as something else and keep their brackets.
    pub fn writes_bare(self) -> bool {
        if !self.namespace.is_empty() || matches!(self.local, "_" | "true" | "false") {
            return false;
        }
        let bytes = self.local.as_bytes();
        bytes
            .first()
            .is_some_and(|first| first.is_ascii_alphabetic() || *first == b'_')
            && bytes.iter().enumerate().all(|(index, byte)| match byte {
                b'-' => bytes
                    .get(index + 1)
                    .is_some_and(|next| next.is_ascii_alphanumeric() || *next == b'_'),
                _ => byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'%'),
            })
    }
}

impl fmt::Display for IriRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.namespace)?;
        f.write_str(self.local)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::hash::{BuildHasher, RandomState};

    #[test]
    fn bare_names_write_without_brackets() {
        for bare in ["alice", "Q42", "_x", "item-1", "my-lib-2", "a%20b"] {
            assert!(IriRef::new(bare).writes_bare(), "{bare}");
        }
        for bracketed in [
            "",
            "_",
            "true",
            "false",
            "1a",
            "a-",
            "a--b",
            "a-%",
            "urn:a",
            "http://e.org/a",
            "a b",
            "-a",
        ] {
            assert!(!IriRef::new(bracketed).writes_bare(), "{bracketed}");
        }
    }

    #[test]
    fn views_split_at_the_canonical_point() {
        let view = IriRef::new("http://example.org/a#b");
        assert_eq!(
            (view.namespace(), view.local()),
            ("http://example.org/a#", "b")
        );

        let view = IriRef::new("http://example.org/");
        assert_eq!(
            (view.namespace(), view.local()),
            ("http://example.org/", "")
        );

        let view = IriRef::new("urn:uuid:1234");
        assert_eq!((view.namespace(), view.local()), ("", "urn:uuid:1234"));

        assert!(IriRef::new("").is_empty());
    }

    #[test]
    fn views_agree_with_their_parts_and_owners() {
        let text = "http://example.org/a";
        let owned = Iri::new(text);
        let view = IriRef::new(text);
        let parts = IriRef::from_parts("http://example.org/", "a");

        let state = RandomState::new();

        assert_eq!(view, parts);
        assert_eq!(owned.as_ref(), parts);
        assert_eq!(view.to_owned(), owned);
        assert_eq!(state.hash_one(view), state.hash_one(parts));
        assert_eq!(state.hash_one(&owned), state.hash_one(view));
        assert_eq!(view.to_cow(), text);
        assert_eq!(parts.to_string(), text);
        assert_eq!(owned.len(), view.len());
    }

    #[test]
    fn order_is_the_order_of_the_text() {
        let texts = [
            "",
            "http://a",
            "http://a#z",
            "http://a/b",
            "http://a/b/c",
            "http://a/b/z",
            "http://a/bb",
            "http://a/z",
            "http://b",
            "urn:a",
        ];

        let mut sorted = texts.to_vec();
        sorted.sort_unstable();
        assert_eq!(sorted, texts, "the fixture is in text order");

        for (index, left) in texts.iter().enumerate() {
            for right in &texts[index + 1..] {
                assert!(IriRef::new(left) < IriRef::new(right), "{left} < {right}");
                assert!(IriRef::new(right) > IriRef::new(left), "{right} > {left}");
            }

            assert_eq!(IriRef::new(left).cmp(&IriRef::new(left)), Ordering::Equal);
        }
    }
}
