//! IRIs, owned and borrowed.

use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt::{self, Write},
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
};

use crate::value::{Escape, NameKind};

/// An owned [IRI](https://www.w3.org/TR/rdf11-concepts/#section-IRIs),
/// the name of an RDF resource.
///
/// Its text should have the form of [RFC 3987](https://www.rfc-editor.org/rfc/rfc3987),
/// but this is not checked.
/// Its borrowed view is [IriRef].
#[derive(Clone)]
pub struct Iri(Arc<str>);

impl Iri {
    /// Creates an IRI from its text.
    pub fn new(iri: impl Into<Arc<str>>) -> Self {
        Self(iri.into())
    }

    /// Returns whether a text can be written between angle brackets in SPARQL,
    /// as an [IRIREF](https://www.w3.org/TR/sparql11-query/#rIRIREF).
    ///
    /// Such a text contains no character up to and including the space,
    /// and none of ``<>"{}|^`\``.
    /// In term syntax, such a text between angle brackets denotes the IRI with that text.
    pub fn is_valid(text: &str) -> bool {
        !text.chars().any(|character| {
            character <= ' '
                || matches!(
                    character,
                    '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\'
                )
        })
    }

    /// Returns the text of the IRI that `written` denotes between angle brackets,
    /// with its character escapes decoded.
    ///
    /// The character escapes are `\uXXXX` and `\UXXXXXXXX`, as in Turtle and SPARQL.
    /// Any other backslash is kept as written.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::Iri;
    ///
    /// assert_eq!(Iri::unescape(r"http://e.org/caf\u00E9"), "http://e.org/café");
    /// assert_eq!(Iri::unescape(r"file:C:\data"), r"file:C:\data");
    /// ```
    pub fn unescape(written: &str) -> Cow<'_, str> {
        if !written.contains('\\') {
            return Cow::Borrowed(written);
        }
        let mut text = String::with_capacity(written.len());
        let mut rest = written;
        while let Some(backslash) = rest.find('\\') {
            text.push_str(&rest[..backslash]);
            let after = &rest[backslash + 1..];
            match Escape::in_iri(after) {
                Some(escape) => {
                    text.push(escape.character);
                    rest = &after[escape.length..];
                }
                None => {
                    text.push('\\');
                    rest = after;
                }
            }
        }
        text.push_str(rest);
        Cow::Owned(text)
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
/// the name of an RDF resource.
///
/// Its owned form is [Iri].
///
/// The view stores the IRI in two segments, which need not be adjacent in memory:
/// - the [namespace](Self::namespace), the text up to and including the last `/` or `#`;
/// - the [local part](Self::local), the text after it.
///
/// Equality and order are those of the whole text, which [to_cow](Self::to_cow) returns.
/// Every view splits its IRI at the same place, so equal texts have equal segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IriRef<'a> {
    /// The text up to and including the last `/` or `#`, which is empty if there is none.
    namespace: &'a str,
    /// The text after the namespace, which contains no `/` or `#`.
    local: &'a str,
}

impl<'a> IriRef<'a> {
    /// Creates a view of an IRI, split after its last `/` or `#`.
    pub fn new(iri: &'a str) -> Self {
        let point = iri.rfind(['/', '#']).map_or(0, |position| position + 1);
        Self {
            namespace: &iri[..point],
            local: &iri[point..],
        }
    }

    /// Creates a view from its two segments,
    /// which must be split where [new](Self::new) splits their IRI.
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

    /// Returns the local part, the text after the namespace.
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
    /// which is borrowed if one segment is empty and joined otherwise.
    pub fn to_cow(self) -> Cow<'a, str> {
        if self.namespace.is_empty() {
            Cow::Borrowed(self.local)
        } else if self.local.is_empty() {
            Cow::Borrowed(self.namespace)
        } else {
            Cow::Owned(self.to_string())
        }
    }

    /// Returns an owned copy of the IRI.
    pub fn to_owned(self) -> Iri {
        match self.to_cow() {
            Cow::Borrowed(text) => Iri::new(text),
            Cow::Owned(text) => Iri::new(text),
        }
    }

    /// Compares the whole texts of two IRIs byte by byte, without joining their segments.
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

            // After the shift, an empty first segment means that the side has no bytes left.
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

impl<'a> IriRef<'a> {
    /// Returns whether term syntax writes this IRI without angle brackets.
    ///
    /// This holds for an IRI that is a [bare name](NameKind::Bare), such as `alice` or `item-1`,
    /// since a bare name is read back as the IRI with its text.
    /// `_`, `true` and `false` have another meaning in term syntax, so they keep their brackets.
    pub fn writes_bare(self) -> bool {
        self.namespace.is_empty()
            && !matches!(self.local, "_" | "true" | "false")
            && NameKind::Bare.admits(self.local)
    }

    /// Returns the IRI in angle brackets, as term syntax writes it.
    ///
    /// Some characters are written as a character escape `\u…`,
    /// so that the text is read back as this IRI:
    /// - `>`, the space, and the ASCII control characters, including `DEL`;
    /// - a `\` that is followed by `u` or `U`, which is written `\u005C`.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::IriRef;
    ///
    /// let bracketed = |iri| IriRef::new(iri).bracketed().to_string();
    /// assert_eq!(bracketed("http://e.org/a"), "<http://e.org/a>");
    /// assert_eq!(bracketed("a b>c"), r"<a\u0020b\u003Ec>");
    /// assert_eq!(bracketed(r"C:\data\u1"), r"<C:\data\u005Cu1>");
    /// ```
    pub fn bracketed(self) -> impl fmt::Display + 'a {
        BracketedIri(self)
    }
}

/// An IRI written in angle brackets, as [IriRef::bracketed] returns it.
struct BracketedIri<'a>(IriRef<'a>);

impl fmt::Display for BracketedIri<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let IriRef { namespace, local } = self.0;
        let plain = |text: &str| {
            !text
                .bytes()
                .any(|byte| byte <= b' ' || matches!(byte, b'>' | b'\\' | 0x7F))
        };
        if plain(namespace) && plain(local) {
            return write!(f, "<{namespace}{local}>");
        }
        f.write_char('<')?;
        let mut characters = namespace.chars().chain(local.chars()).peekable();
        while let Some(character) = characters.next() {
            let escaped = match character {
                '>' | '\u{7F}' => true,
                '\\' => matches!(characters.peek(), Some('u' | 'U')),
                _ => character <= ' ',
            };
            if escaped {
                write!(f, "\\u{:04X}", u32::from(character))?;
            } else {
                f.write_char(character)?;
            }
        }
        f.write_char('>')
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

    /// Term syntax writes an IRI without brackets exactly if it is a bare name
    /// other than `_`, `true` and `false`.
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

    /// A view splits its IRI after the last `/` or `#`, or keeps it whole without either.
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

    /// A view of a text, a view of its segments and the owned IRI are equal,
    /// hash alike and have the same text.
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

    /// Views are ordered by the bytes of their whole text, wherever their segments split.
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
