//! The term syntax cells share.
//!
//! The DSV `term` format and the SPARQL TSV reader both read an IRI in angle brackets,
//! a quoted literal with its language tag or datatype, the value of a typed literal,
//! and a blank node label.
//! What a *bare* cell means (a number, a boolean, an IRI constant, a blank node)
//! each reader decides for itself,
//! and each reads a quoted literal in its own [literal syntax](LiteralSyntax).

use std::borrow::Cow;

use nemo_physical::{
    dictionary::{Dictionary, LabelScope},
    value::{DataValue, Escape, Iri},
};

/// The XML Schema namespace.
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// The syntax of a quoted literal, which differs between readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralSyntax {
    /// Nemo's term syntax, which the DSV `term` format reads and writes:
    /// the escapes [Escape::in_string] reads,
    /// any other backslash kept as written, as programs keep it,
    /// and `xsd:` abbreviating the XML Schema namespace in a datatype.
    TermSyntax,
    /// Turtle's syntax, which SPARQL TSV uses for terms: the escapes `\t`,
    /// `\b`, `\n`, `\r`, `\f`, `\"`, `\'`, `\\`, `\uXXXX`, and
    /// `\UXXXXXXXX`, and datatypes as written. Only the SPARQL reader
    /// reads Turtle terms, so a build without it constructs none.
    #[cfg_attr(not(feature = "sparql"), allow(dead_code))]
    Turtle,
}

impl LiteralSyntax {
    /// Returns the datatype IRI a literal's `^^<…>` names,
    /// the term syntax's `xsd:` abbreviation expanded.
    fn datatype(self, written: &str) -> Cow<'_, str> {
        match (self, written.strip_prefix("xsd:")) {
            (LiteralSyntax::TermSyntax, Some(local)) => Cow::Owned(format!("{XSD}{local}")),
            _ => Cow::Borrowed(written),
        }
    }
}

/// Returns the IRI a cell spells between angle brackets, its character escapes decoded,
/// or `None` for a cell without them.
///
/// The IRI need not be [valid](Iri::is_valid).
pub(crate) fn iri(cell: &str) -> Option<Cow<'_, str>> {
    cell.strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
        .map(Iri::unescape)
}

/// Returns the value of a quoted literal (`"…"`, `"…"@lang`, or `"…"^^<datatype>`),
/// given the cell after its opening quote.
///
/// Returns `None` for a quote that never closes, an escape the syntax does not define,
/// a tail that is neither a language tag nor a datatype, or a lexical its datatype rejects.
pub(crate) fn literal(rest: &str, syntax: LiteralSyntax) -> Option<DataValue> {
    let (raw, tail) = split(rest)?;
    let text = unescape(raw, syntax)?;
    if tail.is_empty() {
        Some(DataValue::string(text.as_str()))
    } else if let Some(language) = tail.strip_prefix('@') {
        DataValue::language_string(text.as_str(), language)
    } else if let Some(datatype) = tail
        .strip_prefix("^^<")
        .and_then(|rest| rest.strip_suffix('>'))
    {
        DataValue::typed_literal(&syntax.datatype(&Iri::unescape(datatype)), &text)
    } else {
        None
    }
}

/// Splits a literal after its opening quote at its closing one,
/// the first quote no backslash escapes, into the raw text and the tail after the quote.
///
/// Returns `None` if the literal never closes.
fn split(rest: &str) -> Option<(&str, &str)> {
    let mut escaped = false;
    for (index, character) in rest.char_indices() {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '"' {
            return Some((&rest[..index], &rest[index + 1..]));
        }
    }
    None
}

/// Resolves the escapes of a literal's raw text in a literal syntax:
/// those [Escape::in_string] reads, and in Turtle also `\b`, a backspace.
///
/// Returns `None` for an escape Turtle does not define, or a backslash that ends the text;
/// term syntax keeps such a backslash, as programs do.
fn unescape(raw: &str, syntax: LiteralSyntax) -> Option<String> {
    if !raw.contains('\\') {
        return Some(raw.to_owned());
    }
    let mut resolved = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(backslash) = rest.find('\\') {
        resolved.push_str(&rest[..backslash]);
        let after = &rest[backslash + 1..];
        let (character, length) = match (Escape::in_string(after), syntax) {
            (Some(escape), _) => (escape.character, escape.length),
            (None, LiteralSyntax::Turtle) if after.starts_with('b') => ('\u{8}', 1),
            (None, LiteralSyntax::Turtle) => return None,
            (None, LiteralSyntax::TermSyntax) => ('\\', 0),
        };
        resolved.push(character);
        rest = &after[length..];
    }
    resolved.push_str(rest);
    Some(resolved)
}

/// The blank node labels of one read: the [label scope](LabelScope) in which each names a null.
///
/// A read given a scope names nulls in it, shared with the other reads given it,
/// such as every response of one SPARQL endpoint.
/// Otherwise the read opens a scope of its own, the document:
/// a label names the same null across TriG graph blocks and across a response's rows,
/// and the scope closes when the read ends.
#[derive(Debug)]
pub(crate) struct BlankNodes {
    /// The scope the labels name nulls in.
    scope: LabelScope,
    /// Whether the read opened the scope, and closes it.
    owned: bool,
}

impl BlankNodes {
    /// Returns the blank nodes of a read, in the given scope,
    /// or in a fresh one under the given name if there is none.
    pub(crate) fn open(given: Option<LabelScope>, name: &str, dictionary: &mut Dictionary) -> Self {
        match given {
            Some(scope) => Self {
                scope,
                owned: false,
            },
            None => Self {
                scope: dictionary.fresh_label_scope(name),
                owned: true,
            },
        }
    }

    /// Returns the null a label names, minted on first sight.
    pub(crate) fn null(&mut self, label: &str, dictionary: &mut Dictionary) -> DataValue {
        DataValue::null(dictionary.labeled_null(self.scope, label))
    }

    /// Ends the read, closing the scope if the read opened it.
    pub(crate) fn close(&self, dictionary: &mut Dictionary) {
        if self.owned {
            dictionary.close_label_scope(self.scope);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_syntax_and_turtle_differ_in_escapes_and_datatype_abbreviations() {
        // The escapes both syntaxes read.
        for syntax in [LiteralSyntax::TermSyntax, LiteralSyntax::Turtle] {
            assert_eq!(
                literal(r#"a\"b\\c\n\t\r""#, syntax),
                Some(DataValue::string("a\"b\\c\n\t\r")),
                "{syntax:?}"
            );
            assert_eq!(literal(r#"unclosed"#, syntax), None, "{syntax:?}");
            assert_eq!(literal(r#"x"tail"#, syntax), None, "{syntax:?}");
            assert_eq!(literal(r#"trailing\"#, syntax), None, "{syntax:?}");
        }
        // Turtle reads `\b` as a backspace; the rule language keeps it for regular expressions,
        // and reads `\f` and `\'` as Turtle does.
        assert_eq!(
            literal(r#"café\b\f\'""#, LiteralSyntax::Turtle),
            Some(DataValue::string("café\u{8}\u{C}'"))
        );
        assert_eq!(
            literal(r#"café\b\f\'""#, LiteralSyntax::TermSyntax),
            Some(DataValue::string("café\\b\u{C}'"))
        );
        assert_eq!(literal(r#"a\u00""#, LiteralSyntax::Turtle), None);
        assert_eq!(
            literal(r#"a\u00""#, LiteralSyntax::TermSyntax),
            Some(DataValue::string(r"a\u00"))
        );
        for syntax in [LiteralSyntax::TermSyntax, LiteralSyntax::Turtle] {
            assert_eq!(
                literal(r#"\u00e9\U0001F600""#, syntax),
                Some(DataValue::string("é\u{1F600}")),
                "{syntax:?}"
            );
        }
        // Only the rule language abbreviates the XML Schema namespace.
        assert_eq!(
            literal(r#"5"^^<xsd:integer>"#, LiteralSyntax::TermSyntax),
            Some(DataValue::integer(5))
        );
        assert_eq!(
            literal(r#"5"^^<xsd:integer>"#, LiteralSyntax::Turtle),
            DataValue::other("xsd:integer", "5")
        );
        assert_eq!(
            literal(r#"x"@en"#, LiteralSyntax::Turtle),
            DataValue::language_string("x", "en")
        );
        assert_eq!(iri("<urn:a>").as_deref(), Some("urn:a"));
        assert_eq!(iri(r"<urn:a\u0020b>").as_deref(), Some("urn:a b"));
        assert_eq!(iri("urn:a"), None);
    }

    #[test]
    fn labels_name_one_null_each() {
        let mut dictionary = Dictionary::new();
        let mut blanks = BlankNodes::open(None, "document", &mut dictionary);
        let first = blanks.null("a", &mut dictionary);
        assert_eq!(blanks.null("a", &mut dictionary), first);
        assert_ne!(blanks.null("b", &mut dictionary), first);
        blanks.close(&mut dictionary);

        // Another read names its own nulls, unless both share a scope.
        let mut other = BlankNodes::open(None, "document", &mut dictionary);
        assert_ne!(other.null("a", &mut dictionary), first);
        let shared = dictionary.label_scope("endpoint");
        let mut one = BlankNodes::open(Some(shared), "document", &mut dictionary);
        let mut two = BlankNodes::open(Some(shared), "document", &mut dictionary);
        let null = one.null("a", &mut dictionary);
        one.close(&mut dictionary);
        assert_eq!(two.null("a", &mut dictionary), null);
    }
}
