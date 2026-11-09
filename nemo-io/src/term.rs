//! Reading the terms that cells hold, shared by the DSV [Term](crate::format::ValueFormat::Term)
//! format and the SPARQL TSV reader.

use std::borrow::Cow;

use nemo_physical::{
    dictionary::{Dictionary, LabelScope},
    value::{DataValue, Escape, Iri},
};

/// The XML Schema namespace.
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// The syntax of a quoted literal, which differs between the readers of terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralSyntax {
    /// Nemo's term syntax, which the DSV [Term](crate::format::ValueFormat::Term) format reads
    /// and writes.
    ///
    /// It resolves the escapes that [Escape::in_string] recognizes
    /// and keeps any other backslash as written.
    /// In a datatype, `xsd:` abbreviates the XML Schema namespace.
    TermSyntax,
    /// Turtle's syntax, which SPARQL TSV uses for terms.
    ///
    /// It resolves the escapes `\t`, `\b`, `\n`, `\r`, `\f`, `\"`, `\'`, `\\`, `\uXXXX`
    /// and `\UXXXXXXXX`, and rejects any other.
    /// Datatypes are read as written.
    ///
    /// Only the SPARQL reader reads Turtle terms, so a build without it constructs none.
    #[cfg_attr(not(feature = "sparql"), allow(dead_code))]
    Turtle,
}

impl LiteralSyntax {
    /// Returns the datatype IRI written in a literal's `^^<…>`,
    /// with the `xsd:` abbreviation of term syntax expanded.
    fn datatype(self, written: &str) -> Cow<'_, str> {
        match (self, written.strip_prefix("xsd:")) {
            (LiteralSyntax::TermSyntax, Some(local)) => Cow::Owned(format!("{XSD}{local}")),
            _ => Cow::Borrowed(written),
        }
    }
}

/// Returns the IRI written between the angle brackets of a cell, with its character escapes
/// decoded, or `None` for a cell not in angle brackets.
///
/// The IRI need not be [valid](Iri::is_valid).
pub(crate) fn iri(cell: &str) -> Option<Cow<'_, str>> {
    cell.strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
        .map(Iri::unescape)
}

/// Returns the value of a quoted literal: `"…"`, `"…"@lang` or `"…"^^<datatype>`.
///
/// `rest` is the cell after the literal's opening quote.
///
/// Returns `None` if
/// - the quote never closes,
/// - the text after the closing quote is neither a language tag nor a datatype,
/// - the language tag is not well-formed, or the datatype rejects the text, or
/// - in [Turtle](LiteralSyntax::Turtle), the text has an escape that Turtle does not define.
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

/// Splits a literal at its closing quote into the raw text before it and the rest after it.
///
/// `rest` is the literal after its opening quote.
/// The closing quote is the first quote that no backslash escapes.
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

/// Resolves the escapes in the raw text of a literal.
///
/// Both syntaxes resolve the escapes that [Escape::in_string] recognizes,
/// and Turtle also resolves `\b`, a backspace.
/// For any other backslash, Turtle returns `None`, and term syntax keeps the backslash as written.
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

/// The blank nodes of one read of a document, each a null that its label names
/// in a [label scope](LabelScope).
///
/// A read given a scope names its nulls in that scope, which other reads may share,
/// such as every response of one SPARQL endpoint.
/// Otherwise the read opens a scope of its own, which closes when the read ends.
/// Either way, a label names the same null throughout the document,
/// for example across the graph blocks of a TriG file or across the rows of a response.
#[derive(Debug)]
pub(crate) struct BlankNodes {
    /// The scope in which the labels name nulls.
    scope: LabelScope,
    /// Whether the read opened the scope, and so must close it.
    owned: bool,
}

impl BlankNodes {
    /// Returns the blank nodes of a new read.
    ///
    /// The read names its nulls in the `given` scope,
    /// or, if that is `None`, in a fresh scope called `name`.
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

    /// Returns the null that a label names, minting it on the label's first use.
    pub(crate) fn null(&self, label: &str, dictionary: &mut Dictionary) -> DataValue {
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

    /// Term syntax and Turtle share most escapes.
    ///
    /// Only Turtle resolves `\b` and rejects an unknown escape,
    /// and only term syntax abbreviates the XML Schema namespace.
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
        // Turtle reads `\b` as a backspace, while term syntax keeps it for regular expressions.
        // Both read `\f` and `\'`.
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
        // Only term syntax abbreviates the XML Schema namespace.
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

    /// A label names one null within a read,
    /// and the same null in another read only if both share a scope.
    #[test]
    fn labels_name_one_null_each() {
        let mut dictionary = Dictionary::new();
        let blanks = BlankNodes::open(None, "document", &mut dictionary);
        let first = blanks.null("a", &mut dictionary);
        assert_eq!(blanks.null("a", &mut dictionary), first);
        assert_ne!(blanks.null("b", &mut dictionary), first);
        blanks.close(&mut dictionary);

        // Another read names its own nulls, unless both share a scope.
        let other = BlankNodes::open(None, "document", &mut dictionary);
        assert_ne!(other.null("a", &mut dictionary), first);
        let shared = dictionary.shared_label_scope("endpoint");
        let one = BlankNodes::open(Some(shared), "document", &mut dictionary);
        let two = BlankNodes::open(Some(shared), "document", &mut dictionary);
        let null = one.null("a", &mut dictionary);
        one.close(&mut dictionary);
        assert_eq!(two.null("a", &mut dictionary), null);
    }
}
