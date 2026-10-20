//! The term syntax cells share.
//!
//! The DSV `term` format and the SPARQL TSV reader both read an IRI in angle brackets,
//! a quoted literal with its language tag or datatype, the value of a typed literal,
//! and a blank node label.
//! What a *bare* cell means (a number, a boolean, an IRI constant, a blank node)
//! each reader decides for itself,
//! and each reads a quoted literal in its own [literal syntax](LiteralSyntax).

use std::{borrow::Cow, collections::HashMap};

use nemo_physical::{dictionary::Dictionary, value::DataValue};

/// The XML Schema namespace.
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// The syntax of a quoted literal, which differs between readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralSyntax {
    /// Nemo's term syntax, which the DSV `term` format reads and writes:
    /// the escapes `\\`, `\"`, `\n`, `\t`, `\r`, `\uXXXX`, and `\UXXXXXXXX`,
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

/// Returns the text between the angle brackets of a cell, or `None` for a cell without them.
///
/// The text may be no [valid](nemo_physical::value::Iri::is_valid) IRI.
pub(crate) fn iri(cell: &str) -> Option<&str> {
    cell.strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
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
        DataValue::typed_literal(&syntax.datatype(datatype), &text)
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

/// Resolves the escapes of a literal's raw text in a literal syntax.
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
        let (character, length) = match (after.chars().next(), syntax) {
            (Some('\\'), _) => ('\\', 1),
            (Some('"'), _) => ('"', 1),
            (Some('n'), _) => ('\n', 1),
            (Some('t'), _) => ('\t', 1),
            (Some('r'), _) => ('\r', 1),
            (Some('b'), LiteralSyntax::Turtle) => ('\u{8}', 1),
            (Some('f'), LiteralSyntax::Turtle) => ('\u{C}', 1),
            (Some('\''), LiteralSyntax::Turtle) => ('\'', 1),
            (Some(letter @ ('u' | 'U')), _) => {
                let width = if letter == 'u' { 4 } else { 8 };
                match (character_escape(&after[1..], width), syntax) {
                    (Some(character), _) => (character, 1 + width),
                    (None, LiteralSyntax::TermSyntax) => ('\\', 0),
                    (None, LiteralSyntax::Turtle) => return None,
                }
            }
            (_, LiteralSyntax::TermSyntax) => ('\\', 0),
            (_, LiteralSyntax::Turtle) => return None,
        };
        resolved.push(character);
        rest = &after[length..];
    }
    resolved.push_str(rest);
    Some(resolved)
}

/// Returns the character the given number of hexadecimal digits at the start of a text name,
/// or `None` if they are not that many digits or name no character.
fn character_escape(text: &str, width: usize) -> Option<char> {
    let digits = text.get(..width)?;
    if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    char::from_u32(u32::from_str_radix(digits, 16).ok()?)
}

/// The blank node labels of one read, each naming one fresh null.
///
/// Their scope is the document:
/// a label names the same null across TriG graph blocks and across a response's rows.
#[derive(Debug, Default)]
pub(crate) struct BlankNodes {
    /// The null each label seen so far names.
    labels: HashMap<String, u64>,
}

impl BlankNodes {
    /// Returns the null a label names, minted on first sight.
    pub(crate) fn null(&mut self, label: &str, dictionary: &mut Dictionary) -> DataValue {
        let id = self
            .labels
            .entry(label.to_owned())
            .or_insert_with(|| dictionary.fresh_null());
        DataValue::null(*id)
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
        // Turtle's further escapes are unknown to the rule language, which keeps the backslash,
        // while both read character escapes.
        assert_eq!(
            literal(r#"café\b\f\'""#, LiteralSyntax::Turtle),
            Some(DataValue::string("café\u{8}\u{C}'"))
        );
        assert_eq!(
            literal(r#"a\bc""#, LiteralSyntax::TermSyntax),
            Some(DataValue::string(r"a\bc"))
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
        assert_eq!(iri("<urn:a>"), Some("urn:a"));
        assert_eq!(iri("urn:a"), None);
    }

    #[test]
    fn labels_name_one_null_each() {
        let mut dictionary = Dictionary::new();
        let mut blanks = BlankNodes::default();
        let first = blanks.null("a", &mut dictionary);
        assert_eq!(blanks.null("a", &mut dictionary), first);
        assert_ne!(blanks.null("b", &mut dictionary), first);
    }
}
