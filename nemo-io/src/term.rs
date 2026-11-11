//! Reading the terms that cells hold, shared by the DSV [Term](crate::value_format::ValueFormat::Term)
//! format and the SPARQL TSV reader.

use std::borrow::Cow;

use nemo_physical::value::{DataValue, Escape, Iri};

/// The XML Schema namespace.
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// The syntax of a quoted literal, which differs between the readers of terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralSyntax {
    /// Nemo's term syntax, which the DSV [Term](crate::value_format::ValueFormat::Term) format reads
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
}
