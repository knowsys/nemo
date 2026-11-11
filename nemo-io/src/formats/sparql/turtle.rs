//! Reading the IRIs and literals of SPARQL TSV cells, which are written in Turtle's syntax.

use std::borrow::Cow;

use nemo_physical::value::{DataValue, Escape, Iri};

/// Returns the IRI written between the angle brackets of a cell, with its character escapes
/// decoded, or `None` for a cell not in angle brackets.
///
/// The IRI need not be [valid](Iri::is_valid).
pub(super) fn iri(cell: &str) -> Option<Cow<'_, str>> {
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
/// - the text has an escape that Turtle does not define.
pub(super) fn literal(rest: &str) -> Option<DataValue> {
    let (raw, tail) = split(rest)?;
    let text = unescape(raw)?;
    if tail.is_empty() {
        Some(DataValue::string(text.as_str()))
    } else if let Some(language) = tail.strip_prefix('@') {
        DataValue::language_string(text.as_str(), language)
    } else if let Some(datatype) = tail
        .strip_prefix("^^<")
        .and_then(|rest| rest.strip_suffix('>'))
    {
        DataValue::typed_literal(&Iri::unescape(datatype), &text)
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
/// Turtle's escapes are `\t`, `\b`, `\n`, `\r`, `\f`, `\"`, `\'`, `\\`, `\uXXXX` and
/// `\UXXXXXXXX`.
/// Returns `None` for any other backslash.
fn unescape(raw: &str) -> Option<String> {
    if !raw.contains('\\') {
        return Some(raw.to_owned());
    }

    let mut resolved = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(backslash) = rest.find('\\') {
        resolved.push_str(&rest[..backslash]);
        let after = &rest[backslash + 1..];

        // Term syntax keeps `\b` for regular expressions, so its escapes lack the backspace.
        let (character, length) = match Escape::in_string(after) {
            Some(escape) => (escape.character, escape.length),
            None if after.starts_with('b') => ('\u{8}', 1),
            None => return None,
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

    /// A literal's escapes are Turtle's, its datatype is read as written,
    /// and an IRI is the text between its brackets.
    #[test]
    fn literals_and_iris_are_read_in_turtles_syntax() {
        assert_eq!(
            literal(r#"a\"b\\c\n\t\r""#),
            Some(DataValue::string("a\"b\\c\n\t\r"))
        );
        assert_eq!(
            literal(r#"café\b\f\'""#),
            Some(DataValue::string("café\u{8}\u{C}'"))
        );
        assert_eq!(
            literal(r#"\u00e9\U0001F600""#),
            Some(DataValue::string("é\u{1F600}"))
        );

        // An escape that Turtle does not define, an open quote and a tail that is neither
        // a language tag nor a datatype are not read.
        assert_eq!(literal(r#"a\u00""#), None);
        assert_eq!(literal(r#"a\d""#), None);
        assert_eq!(literal(r#"unclosed"#), None);
        assert_eq!(literal(r#"x"tail"#), None);
        assert_eq!(literal(r#"trailing\"#), None);

        // A datatype is not abbreviated.
        assert_eq!(
            literal(r#"5"^^<http://www.w3.org/2001/XMLSchema#integer>"#),
            Some(DataValue::integer(5))
        );
        assert_eq!(
            literal(r#"5"^^<xsd:integer>"#),
            DataValue::other("xsd:integer", "5")
        );
        assert_eq!(literal(r#"x"@en"#), DataValue::language_string("x", "en"));
        assert_eq!(iri("<urn:a>").as_deref(), Some("urn:a"));
        assert_eq!(iri(r"<urn:a\u0020b>").as_deref(), Some("urn:a b"));
        assert_eq!(iri("urn:a"), None);
    }
}
