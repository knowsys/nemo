//! String operations.
//!
//! A string operand is a plain string or a language-tagged string,
//! and the first string operand of an operation is its subject.
//! Unless noted otherwise, an operation that returns the subject changed, or a part of it,
//! keeps the subject's language tag.
//!
//! A character is a Unicode code point, as in
//! [XPath](https://www.w3.org/TR/xpath-functions-31/#character-terminology).
//! Lengths and positions count code points, not grapheme clusters,
//! and strings are matched code point by code point, without normalization.

use unicode_segmentation::UnicodeSegmentation;

use crate::{
    function::StackValue,
    value::{DataValue, EvaluationError, NumberRef, ValueRef},
};

/// Returns the text of a string operand, or `None` for anything else.
pub(super) fn text(value: ValueRef<'_>) -> Option<&str> {
    match value {
        ValueRef::String(value) => Some(value),
        ValueRef::LanguageString(value) => Some(value.text()),
        _ => None,
    }
}

/// Returns the texts of a subject and of a second string that is compatible with it,
/// as SPARQL's
/// [argument compatibility](https://www.w3.org/TR/sparql12-query/#func-arg-compatibility)
/// requires.
///
/// The second string must be plain, or have the subject's language tag.
/// Any other operand, or a tagged second string with a plain subject,
/// is a [TypeMismatch](EvaluationError::TypeMismatch).
/// Two tagged strings with different tags are [Malformed](EvaluationError::Malformed).
fn compatible<'a>(
    subject: ValueRef<'a>,
    other: ValueRef<'a>,
) -> Result<(&'a str, &'a str), EvaluationError> {
    let subject_text = text(subject).ok_or(EvaluationError::TypeMismatch)?;
    let other_text = match (subject, other) {
        (_, ValueRef::String(other)) => other,
        (ValueRef::LanguageString(subject), ValueRef::LanguageString(other)) => {
            if subject.language() != other.language() {
                return Err(EvaluationError::Malformed);
            }
            other.text()
        }
        _ => return Err(EvaluationError::TypeMismatch),
    };

    Ok((subject_text, other_text))
}

/// Returns a text as a string with the subject's language tag, if the subject has one.
fn like(subject: ValueRef<'_>, text: String) -> StackValue<'static> {
    match subject {
        ValueRef::LanguageString(subject) => DataValue::language_string(text, subject.language())
            .expect("the subject's language tag is canonical")
            .into(),
        _ => DataValue::string(text).into(),
    }
}

/// Returns the length of a string in characters,
/// SPARQL's [`STRLEN`](https://www.w3.org/TR/sparql12-query/#func-strlen).
pub(crate) fn length(value: ValueRef<'_>) -> StackValue<'static> {
    match text(value) {
        Some(text) => {
            let length = i64::try_from(text.chars().count()).expect("string lengths fit i64");
            ValueRef::Number(NumberRef::Decimal(length.into())).into()
        }
        None => EvaluationError::TypeMismatch.into(),
    }
}

/// Converts a string to upper case,
/// SPARQL's [`UCASE`](https://www.w3.org/TR/sparql12-query/#func-ucase).
///
/// The conversion follows Unicode's default mappings, whatever the language tag.
pub(crate) fn upper_case(value: ValueRef<'_>) -> StackValue<'static> {
    transform(value, str::to_uppercase)
}

/// Converts a string to lower case,
/// SPARQL's [`LCASE`](https://www.w3.org/TR/sparql12-query/#func-lcase).
///
/// The conversion follows Unicode's default mappings, whatever the language tag.
pub(crate) fn lower_case(value: ValueRef<'_>) -> StackValue<'static> {
    transform(value, str::to_lowercase)
}

/// Reverses a string by grapheme clusters,
/// so that a combining mark stays on its base character.
///
/// This is not a SPARQL function.
pub(crate) fn reverse(value: ValueRef<'_>) -> StackValue<'static> {
    transform(value, |text| text.graphemes(true).rev().collect())
}

/// Applies a text function to a string, keeping its language tag.
fn transform(value: ValueRef<'_>, apply: impl FnOnce(&str) -> String) -> StackValue<'static> {
    match text(value) {
        Some(text) => like(value, apply(text)),
        None => EvaluationError::TypeMismatch.into(),
    }
}

/// Removes whitespace, Unicode's `White_Space` characters, from the ends of a string.
///
/// `ends` is [str::trim], [str::trim_start] or [str::trim_end], and decides which ends.
/// This is not a SPARQL function.
pub(crate) fn trim(value: ValueRef<'_>, ends: impl FnOnce(&str) -> &str) -> StackValue<'static> {
    transform(value, |text| ends(text).to_owned())
}

/// Returns whether the first string contains the second,
/// SPARQL's [`CONTAINS`](https://www.w3.org/TR/sparql12-query/#func-contains).
pub(crate) fn contains(subject: ValueRef<'_>, pattern: ValueRef<'_>) -> StackValue<'static> {
    predicate(subject, pattern, |subject, pattern| {
        subject.contains(pattern)
    })
}

/// Returns whether the first string starts with the second,
/// SPARQL's [`STRSTARTS`](https://www.w3.org/TR/sparql12-query/#func-strstarts).
pub(crate) fn starts_with(subject: ValueRef<'_>, pattern: ValueRef<'_>) -> StackValue<'static> {
    predicate(subject, pattern, |subject, pattern| {
        subject.starts_with(pattern)
    })
}

/// Returns whether the first string ends with the second,
/// SPARQL's [`STRENDS`](https://www.w3.org/TR/sparql12-query/#func-strends).
pub(crate) fn ends_with(subject: ValueRef<'_>, pattern: ValueRef<'_>) -> StackValue<'static> {
    predicate(subject, pattern, |subject, pattern| {
        subject.ends_with(pattern)
    })
}

/// Applies a text predicate to a subject and a [compatible] second string.
fn predicate(
    subject: ValueRef<'_>,
    pattern: ValueRef<'_>,
    test: impl FnOnce(&str, &str) -> bool,
) -> StackValue<'static> {
    match compatible(subject, pattern) {
        Ok((subject, pattern)) => test(subject, pattern).into(),
        Err(error) => error.into(),
    }
}

/// Returns the part of a string before the first occurrence of another,
/// SPARQL's [`STRBEFORE`](https://www.w3.org/TR/sparql12-query/#func-strbefore).
///
/// If the other string does not occur, the result is a plain empty string,
/// even for a tagged subject.
pub(crate) fn before(subject: ValueRef<'_>, pattern: ValueRef<'_>) -> StackValue<'static> {
    let (subject_text, pattern) = match compatible(subject, pattern) {
        Ok(texts) => texts,
        Err(error) => return error.into(),
    };

    match subject_text.find(pattern) {
        Some(at) => like(subject, subject_text[..at].to_owned()),
        None => DataValue::string("").into(),
    }
}

/// Returns the part of a string after the first occurrence of another,
/// SPARQL's [`STRAFTER`](https://www.w3.org/TR/sparql12-query/#func-strafter).
///
/// If the other string does not occur, the result is a plain empty string,
/// even for a tagged subject.
pub(crate) fn after(subject: ValueRef<'_>, pattern: ValueRef<'_>) -> StackValue<'static> {
    let (subject_text, pattern) = match compatible(subject, pattern) {
        Ok(texts) => texts,
        Err(error) => return error.into(),
    };

    match subject_text.find(pattern) {
        Some(at) => like(subject, subject_text[at + pattern.len()..].to_owned()),
        None => DataValue::string("").into(),
    }
}

/// Returns the substring from a 1-based position to the end of the string,
/// as [substring_with] does without a length.
pub(crate) fn substring(subject: ValueRef<'_>, start: ValueRef<'_>) -> StackValue<'static> {
    window(subject, start, None)
}

/// Returns the substring of the characters at the 1-based positions
/// from `start` up to, but excluding, `start` plus `length`,
/// SPARQL's [`SUBSTR`](https://www.w3.org/TR/sparql12-query/#func-substr).
///
/// This is XPath's
/// [`fn:substring`](https://www.w3.org/TR/xpath-functions-31/#func-substring)
/// restricted to the integer positions that SPARQL's signature allows.
/// The positions are clamped to the string,
/// so a start below 1 shortens the substring rather than shifting it.
pub(crate) fn substring_with(
    subject: ValueRef<'_>,
    start: ValueRef<'_>,
    length: ValueRef<'_>,
) -> StackValue<'static> {
    window(subject, start, Some(length))
}

/// Returns the substring from `start` to the end, or of `length` characters if it is given.
///
/// A start or a length that is not an integer is a [TypeMismatch](EvaluationError::TypeMismatch).
fn window(
    subject: ValueRef<'_>,
    start: ValueRef<'_>,
    length: Option<ValueRef<'_>>,
) -> StackValue<'static> {
    let Some(subject_text) = text(subject) else {
        return EvaluationError::TypeMismatch.into();
    };
    let Some(start) = position(start) else {
        return EvaluationError::TypeMismatch.into();
    };
    let end = match length {
        Some(length) => match position(length) {
            Some(length) => match window_end(start, length) {
                Ok(end) => end,
                Err(error) => return error.into(),
            },
            None => return EvaluationError::TypeMismatch.into(),
        },
        None => i64::MAX,
    };

    let first = saturated(start).max(1);
    // A count beyond `usize` (on 32-bit targets) exceeds every string's length.
    let skip = usize::try_from(first - 1).unwrap_or(usize::MAX);
    let take = usize::try_from(end.saturating_sub(first).max(0)).unwrap_or(usize::MAX);
    let taken: String = subject_text.chars().skip(skip).take(take).collect();

    like(subject, taken)
}

/// Returns a position operand as a number if it is an integer of any size, and `None` otherwise.
fn position(value: ValueRef<'_>) -> Option<NumberRef<'_>> {
    match value {
        ValueRef::Number(number) if number.is_integral() => Some(number),
        _ => None,
    }
}

/// Returns the end of a substring, its start plus its length, saturated to 64 bits.
///
/// Every string is shorter than the 64-bit range, so saturation selects the same characters.
fn window_end(start: NumberRef<'_>, length: NumberRef<'_>) -> Result<i64, EvaluationError> {
    match (start.to_i64(), length.to_i64()) {
        (Some(start), Some(length)) => Ok(start.saturating_add(length)),
        // A wide start and a wide length may still end within the string,
        // so they are added exactly.
        _ => match (start + length)?.as_ref() {
            ValueRef::Number(end) => Ok(saturated(end)),
            _ => unreachable!("a sum of integers is a number"),
        },
    }
}

/// Returns an integer saturated to 64 bits.
fn saturated(value: NumberRef<'_>) -> i64 {
    match value.to_i64() {
        Some(value) => value,
        // Only integers beyond the 64-bit range are wide.
        None if value.is_negative() => i64::MIN,
        None => i64::MAX,
    }
}

/// Returns the concatenation of strings,
/// SPARQL's [`CONCAT`](https://www.w3.org/TR/sparql12-query/#func-concat).
///
/// The result has the operands' language tag if they all have the same tag,
/// and is a plain string otherwise.
/// The first operand that is an error or not a string decides the result,
/// its error or a [TypeMismatch](EvaluationError::TypeMismatch).
pub(crate) fn concat(arguments: &[StackValue<'_>]) -> StackValue<'static> {
    let mut result = String::new();
    let mut tag: Option<Option<&str>> = None;
    for argument in arguments {
        let value = match argument.as_result() {
            Ok(value) => value,
            Err(error) => return error.into(),
        };
        let Some(text) = text(value) else {
            return EvaluationError::TypeMismatch.into();
        };
        result.push_str(text);

        let language = match value {
            ValueRef::LanguageString(value) => Some(value.language()),
            _ => None,
        };
        tag = Some(match tag {
            None => language,
            Some(tag) if tag == language => tag,
            Some(_) => None,
        });
    }

    match tag.flatten() {
        Some(language) => DataValue::language_string(result, language)
            .expect("the operands' language tag is canonical")
            .into(),
        None => DataValue::string(result).into(),
    }
}

/// Returns the Levenshtein distance between two strings, in characters.
///
/// The strings may have any language tags. This is not a SPARQL function.
pub(crate) fn levenshtein(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    let (Some(left), Some(right)) = (text(left), text(right)) else {
        return EvaluationError::TypeMismatch.into();
    };

    let left: Vec<char> = left.chars().collect();
    let right: Vec<char> = right.chars().collect();
    let mut previous: Vec<usize> = (0..=right.len()).collect();
    let mut current = vec![0usize; right.len() + 1];
    for (row, &from) in left.iter().enumerate() {
        current[0] = row + 1;
        for (column, &to) in right.iter().enumerate() {
            let substitution = previous[column] + usize::from(from != to);
            current[column + 1] = substitution
                .min(previous[column + 1] + 1)
                .min(current[column] + 1);
        }
        std::mem::swap(&mut previous, &mut current);
    }

    let distance = i64::try_from(previous[right.len()]).expect("string lengths fit i64");
    ValueRef::Number(NumberRef::Decimal(distance.into())).into()
}

/// Percent-encodes a string,
/// SPARQL's [`ENCODE_FOR_URI`](https://www.w3.org/TR/sparql12-query/#func-encode),
/// XPath's [`fn:encode-for-uri`](https://www.w3.org/TR/xpath-functions-31/#func-encode-for-uri).
///
/// The unreserved characters of RFC 3986 are kept,
/// and every other byte of the string's UTF-8 encoding is percent-encoded.
/// The result is a plain string.
pub(crate) fn uri_encode(value: ValueRef<'_>) -> StackValue<'static> {
    let Some(text) = text(value) else {
        return EvaluationError::TypeMismatch.into();
    };

    let mut encoded = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }

    DataValue::string(encoded).into()
}

/// Percent-decodes a string, the inverse of [uri_encode].
///
/// An escape that is not `%` followed by two hex digits,
/// or decoded bytes that are not UTF-8, are [Malformed](EvaluationError::Malformed).
/// The result is a plain string. This is not a SPARQL function.
pub(crate) fn uri_decode(value: ValueRef<'_>) -> StackValue<'static> {
    let Some(text) = text(value) else {
        return EvaluationError::TypeMismatch.into();
    };

    let mut decoded = Vec::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut position = 0;
    while position < bytes.len() {
        if bytes[position] == b'%' {
            // An escape has hex digits only, but `from_str_radix` would also accept a sign.
            let byte = text
                .get(position + 1..position + 3)
                .filter(|pair| pair.bytes().all(|digit| digit.is_ascii_hexdigit()))
                .and_then(|pair| u8::from_str_radix(pair, 16).ok());
            let Some(byte) = byte else {
                return EvaluationError::Malformed.into();
            };
            decoded.push(byte);
            position += 3;
        } else {
            decoded.push(bytes[position]);
            position += 1;
        }
    }

    match String::from_utf8(decoded) {
        Ok(decoded) => DataValue::string(decoded).into(),
        Err(_) => EvaluationError::Malformed.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        testing::{number, value},
        value::LanguageStringRef,
    };

    /// Returns the value as a language-tagged string, if it is one.
    fn tagged(value: &DataValue) -> Option<LanguageStringRef<'_>> {
        match value.as_ref() {
            ValueRef::LanguageString(value) => Some(value),
            _ => None,
        }
    }

    /// Returns the text as a string tagged as English.
    fn english(text: &str) -> DataValue {
        DataValue::language_string(text, "en").expect("the tag is nonempty")
    }

    /// Substrings follow XPath's `fn:substring` for integer positions of any size,
    /// and a position that is not an integer is a type mismatch.
    #[test]
    fn substrings_follow_xpath() {
        let subject = DataValue::string("motor car");
        let int = DataValue::integer;
        let from = |start: &DataValue| value(substring(subject.as_ref(), start.as_ref()));
        let with = |start: &DataValue, length: &DataValue| {
            value(substring_with(
                subject.as_ref(),
                start.as_ref(),
                length.as_ref(),
            ))
        };

        assert_eq!(from(&int(6)), DataValue::string(" car"));
        assert_eq!(from(&int(0)), subject);
        assert_eq!(from(&int(-3)), subject);
        assert_eq!(from(&int(i64::MIN)), subject);
        assert_eq!(from(&int(100)), DataValue::string(""));
        // The substring has the positions from the start up to the start plus the length,
        // so a start below 1 shortens it.
        assert_eq!(with(&int(1), &int(5)), DataValue::string("motor"));
        assert_eq!(with(&int(0), &int(3)), DataValue::string("mo"));
        assert_eq!(with(&int(-2), &int(5)), DataValue::string("mo"));
        assert_eq!(with(&int(3), &int(-1)), DataValue::string(""));
        assert_eq!(with(&int(3), &int(i64::MAX)), DataValue::string("tor car"));
        assert_eq!(with(&int(i64::MIN), &int(i64::MAX)), DataValue::string(""));
        // Positions are integers of any size, as in SPARQL's signature,
        // and the end of the substring is computed exactly.
        let (vast, minus_vast) = (
            number("100000000000000000000"),
            number("-100000000000000000000"),
        );
        assert_eq!(from(&vast), DataValue::string(""));
        assert_eq!(from(&minus_vast), subject);
        assert_eq!(with(&int(3), &vast), DataValue::string("tor car"));
        assert_eq!(with(&int(3), &minus_vast), DataValue::string(""));
        assert_eq!(
            with(&minus_vast, &number("100000000000000000003")),
            DataValue::string("mo")
        );
        let decimal = number("1.9");
        assert_eq!(
            substring(subject.as_ref(), decimal.as_ref()).as_result(),
            Err(EvaluationError::TypeMismatch)
        );
        assert_eq!(
            substring(subject.as_ref(), DataValue::string("1").as_ref()).as_result(),
            Err(EvaluationError::TypeMismatch)
        );
    }

    /// A part of a tagged string keeps the tag, unless the second string does not occur,
    /// and a second string with another tag is malformed.
    #[test]
    fn parts_keep_the_subjects_tag() {
        let subject = english("Hello, world");
        let plain = DataValue::string(", ");
        let part = |result: StackValue<'_>| {
            let part = value(result);
            let tag = tagged(&part).expect("a part of a tagged string is tagged");
            (tag.text().to_owned(), tag.language().to_owned())
        };
        let en = |text: &str| (text.to_owned(), String::from("en"));

        assert_eq!(part(before(subject.as_ref(), plain.as_ref())), en("Hello"));
        assert_eq!(part(after(subject.as_ref(), plain.as_ref())), en("world"));
        // A second string that does not occur gives a plain empty string,
        // while the empty string occurs at the start and gives a tagged part.
        assert_eq!(
            value(after(subject.as_ref(), DataValue::string("x").as_ref())),
            DataValue::string("")
        );
        assert_eq!(
            part(before(subject.as_ref(), DataValue::string("").as_ref())),
            en("")
        );
        assert_eq!(
            part(substring(subject.as_ref(), DataValue::integer(8).as_ref())),
            en("world")
        );
        assert_eq!(part(reverse(english("ab").as_ref())), en("ba"));
        assert_eq!(part(trim(english("  a ").as_ref(), str::trim)), en("a"));

        // A second string with another tag is incompatible, as for the predicates.
        let german = DataValue::language_string(", ", "de").expect("the tag is nonempty");
        assert_eq!(
            before(subject.as_ref(), german.as_ref()).as_result(),
            Err(EvaluationError::Malformed)
        );
        assert_eq!(
            part(before(subject.as_ref(), english(", ").as_ref())),
            en("Hello")
        );
    }

    /// A concatenation keeps the tag that all its operands share, is plain otherwise,
    /// and fails for an operand that is not a string.
    #[test]
    fn concatenation_keeps_a_shared_tag_only() {
        let joined = |values: &[DataValue]| {
            let stack: Vec<StackValue<'_>> =
                values.iter().map(|value| value.as_ref().into()).collect();
            value(concat(&stack))
        };
        assert_eq!(joined(&[english("a"), english("b")]), english("ab"));
        assert_eq!(
            joined(&[english("a"), DataValue::string("b")]),
            DataValue::string("ab")
        );
        assert_eq!(
            joined(&[DataValue::string("a"), DataValue::string("b")]),
            DataValue::string("ab")
        );
        assert_eq!(joined(&[]), DataValue::string(""));
        assert_eq!(
            concat(&[DataValue::integer(1).as_ref().into()]).as_result(),
            Err(EvaluationError::TypeMismatch)
        );
    }

    /// Reversal keeps grapheme clusters whole.
    #[test]
    fn reversal_keeps_grapheme_clusters_whole() {
        let reversed = |text: &str| value(reverse(DataValue::string(text).as_ref()));

        // A combining accent, and a flag of two regional indicators.
        assert_eq!(reversed("cafe\u{301}!"), DataValue::string("!e\u{301}fac"));
        assert_eq!(
            reversed("a\u{1F1E9}\u{1F1EA}b"),
            DataValue::string("b\u{1F1E9}\u{1F1EA}a")
        );
    }

    /// Percent-decoding inverts percent-encoding,
    /// and a malformed escape or a byte sequence that is not UTF-8 is malformed.
    #[test]
    fn percent_decoding_inverts_encoding() {
        let encoded = value(uri_encode(DataValue::string("a é/").as_ref()));
        assert_eq!(encoded, DataValue::string("a%20%C3%A9%2F"));
        assert_eq!(
            value(uri_decode(encoded.as_ref())),
            DataValue::string("a é/")
        );

        // An escape is exactly two hex digits, and the bytes must be UTF-8.
        for malformed in ["%+A", "%4", "%4G", "%é0", "%C3"] {
            assert_eq!(
                uri_decode(DataValue::string(malformed).as_ref()).as_result(),
                Err(EvaluationError::Malformed)
            );
        }
    }
}
