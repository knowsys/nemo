//! This module defines tags used to name complex syntax elements.

use std::cmp::Ordering;

use nom::InputTake;

use crate::parser::{
    ParserResult,
    ast::token::{is_name_continue, is_name_start},
    error::ParserErrors,
    input::ParserInput,
};

pub mod aggregation;
pub mod datatype;
pub mod operation;
pub mod parameter;
pub mod structure;

/// A table of keywords, sorted by name so that it can be searched.
#[derive(Debug)]
pub(crate) struct KeywordTable<Kind> {
    /// Keywords and what they stand for, sorted by [Self::compare]
    keywords: Vec<(&'static str, Kind)>,
    /// How two names are compared
    compare: fn(&str, &str) -> Ordering,
}

impl<Kind> KeywordTable<Kind>
where
    Kind: Copy,
{
    /// Create a [KeywordTable] whose names are matched case-sensitively.
    pub(crate) fn new(keywords: impl IntoIterator<Item = (&'static str, Kind)>) -> Self {
        Self::sorted(keywords, |left, right| left.cmp(right))
    }

    /// Create a [KeywordTable] whose names are matched ignoring ASCII case.
    pub(crate) fn new_ignore_ascii_case(
        keywords: impl IntoIterator<Item = (&'static str, Kind)>,
    ) -> Self {
        Self::sorted(keywords, Self::compare_ignore_ascii_case)
    }

    /// Create a [KeywordTable] sorted by the given comparison.
    fn sorted(
        keywords: impl IntoIterator<Item = (&'static str, Kind)>,
        compare: fn(&str, &str) -> Ordering,
    ) -> Self {
        let mut keywords = keywords.into_iter().collect::<Vec<_>>();
        keywords.sort_unstable_by(|(left, _), (right, _)| compare(left, right));

        Self { keywords, compare }
    }

    /// Return the keyword with the given name, if there is one.
    fn get(&self, name: &str) -> Option<Kind> {
        self.keywords
            .binary_search_by(|(keyword, _)| (self.compare)(keyword, name))
            .ok()
            .map(|index| self.keywords[index].1)
    }

    /// Compare two strings, ignoring the case of ASCII characters.
    fn compare_ignore_ascii_case(left: &str, right: &str) -> Ordering {
        left.as_bytes()
            .iter()
            .map(u8::to_ascii_lowercase)
            .cmp(right.as_bytes().iter().map(u8::to_ascii_lowercase))
    }
}

/// Return the length in bytes of the identifier at the start of `input`,
/// or `None` if `input` does not start with one.
fn identifier_length(input: &str) -> Option<usize> {
    let mut characters = input.char_indices();

    let (_, first) = characters.next()?;
    if !is_name_start(first) {
        return None;
    }

    let length = characters
        .find(|(_, character)| !is_name_continue(*character))
        .map_or(input.len(), |(index, _)| index);

    Some(length)
}

/// Parse a keyword by scanning a single identifier and looking it up in `table`.
fn parse_keyword<'a, Kind>(
    input: ParserInput<'a>,
    table: &KeywordTable<Kind>,
) -> ParserResult<'a, Kind>
where
    Kind: Copy,
{
    let error = || nom::Err::Error(ParserErrors::at(input.span));

    let fragment = input.span.fragment();
    let Some(length) = identifier_length(fragment) else {
        return Err(error());
    };

    let Some(kind) = table.get(&fragment[..length]) else {
        return Err(error());
    };

    let (rest, _) = input.take_split(length);
    Ok((rest, kind))
}

#[cfg(test)]
mod test {
    use super::{KeywordTable, identifier_length};

    #[test]
    fn identifier_lengths() {
        assert_eq!(identifier_length("SUM(?x)"), Some(3));
        assert_eq!(identifier_length("SUMX(?x)"), Some(4));
        assert_eq!(identifier_length("STRLEN(?x)"), Some(6));
        assert_eq!(identifier_length("str_len%(?x)"), Some(8));
        assert_eq!(identifier_length("int, b"), Some(3));
        assert_eq!(identifier_length("a"), Some(1));

        assert_eq!(identifier_length("?x"), None);
        assert_eq!(identifier_length("12"), None);
        assert_eq!(identifier_length("("), None);
        assert_eq!(identifier_length(""), None);
        assert_eq!(identifier_length("_blank"), None);

        // Non-ASCII letters are part of an identifier, so a keyword followed by
        // one does not make up a keyword on its own.
        assert_eq!(identifier_length("SUMÄ(?x)"), Some(5));
    }

    #[test]
    fn keyword_lookup() {
        let table = KeywordTable::new_ignore_ascii_case([
            ("STR", 1),
            ("STRLEN", 2),
            ("NUMGREATER", 3),
            ("NUMGREATEREQ", 4),
        ]);

        // Names that are prefixes of other names still resolve to themselves,
        // in either case.
        assert_eq!(table.get("STR"), Some(1));
        assert_eq!(table.get("str"), Some(1));
        assert_eq!(table.get("StrLen"), Some(2));
        assert_eq!(table.get("NUMGREATER"), Some(3));
        assert_eq!(table.get("numgreatereq"), Some(4));

        // A name is never recognized from a prefix of it alone.
        assert_eq!(table.get("STRX"), None);
        assert_eq!(table.get("NUMGREATERE"), None);
        assert_eq!(table.get(""), None);

        // Case-sensitive tables reject a differently-cased name.
        let table = KeywordTable::new([("int", 1), ("string", 2)]);
        assert_eq!(table.get("int"), Some(1));
        assert_eq!(table.get("INT"), None);
    }
}
