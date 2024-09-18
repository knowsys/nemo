//! This module defines data structures that mark spans of text in an input file.

use std::{ops::Range, path::Display};

use nom::InputIter;
use nom_locate::LocatedSpan;
use serde::de::Expected;

/// Locates a certain character within a file,
/// giving its offset, line and column number
#[derive(Debug, Default, Clone, Copy, Eq)]
pub struct CharacterPosition {
    /// Index of the character in the source file
    pub offset: usize,
    /// Line where the character occurs (starting with 1)
    pub line: u32,
    /// Column where the character occurs (starting with 1)
    pub column: u32,
}

impl CharacterPosition {
    /// Return a one character range at this position
    pub fn range(&self) -> Range<usize> {
        self.offset..(self.offset + 1)
    }
}

impl PartialEq for CharacterPosition {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl PartialOrd for CharacterPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CharacterPosition {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset.cmp(&other.offset)
    }
}

/// Describes a region of text with [CharacterPosition]s
#[derive(Debug, Clone, Copy)]
pub struct CharacterRange {
    /// Start position
    pub start: CharacterPosition,
    /// End position
    pub end: CharacterPosition,
}

impl CharacterRange {
    /// Return this information as a [Range].
    pub fn range(&self) -> Range<usize> {
        self.start.offset..self.end.offset
    }
}

/// Maker for a region of text within a string slice
#[derive(Debug, Clone, Copy)]
pub struct Span<'a>(LocatedSpan<&'a str>);

impl<'a> Span<'a> {
    pub fn new(inner: &'a str) -> Span<'a> {
        Span(LocatedSpan::new(inner))
    }

    /// Compute the [CharacterRange] for this region of text.
    pub fn range(&self) -> CharacterRange {
        let start = CharacterPosition {
            offset: self.0.location_offset(),
            line: self.0.location_line(),
            column: u32::try_from(self.0.get_utf8_column())
                .expect("cannot convert column number to u32"),
        };

        let end_offset = start.offset + self.0.fragment().len();
        let end_line = start.line
            + u32::try_from(self.0.fragment().lines().count() - 1)
                .expect("cannot convert line number to u32");
        let end_column = if self.0.fragment().lines().count() > 1 {
            u32::try_from(
                1 + self
                    .0
                    .fragment()
                    .lines()
                    .last()
                    .expect("there is at least one line")
                    .len(),
            )
            .expect("cannot convert column number to u32")
        } else {
            start.column
                + u32::try_from(self.0.fragment().len()).expect("cannot convert text range to u32")
        };

        let end = CharacterPosition {
            offset: end_offset,
            line: end_line,
            column: end_column,
        };

        CharacterRange { start, end }
    }

    /// TODO: Description and Specify safety conditions
    pub fn until_rest(&self, rest: &Self) -> Self {
        unsafe {
            Self(LocatedSpan::new_from_raw_offset(
                self.0.location_offset(),
                self.0.location_line(),
                &self.0[..(rest.0.location_offset() - self.0.location_offset())],
                (),
            ))
        }
    }

    /// Create a [Span] that encloses the given [Span]s.
    /// TODO: Description and Specify safety conditions and verify that this is correct
    pub fn enclose(&self, first: &Self, second: &Self) -> Self {
        unsafe {
            let slice_length =
                second.0.location_offset() + second.0.len() - first.0.location_offset();
            let slice_beginning = first.0.location_offset() - self.0.location_offset();

            Self(LocatedSpan::new_from_raw_offset(
                first.0.location_offset(),
                first.0.location_line(),
                &self.0[slice_beginning..(slice_beginning + slice_length)],
                (),
            ))
        }
    }

    /// Return a [Span] that points to the beginning.
    pub fn beginning(&self) -> Self {
        unsafe {
            if self.0.is_empty() {
                *self
            } else {
                Self(LocatedSpan::new_from_raw_offset(
                    self.0.location_offset(),
                    self.0.location_line(),
                    &self.0[0..1],
                    (),
                ))
            }
        }
    }

    /// Return an empty [Span] that points to the beginning.
    pub fn empty(&self) -> Self {
        unsafe {
            if self.0.is_empty() {
                *self
            } else {
                Self(LocatedSpan::new_from_raw_offset(
                    self.0.location_offset(),
                    self.0.location_line(),
                    &self.0[0..0],
                    (),
                ))
            }
        }
    }

    pub fn location_offset(&self) -> usize {
        self.0.location_offset()
    }

    pub fn location_line(&self) -> u32 {
        self.0.location_line()
    }

    pub fn get_utf8_column(&self) -> usize {
        self.0.get_utf8_column()
    }

    pub fn fragment(&self) -> &'_ str {
        self.0.fragment()
    }
}

impl<'a> std::fmt::Display for Span<'a> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<'a, R> nom::Slice<R> for Span<'a>
where
    &'a str: nom::Slice<R>,
{
    fn slice(&self, range: R) -> Self {
        Span(self.0.slice(range))
    }
}

impl nom_greedyerror::Position for Span<'_> {
    fn position(&self) -> usize {
        nom_greedyerror::Position::position(&self.0)
    }
}

impl nom::Offset for Span<'_> {
    fn offset(&self, second: &Self) -> usize {
        self.0.offset(&second.0)
    }
}

impl<'a> InputIter for Span<'a> {
    type Item = char;
    type Iter = <&'a str as InputIter>::Iter;
    type IterElem = <&'a str as InputIter>::IterElem;

    fn iter_indices(&self) -> Self::Iter {
        self.0.iter_indices()
    }

    fn iter_elements(&self) -> Self::IterElem {
        self.0.iter_elements()
    }

    fn position<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Item) -> bool,
    {
        self.0.position(predicate)
    }

    fn slice_index(&self, count: usize) -> Result<usize, nom::Needed> {
        self.0.slice_index(count)
    }
}

impl nom::InputTake for Span<'_> {
    fn take(&self, count: usize) -> Self {
        Self(self.0.take(count))
    }

    fn take_split(&self, count: usize) -> (Self, Self) {
        let (left, right) = self.0.take_split(count);
        (Self(left), Self(right))
    }
}

impl nom::InputLength for Span<'_> {
    fn input_len(&self) -> usize {
        self.0.input_len()
    }
}

impl nom::FindSubstring<&'_ str> for Span<'_> {
    fn find_substring(&self, substr: &str) -> Option<usize> {
        self.0.find_substring(substr)
    }
}

impl<'a> nom::Compare<Span<'a>> for Span<'a> {
    fn compare(&self, t: Span) -> nom::CompareResult {
        self.0.compare(t.fragment().as_bytes())
    }

    fn compare_no_case(&self, t: Span) -> nom::CompareResult {
        self.0.compare_no_case(t.fragment().as_bytes())
    }
}

impl<'a> nom::Compare<&str> for Span<'a> {
    fn compare(&self, t: &str) -> nom::CompareResult {
        self.0.compare(t)
    }

    fn compare_no_case(&self, t: &str) -> nom::CompareResult {
        self.0.compare_no_case(t)
    }
}
