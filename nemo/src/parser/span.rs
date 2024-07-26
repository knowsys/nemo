//! This module defines data structures that mark spans of text in an input file.

use std::ops::Range;

use nom_locate::LocatedSpan;

/// Locates a certain character within a file,
/// giving its offset, line and column number
#[derive(Debug, Clone, Copy, Eq)]
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

// TODO: Remove this once error is cleaned up
impl Default for CharacterPosition {
    fn default() -> Self {
        Self {
            offset: Default::default(),
            line: Default::default(),
            column: Default::default(),
        }
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
pub struct Span<'a>(pub(crate) LocatedSpan<&'a str>);

impl<'a> From<LocatedSpan<&'a str>> for Span<'a> {
    fn from(value: LocatedSpan<&'a str>) -> Self {
        Self(value)
    }
}

impl<'a> Span<'a> {
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

    /// Create a [ProgramSpan] that encloses the given [ProgramSpan]s.
    /// TODO: Description and Specify safety conditions and verify that this is correct
    pub fn enclose(&self, first: &Self, second: &Self) -> Self {
        unsafe {
            Self(LocatedSpan::new_from_raw_offset(
                first.0.location_offset(),
                first.0.location_line(),
                &self.0
                    [..(second.0.location_offset() + second.0.len() - first.0.location_offset())],
                (),
            ))
        }
    }
}
