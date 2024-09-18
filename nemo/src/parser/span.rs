//! This module defines data structures that mark spans of text in an input file.
//! The `Span` implementation is inspired by nom_locate. (See <https://github.com/fflorent/nom_locate>)

use core::str;
use std::ops::{Deref, Range};

use nom::{Offset, Slice};

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
pub struct Span<'a> {
    allocation_start: *const u8,
    fragment: &'a str,
    line: u32,
}

// SAFETY: Conceptionally, a span is just a slice inside a slice.
unsafe impl Sync for Span<'_> {}

impl<'a> Span<'a> {
    /// Create a span for a particular input with default offset and line values and empty extra data.
    /// You can compute the column through the get_column or get_utf8_column methods.
    ///
    /// offset starts at 0, line starts at 1, and column starts at 1.
    ///
    /// Do not use this constructor in parser functions;
    /// nom and nom_locate assume span offsets are relative to the beginning of the same input.
    /// In these cases, you probably want to use the nom::traits::Slice trait instead.
    pub fn new(inner: &'a str) -> Span<'a> {
        Span {
            allocation_start: inner.as_ptr(),
            fragment: inner,
            line: 1,
        }
    }

    /// Compute the [CharacterRange] for this region of text.
    pub fn range(&self) -> CharacterRange {
        let start = CharacterPosition {
            offset: self.location_offset(),
            line: self.line,
            column: u32::try_from(self.get_utf8_column())
                .expect("cannot convert column number to u32"),
        };

        let end_offset = start.offset + self.fragment.len();
        let end_line = start.line
            + u32::try_from(self.fragment.lines().count() - 1)
                .expect("cannot convert line number to u32");
        let end_column = if self.fragment.lines().count() > 1 {
            u32::try_from(
                1 + self
                    .fragment
                    .lines()
                    .last()
                    .expect("there is at least one line")
                    .len(),
            )
            .expect("cannot convert column number to u32")
        } else {
            start.column
                + u32::try_from(self.fragment.len()).expect("cannot convert text range to u32")
        };

        let end = CharacterPosition {
            offset: end_offset,
            line: end_line,
            column: end_column,
        };

        CharacterRange { start, end }
    }

    /// Extend this span up to (excluding) the first character of rest
    pub fn until_rest(&self, rest: &Self) -> Self {
        assert_eq!(self.allocation_start, rest.allocation_start);

        let start = self.location_offset();
        let end = rest.location_offset();

        // SAFETY: By the assertion above, self and rest are derived from the same allocation
        // because there is no safe way to create those spans otherwise.
        let fragment =
            unsafe { std::str::from_raw_parts(self.allocation_start.add(start), end - start) };

        Self {
            allocation_start: self.allocation_start,
            fragment,
            line: self.line,
        }
    }

    /// Create a [Span] that encloses the given [Span]s.
    pub fn enclose(first: &Self, second: &Self) -> Self {
        assert_eq!(first.allocation_start, second.allocation_start);

        let start = first.location_offset();
        let end = second.location_offset() + second.fragment.len();

        assert!(end >= start);

        // SAFETY: By the assertion above, self and rest are derived from the same allocation,
        // because there is no safe way to create them otherwise
        let fragment =
            unsafe { std::str::from_raw_parts(first.allocation_start.add(start), end - start) };

        Self {
            allocation_start: first.allocation_start,
            fragment,
            line: first.line,
        }
    }

    /// Return a [Span] that points to the beginning.
    pub fn beginning(&self) -> Self {
        if self.fragment.is_empty() {
            *self
        } else {
            Self {
                allocation_start: self.allocation_start,
                fragment: &self.fragment[0..1],
                line: self.line,
            }
        }
    }

    /// Return an empty [Span] that points to the beginning.
    pub fn empty(&self) -> Self {
        Self {
            allocation_start: self.allocation_start,
            fragment: &self.fragment[0..0],
            line: self.line,
        }
    }

    fn get_slice_before(&self) -> &str {
        // SAFETY: since the outer slice starts at self.allocation_start
        // everything from there to self.fragment is also a valid slice.
        unsafe { str::from_raw_parts(self.allocation_start, self.location_offset()) }
    }

    /// The offset represents the position of the fragment relatively to the input of the parser. It starts at offset 0.
    pub fn location_offset(&self) -> usize {
        // SAFETY: self.fragment.as_ptr() is greater then or equal to self.allocation start
        // and they are both derived from the same initial slice.
        unsafe { self.fragment.as_ptr().offset_from(self.allocation_start) as usize }
    }

    /// The line number of the fragment relatively to the input of the parser. It starts at line 1.
    pub fn location_line(&self) -> u32 {
        self.line
    }

    /// Return the column index for UTF8 text. Return value is unspecified for non-utf8 text.
    pub fn get_utf8_column(&self) -> usize {
        let slice_before = self.get_slice_before();
        let offset = slice_before.rfind('\n').map(|x| x + 1).unwrap_or(0);
        bytecount::num_chars(slice_before[offset..].as_bytes())
    }

    /// The fragment that is spanned. The fragment represents a part of the input of the parser.
    pub fn fragment(&self) -> &'_ str {
        self.fragment
    }
}

impl<'a> std::fmt::Display for Span<'a> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.fragment.fmt(formatter)
    }
}

impl Deref for Span<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.fragment
    }
}

impl<'a, R> Slice<R> for Span<'a>
where
    &'a str: Slice<R>,
{
    fn slice(&self, range: R) -> Self {
        let next_fragment = self.fragment.slice(range);
        let consumed = &self.fragment[..self.fragment.offset(next_fragment)];
        let line_offset: u32 = consumed
            .bytes()
            .filter(|b| *b == b'\n')
            .count()
            .try_into()
            .expect("line count overflowed u32");

        Span {
            allocation_start: self.allocation_start,
            fragment: next_fragment,
            line: self.line + line_offset,
        }
    }
}

impl nom_greedyerror::Position for Span<'_> {
    fn position(&self) -> usize {
        self.location_offset()
    }
}

impl Offset for Span<'_> {
    fn offset(&self, second: &Self) -> usize {
        assert_eq!(self.allocation_start, second.allocation_start);
        self.fragment.offset(second.fragment)
    }
}

impl<'a> nom::InputIter for Span<'a> {
    type Item = char;
    type Iter = <&'a str as nom::InputIter>::Iter;
    type IterElem = <&'a str as nom::InputIter>::IterElem;

    fn iter_indices(&self) -> Self::Iter {
        self.fragment.iter_indices()
    }

    fn iter_elements(&self) -> Self::IterElem {
        self.fragment.iter_elements()
    }

    fn position<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Item) -> bool,
    {
        self.fragment.position(predicate)
    }

    fn slice_index(&self, count: usize) -> Result<usize, nom::Needed> {
        self.fragment.slice_index(count)
    }
}

impl nom::InputTake for Span<'_> {
    fn take(&self, count: usize) -> Self {
        self.slice(..count)
    }

    fn take_split(&self, count: usize) -> (Self, Self) {
        (self.slice(count..), self.slice(..count))
    }
}

impl nom::InputLength for Span<'_> {
    fn input_len(&self) -> usize {
        self.fragment.input_len()
    }
}

impl nom::FindSubstring<&'_ str> for Span<'_> {
    fn find_substring(&self, substr: &str) -> Option<usize> {
        self.fragment.find_substring(substr)
    }
}

impl<'a> nom::Compare<Span<'a>> for Span<'a> {
    fn compare(&self, t: Span) -> nom::CompareResult {
        self.fragment.compare(t.fragment().as_bytes())
    }

    fn compare_no_case(&self, t: Span) -> nom::CompareResult {
        self.fragment.compare_no_case(t.fragment().as_bytes())
    }
}

impl<'a> nom::Compare<&str> for Span<'a> {
    fn compare(&self, t: &str) -> nom::CompareResult {
        self.fragment.compare(t)
    }

    fn compare_no_case(&self, t: &str) -> nom::CompareResult {
        self.fragment.compare_no_case(t)
    }
}
