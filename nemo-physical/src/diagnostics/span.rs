//! Locations in source texts.

use std::{fmt, sync::Arc};

/// The id of one source text that a diagnostic may point into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileId(u32);

impl FileId {
    /// Creates the id with the given index.
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    /// Returns the index of the id.
    pub fn index(self) -> u32 {
        self.0
    }
}

/// A range of one source text, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Span {
    /// The text that the range lies in.
    pub file: FileId,
    /// The offset of the first byte of the range.
    pub start: u32,
    /// The offset one past the last byte of the range.
    pub end: u32,
}

impl Span {
    /// Returns the 1-based line and column at which the range starts in the given text.
    ///
    /// The column counts characters, as an editor does.
    /// A start past the end of the text is taken as the end.
    pub fn line_column(&self, text: &str) -> (u32, u32) {
        self.line_column_in(text, &Self::line_starts(text))
    }

    /// Returns the line and column at which the range starts in the given text,
    /// as [line_column](Self::line_column) does.
    ///
    /// `line_starts` must be the [line starts](Self::line_starts) of `text`,
    /// so that many ranges of one text can share them.
    pub fn line_column_in(&self, text: &str, line_starts: &[usize]) -> (u32, u32) {
        let start = (self.start as usize).min(text.len());
        let start = (0..=start)
            .rev()
            .find(|&index| text.is_char_boundary(index))
            .unwrap_or(0);
        let line = line_starts.partition_point(|&line_start| line_start <= start);
        let column = text[line_starts[line - 1]..start].chars().count() + 1;
        let saturate = |count: usize| u32::try_from(count).unwrap_or(u32::MAX);
        (saturate(line), saturate(column))
    }

    /// Returns the byte offsets at which the lines of a text start,
    /// 0 and the offset after every line break.
    pub fn line_starts(text: &str) -> Vec<usize> {
        std::iter::once(0)
            .chain(text.match_indices('\n').map(|(index, _)| index + 1))
            .collect()
    }
}

/// The place at which a range of a source text starts:
/// the name of the text, a line and a column.
///
/// It is shown as `name:line:column`, or as `line:column` for a text without a name.
///
/// # Example
///
/// ```
/// use nemo_physical::diagnostics::Location;
///
/// let location = Location {
///     source: Some("people.rls".into()),
///     line: 3,
///     column: 5,
/// };
/// assert_eq!(location.to_string(), "people.rls:3:5");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Location {
    /// The name of the source text, such as its path, or `None` for a text without a name.
    pub source: Option<Arc<str>>,
    /// The 1-based line.
    pub line: u32,
    /// The 1-based column, which counts characters.
    pub column: u32,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(source) = &self.source {
            write!(f, "{source}:")?;
        }
        write!(f, "{}:{}", self.line, self.column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lines and columns count from 1, columns count characters,
    /// and a start past the end of the text is taken as the end.
    #[test]
    fn lines_and_columns_count_characters_from_one() {
        let text = "p(a) .\nq(\"ä\", b) .";
        let at = |start| Span {
            file: FileId::new(0),
            start,
            end: start,
        };
        assert_eq!(at(0).line_column(text), (1, 1));
        assert_eq!(at(7).line_column(text), (2, 1));
        // `b` follows the two-byte `ä`, and stands in column 8 at byte 15.
        assert_eq!(at(15).line_column(text), (2, 8));
        assert_eq!(at(999).line_column(text), (2, 12));
    }
}
