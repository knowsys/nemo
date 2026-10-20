//! Locations in source texts.

/// Identifies one source text that a diagnostic may point into.
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
    /// The text the range lies in.
    pub file: FileId,
    /// The offset of the first byte of the range.
    pub start: u32,
    /// The offset one past the last byte of the range.
    pub end: u32,
}

impl Span {
    /// Returns where the range starts in the given text, as an editor
    /// counts: the 1-based line and the 1-based column in characters.
    /// A start past the text's end counts from the end.
    pub fn line_column(&self, text: &str) -> (u32, u32) {
        let start = (self.start as usize).min(text.len());
        let start = (0..=start)
            .rev()
            .find(|&index| text.is_char_boundary(index))
            .unwrap_or(0);
        let before = &text[..start];
        let line = before.matches('\n').count() + 1;
        let column = before
            .rsplit_once('\n')
            .map_or(before, |(_, tail)| tail)
            .chars()
            .count()
            + 1;
        let saturate = |count: usize| u32::try_from(count).unwrap_or(u32::MAX);
        (saturate(line), saturate(column))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
