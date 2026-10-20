//! Positions: byte offsets against the line and character the protocol counts in.
//!
//! Spans carry byte offsets.
//! The protocol carries a line and a character *counted in UTF-16 code units*,
//! which is neither bytes nor characters and matches neither for anything outside ASCII.
//! One index converts both ways, and every conversion in the server goes through it.
//!
//! A line's terminator is not part of it, since the protocol addresses what a line contains.
//! So a position never falls between a `\r` and its `\n`,
//! and an offset that does is reported at the end of the content it follows.

/// Where each line of a text begins, and what a position means in it.
#[derive(Debug, Clone)]
pub(crate) struct Lines {
    /// Byte offset of each line's first character, the first being 0.
    starts: Vec<usize>,
}

impl Lines {
    /// Indexes a text.
    ///
    /// A line ends at `\n`, at `\r\n`, or at a `\r` on its own,
    /// the three conventions a file can arrive in.
    pub(crate) fn of(text: &str) -> Self {
        let bytes = text.as_bytes();
        let mut starts = vec![0];
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'\n' => starts.push(index + 1),
                b'\r' => {
                    if bytes.get(index + 1) == Some(&b'\n') {
                        starts.push(index + 2);
                        index += 1;
                    } else {
                        starts.push(index + 1);
                    }
                }
                _ => {}
            }
            index += 1;
        }
        Self { starts }
    }

    /// Returns the line and UTF-16 character a byte offset sits at.
    ///
    /// An offset past the end of the text sits at its end, one inside a
    /// character sits at that character's start, and one inside a line
    /// terminator sits at the end of the line it terminates: a position
    /// is somewhere the editor can put a cursor, always.
    pub(crate) fn position(&self, text: &str, offset: usize) -> (u32, u32) {
        let mut offset = offset.min(text.len());
        while !text.is_char_boundary(offset) {
            offset -= 1;
        }
        let line = self
            .starts
            .partition_point(|&start| start <= offset)
            .saturating_sub(1);
        let start = self.starts[line];
        let bounded = offset.min(self.end(text, line));
        let prefix = text.get(start..bounded).unwrap_or("");
        let character: usize = prefix.chars().map(char::len_utf16).sum();
        (narrow(line), narrow(character))
    }

    /// Returns the byte offset a line and UTF-16 character addresses.
    ///
    /// A position past the end of its line lands at the end of the line's *content*,
    /// and a line past the end of the text at the text's end,
    /// since the protocol lets a client ask about either.
    pub(crate) fn offset(&self, text: &str, line: u32, character: u32) -> usize {
        let Some(&start) = self.starts.get(line as usize) else {
            return text.len();
        };
        let end = self.end(text, line as usize);
        let mut remaining = character as usize;
        let mut offset = start;
        for symbol in text.get(start..end).unwrap_or("").chars() {
            if remaining == 0 {
                break;
            }
            remaining = remaining.saturating_sub(symbol.len_utf16());
            offset += symbol.len_utf8();
        }
        offset
    }

    /// Returns the offset at which a line's content ends,
    /// before the terminator that starts the next one.
    fn end(&self, text: &str, line: usize) -> usize {
        let start = self.starts.get(line).copied().unwrap_or(text.len());
        let mut end = self
            .starts
            .get(line + 1)
            .copied()
            .unwrap_or(text.len())
            .min(text.len());
        let bytes = text.as_bytes();
        if end > start && bytes[end - 1] == b'\n' {
            end -= 1;
        }
        if end > start && bytes[end - 1] == b'\r' {
            end -= 1;
        }
        end
    }
}

/// Returns a count as the protocol carries it.
///
/// Anything a 32-bit position cannot hold is clamped rather than wrapped.
fn narrow(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Texts covering what a source file can be made of: multi-byte and
    /// astral characters, every line ending, and every way a file can
    /// end.
    const CORPUS: &[&str] = &[
        "",
        "\n",
        "\r\n",
        "\r",
        "p(1) .",
        "p(1) .\n",
        "p(1) .\r\nq(2) .\r\n",
        "p(1) .\rq(2) .\r",
        "p(\"ä\") .\nq(\"ö\") .",
        "p(\"𝄞\") .\n",
        "p(\"😀🇩🇪\") .\nq(\"a\u{200d}b\") .\n",
        "\u{feff}p(1) .\n",
        "p(\"ä𝄞ö\") .\nq(?x) :- p(?x) .\n",
        "\n\n\n",
        "p(1) .\n\n\nq(2) .",
        "𝄞",
    ];

    /// Whether an offset sits inside a line terminator, which is not a
    /// place the protocol can name.
    fn inside_terminator(text: &str, offset: usize) -> bool {
        let bytes = text.as_bytes();
        if offset == 0 || offset >= bytes.len() {
            return false;
        }
        bytes[offset] == b'\n' && bytes[offset - 1] == b'\r'
    }

    #[test]
    fn offsets_and_positions_are_inverse() {
        for text in CORPUS {
            let lines = Lines::of(text);
            for offset in 0..=text.len() {
                if !text.is_char_boundary(offset) || inside_terminator(text, offset) {
                    continue;
                }
                let (line, character) = lines.position(text, offset);
                assert_eq!(
                    lines.offset(text, line, character),
                    offset,
                    "at {offset} of {text:?}"
                );
            }
        }
    }

    #[test]
    fn characters_are_counted_as_the_protocol_counts_them() {
        // The reference the protocol names: UTF-16 code units of the
        // text before the offset, on its line.
        for text in CORPUS {
            let lines = Lines::of(text);
            for offset in 0..=text.len() {
                if !text.is_char_boundary(offset) || inside_terminator(text, offset) {
                    continue;
                }
                let (line, character) = lines.position(text, offset);
                let start = lines.starts[line as usize];
                let expected: usize = text[start..offset].encode_utf16().count();
                assert_eq!(character as usize, expected, "at {offset} of {text:?}");
            }
        }
    }

    #[test]
    fn an_astral_character_is_two_units_and_four_bytes() {
        // `ä` is one UTF-16 unit and two bytes; `𝄞` is two units and
        // four bytes, which is where counting characters would go wrong.
        let text = "p(\"ä𝄞\") .";
        let lines = Lines::of(text);
        let offset = text.find('𝄞').expect("the text holds it");
        assert_eq!(lines.position(text, offset), (0, 4));
        assert_eq!(lines.offset(text, 0, 4), offset);
        // Past the clef: two units further along, not one.
        assert_eq!(lines.position(text, offset + '𝄞'.len_utf8()), (0, 6));
    }

    #[test]
    fn an_offset_inside_a_character_reports_where_it_starts() {
        let text = "p(\"𝄞\") .";
        let lines = Lines::of(text);
        let start = text.find('𝄞').expect("the text holds it");
        for inside in 1..'𝄞'.len_utf8() {
            assert_eq!(
                lines.position(text, start + inside),
                lines.position(text, start),
                "{inside} bytes into the clef"
            );
        }
        // And a position inside the pair addresses the character after.
        assert_eq!(lines.offset(text, 0, 4), start + '𝄞'.len_utf8());
    }

    #[test]
    fn a_line_ends_before_its_terminator() {
        let text = "abc\r\ndef\r\n";
        let lines = Lines::of(text);
        // The line holds three characters; asking past them stops there.
        assert_eq!(lines.offset(text, 0, 3), 3);
        assert_eq!(lines.offset(text, 0, 4), 3, "the `\\r` is not content");
        assert_eq!(lines.offset(text, 0, 999), 3);
        assert_eq!(lines.position(text, 3), (0, 3));
        assert_eq!(lines.position(text, 5), (1, 0), "the second line starts");
    }

    #[test]
    fn positions_past_the_end_stay_inside_the_text() {
        let text = "p(1) .\n";
        let lines = Lines::of(text);
        assert_eq!(lines.offset(text, 99, 0), text.len());
        assert_eq!(
            lines.offset(text, 0, 99),
            6,
            "the line ends before its break"
        );
        assert_eq!(lines.position(text, 999), (1, 0));
    }

    #[test]
    fn every_convention_starts_a_line() {
        for (text, breaks) in [("a\nb\nc", 3), ("a\r\nb\r\nc", 3), ("a\rb\rc", 3)] {
            let lines = Lines::of(text);
            assert_eq!(lines.starts.len(), breaks, "{text:?}");
            let last = text.rfind('c').expect("the text ends in c");
            assert_eq!(lines.position(text, last), (2, 0), "{text:?}");
        }
    }
}
