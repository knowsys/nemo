//! The names of term syntax, which are written without quotes or brackets.

/// A kind of name in term syntax.
///
/// The kinds differ in what may start a name and in what may join two parts of one,
/// which depends on what precedes the name.
/// A name starts with a letter or `_`, or also with a digit if it is [Local](Self::Local).
/// It continues with letters, digits, `_` and `%`.
/// The letters are those of Turtle's and SPARQL's names, which go beyond ASCII.
/// A name may also continue with the marks of those names, such as combining accents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameKind {
    /// A name that follows a marker character, such as `?` or `@`.
    ///
    /// No character joins two parts, so the name at the start of `x-1` is `x`.
    Marked,
    /// A name without a marker, such as `alice` or `my-lib`.
    ///
    /// A `-` joins two parts.
    Bare,
    /// A local name after a prefix and its `:`, or a blank node label after `_:`.
    ///
    /// Examples are the `42` of `wd:42`, the `v1.2` of `ex:v1.2` and the `b.1` of `_:b.1`.
    /// As in Turtle and SPARQL, such a name may also start with a digit,
    /// and a `-` or a `.` joins two parts.
    /// So the name never ends with a `-` or a `.`.
    Local,
}

impl NameKind {
    /// Returns the length in bytes of the longest name of this kind at the start of a text,
    /// or 0 if the text does not start with one.
    ///
    /// A `-` or `.` joins two parts only if a letter, a digit, `_` or a mark follows it.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::NameKind;
    ///
    /// assert_eq!(NameKind::Bare.length("my-lib::tc"), 6);
    /// assert_eq!(NameKind::Marked.length("x-1"), 1);
    /// assert_eq!(NameKind::Local.length("v1.2 ."), 4);
    /// assert_eq!(NameKind::Local.length("a. b"), 1);
    /// assert_eq!(NameKind::Bare.length("42"), 0);
    /// assert_eq!(NameKind::Bare.length("café"), 5);
    /// ```
    pub fn length(self, text: &str) -> usize {
        let mut end = 0;
        let mut characters = text.char_indices().peekable();
        while let Some((index, character)) = characters.next() {
            let taken = if index == 0 {
                self.starts(character)
            } else if Self::continues(character) {
                true
            } else {
                self.joins(character)
                    && characters
                        .peek()
                        .is_some_and(|&(_, next)| next != '%' && Self::continues(next))
            };
            if !taken {
                break;
            }
            end = index + character.len_utf8();
        }
        end
    }

    /// Returns whether a text is exactly one name of this kind.
    pub fn admits(self, text: &str) -> bool {
        !text.is_empty() && self.length(text) == text.len()
    }

    /// Returns whether a character can continue a name of any kind.
    ///
    /// These characters are the letters, the digits, `_`, `%`, and the marks of Turtle's names.
    /// A `%` is part of the name as written, and does not start an escape sequence.
    pub fn continues(character: char) -> bool {
        is_letter(character)
            || character.is_ascii_digit()
            || matches!(
                character,
                '_' | '%' | '\u{B7}' | '\u{300}'..='\u{36F}' | '\u{203F}'..='\u{2040}'
            )
    }

    /// Returns whether a character can start a name of this kind.
    fn starts(self, character: char) -> bool {
        is_letter(character)
            || character == '_'
            || (self == NameKind::Local && character.is_ascii_digit())
    }

    /// Returns whether a character can join two parts of a name of this kind.
    fn joins(self, character: char) -> bool {
        match self {
            NameKind::Marked => false,
            NameKind::Bare => character == '-',
            NameKind::Local => matches!(character, '-' | '.'),
        }
    }
}

/// Returns whether a character is a letter in Turtle's and SPARQL's names,
/// which their grammars call `PN_CHARS_BASE`.
fn is_letter(character: char) -> bool {
    matches!(
        character,
        'A'..='Z'
            | 'a'..='z'
            | '\u{C0}'..='\u{D6}'
            | '\u{D8}'..='\u{F6}'
            | '\u{F8}'..='\u{2FF}'
            | '\u{370}'..='\u{37D}'
            | '\u{37F}'..='\u{1FFF}'
            | '\u{200C}'..='\u{200D}'
            | '\u{2070}'..='\u{218F}'
            | '\u{2C00}'..='\u{2FEF}'
            | '\u{3001}'..='\u{D7FF}'
            | '\u{F900}'..='\u{FDCF}'
            | '\u{FDF0}'..='\u{FFFD}'
            | '\u{10000}'..='\u{EFFFF}'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each kind finds the longest name of its own rules at the start of a text.
    ///
    /// Each case lists the lengths in bytes for `Marked`, `Bare` and `Local`.
    #[test]
    fn names_follow_their_kind() {
        let cases = [
            ("alice", [5, 5, 5]),
            ("my-lib", [2, 6, 6]),
            ("item-1", [4, 6, 6]),
            ("a-%b", [1, 1, 1]),
            ("42", [0, 0, 2]),
            ("4-2a", [0, 0, 4]),
            ("v1.2", [2, 2, 4]),
            ("a.b.c", [1, 1, 5]),
            ("a.", [1, 1, 1]),
            ("a.%x", [1, 1, 1]),
            ("a..b", [1, 1, 1]),
            ("x%20y", [5, 5, 5]),
            ("_", [1, 1, 1]),
            ("straße", [7, 7, 7]),
            ("東京", [6, 6, 6]),
            ("é-1", [2, 4, 4]),
            ("e\u{301}", [3, 3, 3]),
            ("\u{301}e", [0, 0, 0]),
            ("-a", [0, 0, 0]),
            ("", [0, 0, 0]),
        ];
        for (text, lengths) in cases {
            let found =
                [NameKind::Marked, NameKind::Bare, NameKind::Local].map(|kind| kind.length(text));
            assert_eq!(found, lengths, "{text:?}");
        }
    }

    /// A text is a name of a kind only if the whole text is one name of that kind.
    #[test]
    fn a_name_is_admitted_whole() {
        assert!(NameKind::Bare.admits("café-1"));
        assert!(!NameKind::Bare.admits("a.b"));
        assert!(NameKind::Local.admits("a.b"));
        assert!(!NameKind::Local.admits("a."));
        assert!(!NameKind::Local.admits(""));
        assert!(!NameKind::Marked.admits("x-1"));
    }
}
