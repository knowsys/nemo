//! Escape sequences of term syntax.

/// A decoded escape sequence of term syntax.
///
/// An escape sequence is a backslash followed by the characters that encode one character.
/// The functions here read the text that follows the backslash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Escape {
    /// The character that the escape sequence encodes.
    pub character: char,
    /// The length in bytes of the escape sequence, without its backslash.
    pub length: usize,
}

impl Escape {
    /// Reads an escape sequence of a quoted string from the start of `after`,
    /// the text that follows the backslash.
    ///
    /// The escape sequences are `\\`, `\"`, `\'`, `\n`, `\t`, `\r`, `\f`,
    /// and the character escapes `\uXXXX` and `\UXXXXXXXX` of Turtle and SPARQL.
    ///
    /// Returns `None` for any other text, whose backslash then stands for itself.
    /// This keeps a pattern such as `"\d+"` intact for a regular expression.
    /// It also holds for `\b`, which Turtle reads as a backspace,
    /// so that it stays a word boundary in a regular expression.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::Escape;
    ///
    /// let character = |after| Escape::in_string(after).map(|escape| escape.character);
    /// assert_eq!(character("n and more"), Some('\n'));
    /// assert_eq!(character("u00E9"), Some('é'));
    /// assert_eq!(character("d+"), None);
    /// assert_eq!(character("b"), None);
    /// ```
    pub fn in_string(after: &str) -> Option<Self> {
        let character = match after.chars().next()? {
            '\\' => '\\',
            '"' => '"',
            '\'' => '\'',
            'n' => '\n',
            't' => '\t',
            'r' => '\r',
            'f' => '\u{C}',
            _ => return Self::in_iri(after),
        };
        Some(Self {
            character,
            length: 1,
        })
    }

    /// Reads a character escape `\uXXXX` or `\UXXXXXXXX` from the start of `after`,
    /// the text that follows the backslash.
    ///
    /// These are the only escape sequences of an IRI.
    /// Returns `None` for any other text,
    /// including a `\u` or `\U` whose hexadecimal digits are missing or encode no character.
    pub fn in_iri(after: &str) -> Option<Self> {
        let width = match after.as_bytes().first()? {
            b'u' => 4,
            b'U' => 8,
            _ => return None,
        };
        let digits = after.get(1..1 + width)?;
        if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let character = char::from_u32(u32::from_str_radix(digits, 16).ok()?)?;
        Some(Self {
            character,
            length: 1 + width,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each escape sequence is read as its character and length,
    /// and an incomplete or unknown one is not read at all.
    #[test]
    fn escapes_read_their_characters_and_lengths() {
        let read = |after| Escape::in_string(after).map(|escape| (escape.character, escape.length));
        assert_eq!(read("\\x"), Some(('\\', 1)));
        assert_eq!(read("'"), Some(('\'', 1)));
        assert_eq!(read("f"), Some(('\u{C}', 1)));
        assert_eq!(read("U0001F600!"), Some(('😀', 9)));
        assert_eq!(read("u00e"), None, "too few digits");
        assert_eq!(read("uD800"), None, "a surrogate names no character");
        assert_eq!(read("b"), None);
        assert_eq!(read(""), None);

        // An IRI has only the character escapes.
        assert_eq!(Escape::in_iri("n"), None);
        assert_eq!(
            Escape::in_iri("u003E").map(|escape| escape.character),
            Some('>')
        );
    }
}
