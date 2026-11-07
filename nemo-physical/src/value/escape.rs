//! Escape sequences of term syntax.

/// An escape sequence of term syntax, read after its backslash:
/// the character it encodes, and its length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Escape {
    /// The character the escape encodes.
    pub character: char,
    /// The length in bytes of the escape after its backslash.
    pub length: usize,
}

impl Escape {
    /// Reads the escape of a quoted string at the start of a text, the text after its backslash:
    /// `\\`, `\"`, `\'`, `\n`, `\t`, `\r`, `\f`, and the character escapes `\uXXXX` and
    /// `\UXXXXXXXX` of Turtle and SPARQL.
    ///
    /// Returns `None` for anything else, whose backslash stays as written,
    /// so that `"\d+"` reaches a regular expression as written.
    /// So does `\b`, a word boundary in a regular expression, which Turtle reads as a backspace.
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

    /// Reads the character escape `\uXXXX` or `\UXXXXXXXX` at the start of a text,
    /// the text after its backslash: the one escape of IRIs.
    ///
    /// Returns `None` for anything else,
    /// including a `\u` without the hexadecimal digits of a character.
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

        // An IRI knows only the character escapes.
        assert_eq!(Escape::in_iri("n"), None);
        assert_eq!(
            Escape::in_iri("u003E").map(|escape| escape.character),
            Some('>')
        );
    }
}
