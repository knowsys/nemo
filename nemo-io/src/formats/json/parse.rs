//! A parser of JSON texts that keeps each number as it is written.
//!
//! The spelling of a number decides what value it is read as,
//! and an integer may have any number of digits, so no number is converted while parsing.

use std::borrow::Cow;

/// The deepest nesting of arrays and objects that is parsed.
const MAX_NESTING: usize = 128;

/// A JSON value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Json<'a> {
    /// `null`.
    Null,
    /// `true` or `false`.
    Boolean(bool),
    /// A number, as it is written.
    Number(&'a str),
    /// A string, with its escapes resolved.
    String(Cow<'a, str>),
    /// An array.
    Array(Vec<Json<'a>>),
    /// An object, as its members in the order they are written.
    Object(Vec<(Cow<'a, str>, Json<'a>)>),
}

impl<'a> Json<'a> {
    /// Returns the value of the member with the given key, or `None` if this is not an object
    /// with such a member.
    ///
    /// If the object has the key more than once, the last member counts.
    pub(super) fn member(&self, key: &str) -> Option<&Json<'a>> {
        match self {
            Json::Object(members) => members
                .iter()
                .rev()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value),
            _ => None,
        }
    }

    /// Returns the value that a JSON Pointer (RFC 6901) names inside this value,
    /// or `None` if there is none.
    ///
    /// The empty pointer names the value itself.
    pub(super) fn at(&self, pointer: &str) -> Option<&Json<'a>> {
        if pointer.is_empty() {
            return Some(self);
        }

        let mut value = self;
        for token in pointer.strip_prefix('/')?.split('/') {
            let token = token.replace("~1", "/").replace("~0", "~");
            value = match value {
                Json::Object(_) => value.member(&token)?,
                Json::Array(elements) => {
                    // An index is written without a sign and without leading zeros.
                    let index: usize = token
                        .parse()
                        .ok()
                        .filter(|_| token == "0" || !token.starts_with(['0', '+']))?;
                    elements.get(index)?
                }
                _ => return None,
            };
        }

        Some(value)
    }
}

/// Why a text is not JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParseError {
    /// What the parser expected or found.
    pub(super) reason: &'static str,
    /// The byte position in the text.
    pub(super) position: usize,
}

/// A parser at a position in a JSON text.
#[derive(Debug)]
pub(super) struct Parser<'a> {
    /// The text parsed.
    text: &'a str,
    /// The position of the next byte to read.
    position: usize,
    /// The number of arrays and objects that enclose the position.
    depth: usize,
}

impl<'a> Parser<'a> {
    /// Creates a parser at the start of a text.
    pub(super) fn new(text: &'a str) -> Self {
        Self {
            text,
            position: 0,
            depth: 0,
        }
    }

    /// Parses a text that holds exactly one value.
    pub(super) fn document(text: &'a str) -> Result<Json<'a>, ParseError> {
        let mut parser = Self::new(text);
        let value = parser.value()?;
        parser.end()?;

        Ok(value)
    }

    /// Returns the error of the given reason at the position.
    fn error(&self, reason: &'static str) -> ParseError {
        ParseError {
            reason,
            position: self.position,
        }
    }

    /// Returns the byte at the position.
    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.position).copied()
    }

    /// Consumes the given byte if it is next, and returns whether it was.
    fn eat(&mut self, byte: u8) -> bool {
        let next = self.peek() == Some(byte);
        if next {
            self.position += 1;
        }

        next
    }

    /// Consumes the whitespace of JSON: spaces, tabs and line breaks.
    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.position += 1;
        }
    }

    /// Parses one value at the position, after any whitespace.
    pub(super) fn value(&mut self) -> Result<Json<'a>, ParseError> {
        self.skip_whitespace();
        match self.peek() {
            Some(b'"') => self.string().map(Json::String),
            Some(b'{') => self.nested(Self::object),
            Some(b'[') => self.nested(Self::array),
            Some(b'-' | b'0'..=b'9') => self.number().map(Json::Number),
            Some(b't') => self.literal("true", Json::Boolean(true)),
            Some(b'f') => self.literal("false", Json::Boolean(false)),
            Some(b'n') => self.literal("null", Json::Null),
            Some(_) => Err(self.error("expected a value")),
            None => Err(self.error("expected a value, found the end of the text")),
        }
    }

    /// Parses an array or an object with `parse`, one level of nesting deeper.
    fn nested(
        &mut self,
        parse: impl FnOnce(&mut Self) -> Result<Json<'a>, ParseError>,
    ) -> Result<Json<'a>, ParseError> {
        if self.depth >= MAX_NESTING {
            return Err(self.error("the value is nested too deeply"));
        }

        self.depth += 1;
        let value = parse(self);
        self.depth -= 1;

        value
    }

    /// Parses one of the literals `true`, `false` and `null`.
    fn literal(&mut self, word: &'static str, value: Json<'a>) -> Result<Json<'a>, ParseError> {
        if !self.text[self.position..].starts_with(word) {
            return Err(self.error("expected a value"));
        }

        self.position += word.len();
        Ok(value)
    }

    /// Parses a number, and returns it as it is written.
    ///
    /// A number is an optional `-`, then `0` or digits that do not start with `0`,
    /// then an optional fraction and an optional exponent.
    fn number(&mut self) -> Result<&'a str, ParseError> {
        let start = self.position;
        self.eat(b'-');
        match self.peek() {
            Some(b'0') => self.position += 1,
            Some(b'1'..=b'9') => self.digits(),
            _ => return Err(self.error("expected a digit")),
        }

        if self.eat(b'.') {
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.error("expected a digit after the point"));
            }

            self.digits();
        }

        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.position += 1;
            if !self.eat(b'+') {
                self.eat(b'-');
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.error("expected a digit in the exponent"));
            }

            self.digits();
        }

        Ok(&self.text[start..self.position])
    }

    /// Consumes digits.
    fn digits(&mut self) {
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.position += 1;
        }
    }

    /// Parses a string, and returns its text with the escapes resolved.
    fn string(&mut self) -> Result<Cow<'a, str>, ParseError> {
        self.position += 1;

        // The text is copied only from the first escape on.
        let mut resolved: Option<String> = None;
        let mut segment = self.position;
        loop {
            let rest = &self.text.as_bytes()[self.position..];
            let Some(special) = rest
                .iter()
                .position(|&byte| byte == b'"' || byte == b'\\' || byte < 0x20)
            else {
                self.position = self.text.len();
                return Err(self.error("the string does not end"));
            };
            self.position += special;

            match self.text.as_bytes()[self.position] {
                b'"' => {
                    let last = &self.text[segment..self.position];
                    self.position += 1;

                    return Ok(match resolved {
                        Some(mut text) => {
                            text.push_str(last);
                            Cow::Owned(text)
                        }
                        None => Cow::Borrowed(last),
                    });
                }
                b'\\' => {
                    let text = resolved.get_or_insert_with(String::new);
                    text.push_str(&self.text[segment..self.position]);
                    self.position += 1;
                    let character = self.escape()?;
                    text.push(character);
                    segment = self.position;
                }
                _ => return Err(self.error("a control character must be escaped in a string")),
            }
        }
    }

    /// Parses the escape sequence after a backslash, and returns its character.
    fn escape(&mut self) -> Result<char, ParseError> {
        let Some(byte) = self.peek() else {
            return Err(self.error("the string does not end"));
        };
        self.position += 1;

        Ok(match byte {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\u{8}',
            b'f' => '\u{C}',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => {
                let unit = self.hex4()?;
                match unit {
                    // A code point beyond the basic plane is written as two surrogates.
                    0xD800..=0xDBFF => {
                        if !(self.eat(b'\\') && self.eat(b'u')) {
                            return Err(self.error("expected the second half of a surrogate pair"));
                        }

                        let low = self.hex4()?;
                        if !(0xDC00..=0xDFFF).contains(&low) {
                            return Err(self.error("expected the second half of a surrogate pair"));
                        }

                        let code = 0x10000 + ((unit - 0xD800) << 10) + (low - 0xDC00);
                        char::from_u32(code).expect("a surrogate pair encodes a character")
                    }
                    0xDC00..=0xDFFF => {
                        return Err(self.error("a surrogate pair starts with its first half"));
                    }
                    _ => char::from_u32(unit).expect("a code unit outside the surrogates"),
                }
            }
            _ => {
                self.position -= 1;
                return Err(self.error("unknown escape sequence"));
            }
        })
    }

    /// Parses four hexadecimal digits.
    fn hex4(&mut self) -> Result<u32, ParseError> {
        let digits = self
            .text
            .get(self.position..self.position + 4)
            .filter(|digits| digits.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or_else(|| self.error("expected four hexadecimal digits"))?;
        self.position += 4;

        Ok(u32::from_str_radix(digits, 16).expect("four hexadecimal digits are a number"))
    }

    /// Parses an array.
    fn array(&mut self) -> Result<Json<'a>, ParseError> {
        let mut elements = Vec::new();
        self.elements(|parser| {
            elements.push(parser.value()?);
            Ok(())
        })?;

        Ok(Json::Array(elements))
    }

    /// Parses an array at the position, and passes each element to `visit` as it is parsed.
    ///
    /// The elements need not be kept, so an array of many records is read one record at a time.
    pub(super) fn elements(
        &mut self,
        mut element: impl FnMut(&mut Self) -> Result<(), ParseError>,
    ) -> Result<(), ParseError> {
        self.skip_whitespace();
        if !self.eat(b'[') {
            return Err(self.error("expected an array"));
        }

        self.skip_whitespace();
        if self.eat(b']') {
            return Ok(());
        }

        loop {
            element(self)?;
            self.skip_whitespace();
            if self.eat(b']') {
                return Ok(());
            }
            if !self.eat(b',') {
                return Err(self.error("expected `,` or `]`"));
            }
        }
    }

    /// Parses the first element of the array at the position,
    /// or returns `None` for an array without elements.
    pub(super) fn first_element(&mut self) -> Result<Option<Json<'a>>, ParseError> {
        self.skip_whitespace();
        if !self.eat(b'[') {
            return Err(self.error("expected an array"));
        }

        self.skip_whitespace();
        if self.peek() == Some(b']') {
            return Ok(None);
        }

        self.value().map(Some)
    }

    /// Parses an object.
    fn object(&mut self) -> Result<Json<'a>, ParseError> {
        self.position += 1;
        let mut members = Vec::new();
        self.skip_whitespace();
        if self.eat(b'}') {
            return Ok(Json::Object(members));
        }

        loop {
            self.skip_whitespace();
            if self.peek() != Some(b'"') {
                return Err(self.error("expected a key in quotes"));
            }

            let key = self.string()?;
            self.skip_whitespace();
            if !self.eat(b':') {
                return Err(self.error("expected `:` after the key"));
            }

            members.push((key, self.value()?));
            self.skip_whitespace();
            if self.eat(b'}') {
                return Ok(Json::Object(members));
            }
            if !self.eat(b',') {
                return Err(self.error("expected `,` or `}`"));
            }
        }
    }

    /// Consumes the rest of the text, which must be whitespace only.
    pub(super) fn end(&mut self) -> Result<(), ParseError> {
        self.skip_whitespace();
        if self.position < self.text.len() {
            return Err(self.error("expected the end of the text"));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Converts a parsed value into the value that `serde_json` parses,
    /// with each number converted as `serde_json` converts it.
    fn reference(value: &Json<'_>) -> serde_json::Value {
        match value {
            Json::Null => serde_json::Value::Null,
            Json::Boolean(value) => serde_json::Value::Bool(*value),
            Json::Number(text) => serde_json::from_str(text).expect("the number is JSON"),
            Json::String(text) => serde_json::Value::String(text.clone().into_owned()),
            Json::Array(elements) => elements.iter().map(reference).collect(),
            Json::Object(members) => serde_json::Value::Object(
                members
                    .iter()
                    .map(|(key, value)| (key.clone().into_owned(), reference(value)))
                    .collect(),
            ),
        }
    }

    /// The parser accepts and rejects the same texts as `serde_json`,
    /// and reads the same values from the texts it accepts.
    #[test]
    fn the_parser_agrees_with_serde_json() {
        let texts = [
            r#"null"#,
            r#" [ ] "#,
            r#"{}"#,
            r#"[1, -0, 2.50, 1e3, 1E-3, -1.5e+10, 123456789012345678901234567890]"#,
            r#"["", "plain", "esc \" \\ \/ \b \f \n \r \t", "\u00e9\u20AC", "\ud83d\ude00"]"#,
            r#"{"a": {"b": [true, false, null, {"c": "d"}]}, "é": 1}"#,
            "[\n  {\"k\"\t:\r\n 1}\n]",
            "\"tab\tinside\"",
            r#"[1,]"#,
            r#"[,1]"#,
            r#"{"a" 1}"#,
            r#"{"a": 1,}"#,
            r#"{a: 1}"#,
            r#"01"#,
            r#"1."#,
            r#".5"#,
            r#"+1"#,
            r#"1e"#,
            r#"-"#,
            r#"tru"#,
            r#"nul"#,
            r#""open"#,
            r#""bad \x escape""#,
            r#""\ud83d""#,
            r#""\ude00""#,
            r#""\u12""#,
            r#"[1] 2"#,
            r#""#,
            r#"[1 2]"#,
            r#"NaN"#,
        ];

        for text in texts {
            let parsed = Parser::document(text);
            let expected: Result<serde_json::Value, _> = serde_json::from_str(text);
            assert_eq!(
                parsed.as_ref().ok().map(reference),
                expected.ok(),
                "{text:?}: {parsed:?}"
            );
        }
    }

    /// A number keeps its spelling, and a string borrows its text unless it has an escape.
    #[test]
    fn numbers_and_plain_strings_are_kept_as_written() {
        let value = Parser::document(r#"[2.0, 2, "plain", "a\nb"]"#).expect("the text is JSON");
        let Json::Array(elements) = value else {
            panic!("the text is an array");
        };

        assert_eq!(elements[0], Json::Number("2.0"));
        assert_eq!(elements[1], Json::Number("2"));
        assert!(matches!(&elements[2], Json::String(Cow::Borrowed("plain"))));
        assert!(matches!(&elements[3], Json::String(Cow::Owned(text)) if text == "a\nb"));
    }

    /// A pointer names a value inside a document, by keys and by indexes.
    #[test]
    fn pointers_name_values_inside_a_document() {
        let text = r#"{"message": {"items": [10, 20]}, "a/b": {"~": 1}, "": 2}"#;
        let document = Parser::document(text).expect("the text is JSON");

        assert_eq!(document.at(""), Some(&document));
        assert_eq!(document.at("/message/items/1"), Some(&Json::Number("20")));
        assert_eq!(document.at("/a~1b/~0"), Some(&Json::Number("1")));
        assert_eq!(document.at("/"), Some(&Json::Number("2")));
        for missing in [
            "/message/items/2",
            "/message/items/01",
            "/message/none",
            "message",
        ] {
            assert_eq!(document.at(missing), None, "{missing}");
        }
    }

    /// Values nested beyond the limit are refused, and deep ones up to it are parsed.
    #[test]
    fn nesting_is_bounded() {
        let nested = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));

        assert!(Parser::document(&nested(MAX_NESTING)).is_ok());
        assert!(Parser::document(&nested(MAX_NESTING + 1)).is_err());
    }
}
