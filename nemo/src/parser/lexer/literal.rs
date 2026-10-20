//! String and format-string literals, which share one body scanner.

use crate::{
    diagnostics::ParseError,
    model::Span,
    parser::lexer::{FStringPart, Lexer, TokenKind, is_name_continue},
};

/// What one step through a literal's body met.
enum LiteralStep {
    /// Content, taken into the text.
    Content,
    /// The closing quotes, consumed.
    Closed,
    /// The end of the input, before any closing quotes.
    Unterminated,
}

impl Lexer<'_> {
    /// Returns whether three quotes open the literal at the current position,
    /// past its first quote: the multiline form, closed by three quotes.
    ///
    /// The two further quotes are consumed.
    fn opens_multiline(&mut self) -> bool {
        let multiline = self.peek() == Some(b'"') && self.peek_at(1) == Some(b'"');
        if multiline {
            self.position += 2;
        }
        multiline
    }

    /// Returns whether the quote at the current position closes the multiline
    /// form: two more follow it.
    fn closes_multiline(&self) -> bool {
        self.peek_at(1) == Some(b'"') && self.peek_at(2) == Some(b'"')
    }

    /// Consumes an escape sequence, its backslash at the current position, into the text:
    /// `\\`, `\"`, `\n`, `\t`, `\r`, and the character escapes `\uXXXX` and `\UXXXXXXXX`
    /// of Turtle and SPARQL.
    ///
    /// An unknown escape, or a `\u` without the digits of a character, passes through verbatim,
    /// since the backslash may be meant for a later consumer
    /// (regular expressions, replacement patterns).
    fn escape(&mut self, text: &mut String) {
        self.position += 1;
        let escaped = match self.peek() {
            Some(b'\\') => '\\',
            Some(b'"') => '"',
            Some(b'n') => '\n',
            Some(b't') => '\t',
            Some(b'r') => '\r',
            Some(letter @ (b'u' | b'U')) => {
                let width = if letter == b'u' { 4 } else { 8 };
                match self.character_escape(width) {
                    Some(character) => {
                        text.push(character);
                        self.position += 1 + width;
                    }
                    None => text.push('\\'),
                }
                return;
            }
            _ => {
                text.push('\\');
                return;
            }
        };
        text.push(escaped);
        self.position += 1;
    }

    /// Returns the character the given number of hexadecimal digits after the current position
    /// name, or `None` if they are not that many digits or name no character.
    fn character_escape(&self, width: usize) -> Option<char> {
        let digits = self
            .bytes
            .get(self.position + 1..self.position + 1 + width)?;
        if !digits.iter().all(u8::is_ascii_hexdigit) {
            return None;
        }
        let digits = std::str::from_utf8(digits).ok()?;
        char::from_u32(u32::from_str_radix(digits, 16).ok()?)
    }

    /// Takes one step through the body of a string or format string into its text:
    /// a character, an escape, a line break, an inner quote of the multiline form,
    /// or the closing quotes.
    ///
    /// Source line endings normalize, since a line break in a literal
    /// means the text, not the file's encoding of it.
    fn literal_step(&mut self, multiline: bool, text: &mut String) -> LiteralStep {
        let Some(byte) = self.peek() else {
            return LiteralStep::Unterminated;
        };
        match byte {
            b'"' if multiline && !self.closes_multiline() => {
                text.push('"');
                self.position += 1;
            }
            b'"' => {
                self.position += if multiline { 3 } else { 1 };
                return LiteralStep::Closed;
            }
            b'\r' if self.peek_at(1) == Some(b'\n') => self.position += 1,
            b'\\' => self.escape(text),
            _ => {
                let character = self.character();
                text.push(character);
            }
        }
        LiteralStep::Content
    }

    /// Consumes a string literal, processing escapes.
    ///
    /// Three quotes open the multiline form, closed by three quotes, as for format strings.
    pub(super) fn string(&mut self, start: usize) {
        self.position += 1;
        let multiline = self.opens_multiline();
        let mut value = String::new();
        loop {
            match self.literal_step(multiline, &mut value) {
                LiteralStep::Content => {}
                LiteralStep::Closed => {
                    self.push(TokenKind::String(value), start);
                    return;
                }
                LiteralStep::Unterminated => {
                    self.error(start, ParseError::UnterminatedString);
                    return;
                }
            }
        }
    }

    /// Consumes a format string, from the opening quote.
    ///
    /// Text pieces read as string literals do, plus `{{` and `}}` for literal braces.
    /// `{…}` captures an embedded expression's source region, and braces nest.
    /// A stray `}` is reported where it stands, and the literal still runs to its closing quotes,
    /// as an error token.
    pub(super) fn format_string(&mut self, start: usize) {
        self.position += 1;
        let multiline = self.opens_multiline();
        let mut parts = Vec::new();
        let mut text = String::new();
        // Whether a stray `}` was reported, which makes the literal an error token.
        let mut stray = false;
        loop {
            match self.peek() {
                Some(b'{') if self.peek_at(1) == Some(b'{') => {
                    text.push('{');
                    self.position += 2;
                }
                Some(b'}') if self.peek_at(1) == Some(b'}') => {
                    text.push('}');
                    self.position += 2;
                }
                Some(b'{') => {
                    let Some(region) = self.embedded_expression(start) else {
                        return;
                    };
                    if !text.is_empty() {
                        parts.push(FStringPart::Text(std::mem::take(&mut text)));
                    }
                    parts.push(FStringPart::Expression(region));
                }
                Some(b'}') => {
                    let brace = self.position;
                    self.position += 1;
                    let span = self.span(brace);
                    self.diagnostics.push(ParseError::StrayBrace.at(Some(span)));
                    stray = true;
                }
                _ => match self.literal_step(multiline, &mut text) {
                    LiteralStep::Content => {}
                    LiteralStep::Closed if stray => {
                        self.push(TokenKind::Error, start);
                        return;
                    }
                    LiteralStep::Closed => {
                        if !text.is_empty() {
                            parts.push(FStringPart::Text(text));
                        }
                        self.push(TokenKind::FString(parts), start);
                        return;
                    }
                    LiteralStep::Unterminated => {
                        self.error(start, ParseError::UnterminatedFormatString);
                        return;
                    }
                },
            }
        }
    }

    /// Consumes an embedded expression, from its opening brace at the
    /// current position through the brace closing it (braces nest):
    /// the source region between them. `None` for one the input ends
    /// inside, reported as an unterminated format string starting at
    /// `start`.
    fn embedded_expression(&mut self, start: usize) -> Option<Span> {
        self.position += 1;
        let expression = self.position;
        let mut depth = 1usize;
        loop {
            match self.peek() {
                None => {
                    self.error(start, ParseError::UnterminatedFormatString);
                    return None;
                }
                // A brace inside a string of the expression closes nothing.
                Some(b'"') => {
                    let format = self.position > expression
                        && self.bytes[self.position - 1] == b'f'
                        && (self.position - 1 == expression
                            || !is_name_continue(self.bytes[self.position - 2]));
                    if !self.skip_literal(start, format) {
                        return None;
                    }
                    continue;
                }
                Some(b'{') => depth += 1,
                Some(b'}') => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            self.position += 1;
        }
        let region = self.span(expression);
        self.position += 1;
        Some(region)
    }

    /// Steps over a string or format string inside an embedded expression,
    /// from its opening quote through its closing quotes,
    /// the expressions of a format string included.
    /// Returns `false` for one the input ends inside,
    /// reported as an unterminated format string starting at `start`.
    fn skip_literal(&mut self, start: usize, format: bool) -> bool {
        self.position += 1;
        let multiline = self.opens_multiline();
        let mut text = String::new();
        loop {
            if format && self.peek() == Some(b'{') {
                if self.peek_at(1) == Some(b'{') {
                    self.position += 2;
                } else if self.embedded_expression(start).is_none() {
                    return false;
                }
                continue;
            }
            match self.literal_step(multiline, &mut text) {
                LiteralStep::Content => {}
                LiteralStep::Closed => return true,
                LiteralStep::Unterminated => {
                    self.error(start, ParseError::UnterminatedFormatString);
                    return false;
                }
            }
        }
    }
}
