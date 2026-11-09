//! Lexing string literals and format strings, whose bodies are read the same way.

use nemo_physical::value::{Escape, NameKind};

use crate::{
    diagnostics::ParseError,
    model::Span,
    parser::lexer::{FStringPart, Lexer, TokenKind},
};

/// The result of one step through the body of a literal.
enum LiteralStep {
    /// Content, which was added to the text.
    Content,
    /// The closing quotes, which were consumed.
    Closed,
    /// The end of the input, before any closing quotes.
    Unterminated,
}

impl Lexer<'_> {
    /// Returns whether the literal is multiline, which means that it opens with three quotes
    /// and closes with three quotes.
    ///
    /// The current position must be after the literal's first quote.
    /// If two more quotes follow, they are consumed.
    fn opens_multiline(&mut self) -> bool {
        let multiline = self.peek() == Some(b'"') && self.peek_at(1) == Some(b'"');
        if multiline {
            self.position += 2;
        }
        multiline
    }

    /// Returns whether the quote at the current position closes a multiline literal,
    /// which it does if two more quotes follow it.
    fn closes_multiline(&self) -> bool {
        self.peek_at(1) == Some(b'"') && self.peek_at(2) == Some(b'"')
    }

    /// Consumes the escape sequence whose backslash is at the current position,
    /// and adds the character that it stands for to `text`.
    ///
    /// [Escape::in_string] decodes the escape.
    /// A backslash that does not start an escape it decodes is kept in the text,
    /// since the text may be interpreted later, for example as a regular expression,
    /// where the backslash matters.
    fn escape(&mut self, text: &mut String) {
        self.position += 1;
        match Escape::in_string(&self.source[self.position..]) {
            Some(escape) => {
                text.push(escape.character);
                self.position += escape.length;
            }
            None => text.push('\\'),
        }
    }

    /// Takes one step through the body of a string or format string,
    /// and adds what it reads to `text`.
    ///
    /// A step reads a character, an escape, a quote inside a multiline literal,
    /// or the closing quotes.
    /// A `\r` before `\n` is dropped, so that a line break in a literal is `\n`
    /// however the file encodes line breaks.
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

    /// Consumes a string literal that starts at `start`, decodes its escapes,
    /// and emits its token.
    ///
    /// Three quotes open a multiline literal, which closes with three quotes.
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

    /// Consumes a format string whose `f` is at `start` and whose opening quote is at the
    /// current position, and emits its token.
    ///
    /// The text parts are read as in a string literal, and `{{` and `}}` stand for braces.
    /// `{…}` encloses an embedded expression, whose span becomes a part,
    /// and braces inside the expression nest.
    /// A stray `}` is reported at its position.
    /// The format string then still extends to its closing quotes, but becomes an error token.
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

    /// Consumes an embedded expression, from its opening brace at the current position
    /// through its closing brace, and returns the span between the braces.
    ///
    /// Braces inside the expression nest, and braces inside its string literals are skipped.
    /// Returns `None` if the input ends inside the expression,
    /// and reports an unterminated format string that starts at `start`.
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
                // A brace inside a string literal of the expression closes nothing.
                // A quote directly after an `f` that does not end a longer name
                // opens a format string.
                Some(b'"') => {
                    let format = self.position > expression
                        && self.bytes[self.position - 1] == b'f'
                        && !self.source[expression..self.position - 1]
                            .chars()
                            .next_back()
                            .is_some_and(NameKind::continues);
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

    /// Consumes a string literal or format string inside an embedded expression,
    /// from its opening quote through its closing quotes.
    ///
    /// `format` says whether it is a format string,
    /// whose own embedded expressions are then consumed too.
    /// Returns `false` if the input ends inside the literal,
    /// and reports an unterminated format string that starts at `start`.
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
