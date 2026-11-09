//! Lexing number literals.

use nemo_physical::value::NameKind;

use crate::parser::lexer::{Lexer, TokenKind};

impl Lexer<'_> {
    /// Consumes a number literal that starts at `start`, and emits its token.
    ///
    /// A number literal is either a binary, octal or hexadecimal number after `0b`, `0o`
    /// or `0x`, or digits with an optional fraction, exponent and type suffix.
    pub(super) fn number(&mut self, start: usize) {
        if self.peek() == Some(b'0')
            && let Some(marker @ (b'b' | b'o' | b'x')) = self.peek_at(1)
        {
            let digit = |byte: u8| match marker {
                b'b' => matches!(byte, b'0' | b'1'),
                b'o' => matches!(byte, b'0'..=b'7'),
                _ => byte.is_ascii_hexdigit(),
            };
            if self.peek_at(2).is_some_and(digit) {
                self.position += 2;
                self.skip_while(digit);
                self.push(TokenKind::Number, start);
                return;
            }
        }
        self.skip_while(|byte| byte.is_ascii_digit());
        if self.peek() == Some(b'.') && self.peek_at(1).is_some_and(|byte| byte.is_ascii_digit()) {
            self.position += 1;
            self.skip_while(|byte| byte.is_ascii_digit());
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            let mark = self.position;
            self.position += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.position += 1;
            }
            if self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                self.skip_while(|byte| byte.is_ascii_digit());
            } else {
                // This is not an exponent, so `1e…` is lexed as `1` followed by a name `e…`.
                self.position = mark;
            }
        }
        // A type suffix `f` or `F` marks a float, and `d` or `D` a double.
        // It is consumed only if no character of a name follows it.
        if matches!(self.peek(), Some(b'f' | b'F' | b'd' | b'D'))
            && !self.source[self.position + 1..]
                .chars()
                .next()
                .is_some_and(NameKind::continues)
        {
            self.position += 1;
        }
        self.push(TokenKind::Number, start);
    }
}
