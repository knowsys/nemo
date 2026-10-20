//! Number literals.

use crate::parser::lexer::{Lexer, TokenKind, is_name_continue};

impl Lexer<'_> {
    /// Consumes a number literal: digits, an optional fraction, and an optional exponent,
    /// or after `0b`, `0o` or `0x` a radix form.
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
                // Not an exponent after all: `1e` starts `1` and `e…`.
                self.position = mark;
            }
        }
        // A type suffix: `f`/`F` marks a float, `d`/`D` a double, taken
        // only if nothing name-like follows.
        if matches!(self.peek(), Some(b'f' | b'F' | b'd' | b'D'))
            && !self.peek_at(1).is_some_and(is_name_continue)
        {
            self.position += 1;
        }
        self.push(TokenKind::Number, start);
    }
}
