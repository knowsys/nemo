//! The lexer: source text into a token buffer.

mod literal;
mod number;
mod token;

pub(super) use self::token::{FStringPart, Token, TokenKind};

use nemo_physical::{
    diagnostics::Diagnostic,
    value::{Iri, NameKind},
};

use crate::{
    diagnostics::ParseError,
    model::{FileId, Span, VariableKind},
};

/// Tokenizes a source file completely.
///
/// Lexical problems become diagnostics plus [Error](TokenKind::Error) tokens,
/// so every byte of the input ends up in a token.
/// The buffer ends with [End](TokenKind::End).
pub(super) fn tokenize(source: &str, file: FileId) -> (Vec<Token>, Vec<Diagnostic>) {
    tokenize_at(source, file, 0)
}

/// Tokenizes a slice of a larger file completely,
/// with spans shifted by the slice's offset within it (for embedded format-string expressions).
pub(super) fn tokenize_at(
    source: &str,
    file: FileId,
    offset: usize,
) -> (Vec<Token>, Vec<Diagnostic>) {
    Lexer {
        source,
        bytes: source.as_bytes(),
        file,
        offset,
        position: 0,
        tokens: Vec::new(),
        diagnostics: Vec::new(),
    }
    .run()
}

/// The lexer state over one source file.
struct Lexer<'a> {
    /// The source text.
    source: &'a str,
    /// The source as bytes, for single-byte dispatch.
    bytes: &'a [u8],
    /// The file the spans point into.
    file: FileId,
    /// The offset of this text within its file.
    offset: usize,
    /// The current byte position.
    position: usize,
    /// The tokens produced so far.
    tokens: Vec<Token>,
    /// The problems found so far.
    diagnostics: Vec<Diagnostic>,
}

impl Lexer<'_> {
    /// Runs the lexer to the end of the input.
    fn run(mut self) -> (Vec<Token>, Vec<Diagnostic>) {
        // Spans address a file by 32-bit offsets. A text they cannot
        // address is rejected whole rather than given wrong locations.
        if !addressable(self.offset, self.source.len()) {
            self.diagnostics.push(ParseError::TextTooLarge.diagnostic());
            self.push(TokenKind::End, 0);
            return (self.tokens, self.diagnostics);
        }
        // A UTF-8 byte order mark opens many a Windows-written file; at
        // the very start it is encoding metadata, not content.
        if self.offset == 0 && self.source.starts_with('\u{feff}') {
            self.position = '\u{feff}'.len_utf8();
        }
        loop {
            self.skip_trivia();
            let start = self.position;
            let Some(byte) = self.peek() else {
                self.push(TokenKind::End, start);
                return (self.tokens, self.diagnostics);
            };
            self.token(byte, start);
        }
    }

    /// Lexes one token, from the given byte at `start`.
    fn token(&mut self, byte: u8, start: usize) {
        match byte {
            b'(' => self.single(TokenKind::ParenOpen),
            b')' => self.single(TokenKind::ParenClose),
            b'{' => self.single(TokenKind::BraceOpen),
            b'}' => self.single(TokenKind::BraceClose),
            b'[' => self.single(TokenKind::BracketOpen),
            b']' => self.single(TokenKind::BracketClose),
            b',' => self.single(TokenKind::Comma),
            b'.' => self.single(TokenKind::Dot),
            b'~' => self.single(TokenKind::Tilde),
            b'|' => self.single(TokenKind::Bar),
            b'+' => self.single(TokenKind::Plus),
            b'*' => self.single(TokenKind::Star),
            b'/' => self.single(TokenKind::Slash),
            b'=' => {
                self.position += 1;
                self.either(b'=', TokenKind::ValueEqual, TokenKind::Equal, start);
            }
            b'-' => {
                self.position += 1;
                self.either(b'>', TokenKind::ArrowRight, TokenKind::Minus, start);
            }
            b'>' => {
                self.position += 1;
                self.either(b'=', TokenKind::GreaterEq, TokenKind::Greater, start);
            }
            b':' => self.colon(start),
            b'!' => self.bang(start),
            b'?' => {
                self.position += 1;
                self.marked_name(TokenKind::Variable(VariableKind::Universal), start);
            }
            b'$' => {
                self.position += 1;
                self.marked_name(TokenKind::Variable(VariableKind::Global), start);
            }
            b'@' => self.at_sign(start),
            b'#' => {
                self.position += 1;
                if self.eat_byte(b'[') {
                    self.push(TokenKind::AttributeOpen, start);
                } else {
                    self.marked_name(TokenKind::Hash, start);
                }
            }
            b'^' => {
                self.position += 1;
                if self.eat_byte(b'^') {
                    self.push(TokenKind::Carets, start);
                } else {
                    // After a string, the carets of a datatype were meant,
                    // and elsewhere most likely a power.
                    let hint = if self.previous_is_string() {
                        "write `^^` to give a literal its datatype"
                    } else {
                        "write a power as `POW(base, exponent)`"
                    };
                    let span = self.span(start);
                    self.reject(span, ParseError::LoneCaret.at(Some(span)).with_hint(hint));
                }
            }
            b'<' => self.less_or_iri(start),
            b'"' => self.string(start),
            byte if byte.is_ascii_digit() => self.number(start),
            // `f"…"` opens a format string, never a name.
            b'f' if self.peek_at(1) == Some(b'"') => {
                self.position += 1;
                self.format_string(start);
            }
            // `_:label` is a blank node, whose label is a local name;
            // a bare `_` (or `_name`) a name,
            // which the parser reads as anonymous where it is alone.
            b'_' if self.peek_at(1) == Some(b':')
                && NameKind::Local.length(&self.source[start + 2..]) > 0 =>
            {
                self.position += 2;
                self.take_name(NameKind::Local);
                self.push(TokenKind::BlankNode, start);
            }
            _ if self.take_name(NameKind::Bare) => self.push(TokenKind::Ident, start),
            _ => {
                let character = self.character();
                self.error(start, ParseError::UnexpectedCharacter { character });
            }
        }
    }

    /// Lexes what `:` opens: `::`, `:-`, or the colon itself.
    ///
    /// A colon that touches a name before it separates a prefix from a local name,
    /// and so does a colon after no name at all, the empty prefix's, as in `:a`.
    /// A local name directly after such a colon is lexed as one, as in `wd:42` or `ex:v1.2`.
    /// A colon after a name and a space, as in `age :30`, separates a named argument's label
    /// from its value instead.
    fn colon(&mut self, start: usize) {
        let separates_prefix = self.tokens.last().is_none_or(|token| {
            token.kind != TokenKind::Ident || token.span.end as usize == self.offset + start
        });
        self.position += 1;
        let kind = if self.eat_byte(b':') {
            TokenKind::DoubleColon
        } else if self.eat_byte(b'-') {
            TokenKind::ArrowLeft
        } else {
            TokenKind::Colon
        };
        let colon = kind == TokenKind::Colon;
        self.push(kind, start);
        if colon && separates_prefix {
            let local = self.position;
            if self.take_name(NameKind::Local) {
                self.push(TokenKind::Ident, local);
            }
        }
    }

    /// Lexes what `!` opens: `!=`, `!==`, or an existential variable.
    fn bang(&mut self, start: usize) {
        self.position += 1;
        if self.eat_byte(b'=') {
            self.either(b'=', TokenKind::ValueUnequal, TokenKind::Unequal, start);
        } else {
            self.marked_name(TokenKind::Variable(VariableKind::Existential), start);
        }
    }

    /// Lexes what `@` opens:
    /// a language tag directly after a string literal, with any `-` subtags (`en-US`),
    /// and a directive keyword otherwise.
    fn at_sign(&mut self, start: usize) {
        self.position += 1;
        if !self.previous_is_string() {
            self.marked_name(TokenKind::At, start);
            return;
        }
        if !self.take_name(NameKind::Marked) {
            self.error(start, ParseError::MarkerWithoutName);
            return;
        }
        while self.peek() == Some(b'-')
            && self
                .peek_at(1)
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
        {
            self.position += 1;
            self.skip_while(|byte| byte.is_ascii_alphanumeric());
        }
        self.push(TokenKind::LanguageTag, start);
    }

    /// Skips whitespace, `%` line comments, and `/* … */` block comments.
    ///
    /// An unterminated block comment runs to the end.
    fn skip_trivia(&mut self) {
        loop {
            match self.peek() {
                Some(byte) if byte.is_ascii_whitespace() => self.position += 1,
                Some(b'%') => self.skip_while(|byte| byte != b'\n'),
                Some(b'/') if self.peek_at(1) == Some(b'*') => {
                    self.position += 2;
                    while let Some(byte) = self.peek() {
                        self.position += 1;
                        if byte == b'*' && self.eat_byte(b'/') {
                            break;
                        }
                    }
                }
                _ => return,
            }
        }
    }

    /// Returns the byte at the current position.
    fn peek(&self) -> Option<u8> {
        self.peek_at(0)
    }

    /// Returns the byte `n` places after the current position.
    fn peek_at(&self, n: usize) -> Option<u8> {
        self.bytes.get(self.position + n).copied()
    }

    /// Consumes the given byte if it is next.
    fn eat_byte(&mut self, byte: u8) -> bool {
        let next = self.peek() == Some(byte);
        if next {
            self.position += 1;
        }
        next
    }

    /// Consumes bytes as long as they satisfy the predicate.
    fn skip_while(&mut self, predicate: impl Fn(u8) -> bool) {
        while self.peek().is_some_and(&predicate) {
            self.position += 1;
        }
    }

    /// Consumes the character at the current position.
    fn character(&mut self) -> char {
        let character = self.source[self.position..]
            .chars()
            .next()
            .expect("the position is on a character boundary");
        self.position += character.len_utf8();
        character
    }

    /// Emits a single-byte token.
    fn single(&mut self, kind: TokenKind) {
        let start = self.position;
        self.position += 1;
        self.push(kind, start);
    }

    /// Emits the `long` token if `second` is the next byte, consuming it,
    /// and the `short` one otherwise, the token's first byte already consumed.
    fn either(&mut self, second: u8, long: TokenKind, short: TokenKind, start: usize) {
        let kind = if self.eat_byte(second) { long } else { short };
        self.push(kind, start);
    }

    /// Consumes a name after a marker character, emitting the given kind
    /// (or an error if no name follows).
    fn marked_name(&mut self, kind: TokenKind, start: usize) {
        if self.take_name(NameKind::Marked) {
            self.push(kind, start);
        } else {
            self.error(start, ParseError::MarkerWithoutName);
        }
    }

    /// Consumes the longest name of the given kind at the current position,
    /// returning whether there is one.
    fn take_name(&mut self, kind: NameKind) -> bool {
        let length = kind.length(&self.source[self.position..]);
        self.position += length;
        length > 0
    }

    /// Returns whether the previous token is a string ending exactly at the current start,
    /// the adjacency that makes `@en` a language tag.
    fn previous_is_string(&self) -> bool {
        self.tokens.last().is_some_and(|token| {
            matches!(token.kind, TokenKind::String(_))
                && token.span.end as usize == self.offset + self.position - 1
        })
    }

    /// Distinguishes an IRI from a less-than: `<` opens an IRI when a
    /// closing `>` appears before any whitespace.
    ///
    /// An IRI that spells the character U+0000, as written or as a character escape,
    /// is refused.
    fn less_or_iri(&mut self, start: usize) {
        let rest = &self.bytes[start + 1..];
        let closes = rest
            .iter()
            .take_while(|byte| !byte.is_ascii_whitespace())
            .position(|&byte| byte == b'>');

        match closes {
            Some(length) => {
                self.position = start + 1 + length + 1;
                let written = &self.source[start + 1..start + 1 + length];
                if Iri::unescape(written).contains('\u{0}') {
                    self.error(start, ParseError::NulInIri);
                } else {
                    self.push(TokenKind::Iri, start);
                }
            }
            None => {
                self.position += 1;
                self.either(b'=', TokenKind::LessEq, TokenKind::Less, start);
            }
        }
    }

    /// Emits a token from `start` to the current position.
    fn push(&mut self, kind: TokenKind, start: usize) {
        self.tokens.push(Token {
            kind,
            span: self.span(start),
        });
    }

    /// Records a lexical problem and emits an error token.
    fn error(&mut self, start: usize, kind: ParseError) {
        let span = self.span(start);
        self.reject(span, kind.at(Some(span)));
    }

    /// Records the diagnostic of a lexical problem and emits an error token over `span`.
    fn reject(&mut self, span: Span, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
        self.tokens.push(Token {
            kind: TokenKind::Error,
            span,
        });
    }

    /// Returns the span from `start` to the current position.
    fn span(&self, start: usize) -> Span {
        Span {
            file: self.file,
            start: u32::try_from(self.offset + start).expect("source files stay below 4 GiB"),
            end: u32::try_from(self.offset + self.position).expect("source files stay below 4 GiB"),
        }
    }
}

/// Returns whether a text of the given length at the given offset within its
/// file stays addressable by 32-bit spans.
fn addressable(offset: usize, length: usize) -> bool {
    offset
        .checked_add(length)
        .is_some_and(|end| u32::try_from(end).is_ok())
}

#[cfg(test)]
mod tests {
    use super::addressable;

    /// No test holds a 4 GiB text, so the guard is checked on its own.
    #[test]
    fn texts_beyond_the_span_range_are_not_addressable() {
        assert!(addressable(0, u32::MAX as usize));
        assert!(addressable(7, u32::MAX as usize - 7));
        assert!(!addressable(1, u32::MAX as usize));
        assert!(!addressable(usize::MAX, 1));
    }
}
