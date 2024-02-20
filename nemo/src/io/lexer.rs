//! Lexical tokenization of rulewerk-style rules.

use std::str::Chars;

const EOF_CHAR: char = '\0';

#[derive(Debug, Copy, Clone, PartialEq)]
struct Span<'a> {
    offset: usize,
    line: usize,
    // size: usize,
    fragment: &'a str,
}
// impl<'a> Span<'a> {
impl<'a> Span<'a> {
    fn new(offset: usize, line: usize, input: &'a str) -> Span<'a> {
        // fn new(offset: usize, line: usize, size: usize) -> Span {
        Span {
            offset,
            line,
            fragment: input,
            // size,
        }
    }
}

#[derive(Debug, Clone)]
struct Lexer<'a> {
    input: &'a str,
    len_remaining: usize,
    offset: usize,
    lines: usize,
    chars: Chars<'a>,
}
impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Lexer<'a> {
        Lexer {
            input,
            len_remaining: input.len(),
            offset: 0,
            lines: 1,
            chars: input.chars(),
        }
    }
    fn consumed_char_length(&self) -> usize {
        self.len_remaining - self.chars.as_str().len()
    }
    fn update_remaining_len(&mut self) {
        self.len_remaining = self.chars.as_str().len();
    }
    fn peek(&self, count: usize) -> char {
        self.chars.clone().nth(count - 1).unwrap_or(EOF_CHAR)
    }
    fn bump(&mut self) -> Option<char> {
        self.chars.next()
    }
    fn is_eof(&self) -> bool {
        self.chars.as_str().is_empty()
    }
    fn bump_while(&mut self, mut predicate: impl FnMut(char) -> bool) {
        while predicate(self.peek(1)) && !self.is_eof() {
            self.bump();
        }
    }
    fn get_tokens(&mut self) -> Vec<Token> {
        use TokenKind::*;
        let mut vec = Vec::new();
        loop {
            let old_line_num = self.lines;
            let first_char = match self.bump() {
                Some(c) => c,
                None => {
                    let eof_tok = Token::new(
                        Eof,
                        Span::new(
                            self.offset,
                            self.lines,
                            &self.input[self.offset..self.offset],
                        ),
                    );
                    vec.push(eof_tok);
                    return vec;
                }
            };
            let token_kind = match first_char {
                '%' => match (self.peek(1), self.peek(2)) {
                    (n1, n2) if n1.is_digit(16) && n2.is_digit(16) => self.pct_encoded(),
                    _ => self.comment(),
                },
                '\n' => {
                    self.lines += 1;
                    Whitespace
                }
                c if is_whitespace(c) => self.whitespace(),
                c if unicode_ident::is_xid_start(c) => self.ident(),
                c @ '0'..='9' => self.number(),
                '?' => QuestionMark,
                '!' => ExclamationMark,
                '(' => OpenParen,
                ')' => CloseParen,
                '[' => OpenBracket,
                ']' => CloseBracket,
                '{' => OpenBrace,
                '}' => CloseBrace,
                '.' => Dot,
                ',' => Comma,
                ':' => Colon,
                ';' => Semicolon,
                '>' => Greater,
                '=' => Equal,
                '<' => Less,
                '~' => Tilde,
                '^' => Caret,
                '#' => Hash,
                '_' => Underscore,
                '@' => At,
                '+' => Plus,
                '-' => Minus,
                '*' => Star,
                '/' => Slash,
                '$' => Dollar,
                '&' => Ampersand,
                '\'' => Apostrophe,
                '\u{A0}'..='\u{D7FF}'
                | '\u{F900}'..='\u{FDCF}'
                | '\u{FDF0}'..='\u{FFEF}'
                | '\u{10000}'..='\u{1FFFD}'
                | '\u{20000}'..='\u{2FFFD}'
                | '\u{30000}'..='\u{3FFFD}'
                | '\u{40000}'..='\u{4FFFD}'
                | '\u{50000}'..='\u{5FFFD}'
                | '\u{60000}'..='\u{6FFFD}'
                | '\u{70000}'..='\u{7FFFD}'
                | '\u{80000}'..='\u{8FFFD}'
                | '\u{90000}'..='\u{9FFFD}'
                | '\u{A0000}'..='\u{AFFFD}'
                | '\u{B0000}'..='\u{BFFFD}'
                | '\u{C0000}'..='\u{CFFFD}'
                | '\u{D0000}'..='\u{DFFFD}'
                | '\u{E1000}'..='\u{EFFFD}' => self.ucschar(),
                '\u{E000}'..='\u{F8FF}'
                | '\u{F0000}'..='\u{FFFFD}'
                | '\u{100000}'..='\u{10FFFD}' => self.iprivate(),
                _ => todo!(),
            };
            let tok_len = self.consumed_char_length();

            // let fragment = &*self.input;
            let token = Token::new(
                token_kind,
                Span::new(
                    self.offset,
                    old_line_num,
                    &self.input[self.offset..(self.offset + tok_len)],
                ),
                // Span::new(self.offset, self.lines, tok_len),
            );
            self.offset += tok_len;
            self.update_remaining_len();
            vec.push(token);
        }
    }

    fn number(&mut self) -> TokenKind {
        self.bump_while(is_hex_digit);
        TokenKind::Number
    }
    fn pct_encoded(&mut self) -> TokenKind {
        self.bump();
        self.bump();
        TokenKind::PctEncoded
    }
    fn comment(&mut self) -> TokenKind {
        self.bump_while(|c| c != '\n');
        self.bump();
        self.lines += 1;
        TokenKind::Comment
    }
    fn whitespace(&mut self) -> TokenKind {
        self.bump_while(|c| is_whitespace(c) && c != '\n');
        if '\n' == self.peek(1) {
            self.bump();
            self.lines += 1;
            return TokenKind::Whitespace;
        }
        TokenKind::Whitespace
    }
    fn ident(&mut self) -> TokenKind {
        self.bump_while(unicode_ident::is_xid_continue);
        TokenKind::Ident
    }

    fn ucschar(&mut self) -> TokenKind {
        self.bump_while(is_ucschar);
        TokenKind::UcsChars
    }

    fn iprivate(&mut self) -> TokenKind {
        self.bump_while(is_iprivate);
        TokenKind::Iprivate
    }
}

fn is_hex_digit(c: char) -> bool {
    c.is_digit(16)
}

fn is_whitespace(c: char) -> bool {
    // support also vertical tab, form feed, NEXT LINE (latin1),
    // LEFT-TO-RIGHT MARK, RIGHT-TO-LEFT MARK, LINE SEPARATOR and PARAGRAPH SEPARATOR?
    matches!(c, ' ' | '\n' | '\t' | '\r')
}

fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    if let Some(char) = chars.next() {
        unicode_ident::is_xid_start(char) && chars.all(unicode_ident::is_xid_continue)
    } else {
        false
    }
}

fn is_ucschar(c: char) -> bool {
    matches!(c, '\u{A0}'..='\u{D7FF}'
            | '\u{F900}'..='\u{FDCF}'
            | '\u{FDF0}'..='\u{FFEF}'
            | '\u{10000}'..='\u{1FFFD}'
            | '\u{20000}'..='\u{2FFFD}'
            | '\u{30000}'..='\u{3FFFD}'
            | '\u{40000}'..='\u{4FFFD}'
            | '\u{50000}'..='\u{5FFFD}'
            | '\u{60000}'..='\u{6FFFD}'
            | '\u{70000}'..='\u{7FFFD}'
            | '\u{80000}'..='\u{8FFFD}'
            | '\u{90000}'..='\u{9FFFD}'
            | '\u{A0000}'..='\u{AFFFD}'
            | '\u{B0000}'..='\u{BFFFD}'
            | '\u{C0000}'..='\u{CFFFD}'
            | '\u{D0000}'..='\u{DFFFD}'
            | '\u{E1000}'..='\u{EFFFD}')
}

fn is_iprivate(c: char) -> bool {
    matches!(c, '\u{E000}'..='\u{F8FF}' | '\u{F0000}'..='\u{FFFFD}' | '\u{100000}'..='\u{10FFFD}')
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct Token<'a> {
    kind: TokenKind,
    span: Span<'a>,
}
// impl<'a> Token<'a> {
impl<'a> Token<'a> {
    fn new(kind: TokenKind, span: Span<'a>) -> Token<'a> {
        Token { kind, span }
    }
}

/// All the tokens the input gets parsed into.
#[derive(Debug, PartialEq, Copy, Clone)]
enum TokenKind {
    // Syntactic symbols:
    /// '?'
    QuestionMark,
    /// '!'
    ExclamationMark,
    /// '('
    OpenParen,
    /// ')'
    CloseParen,
    /// '['
    OpenBracket,
    /// ']'
    CloseBracket,
    /// '{'
    OpenBrace,
    /// '}'
    CloseBrace,
    /// '.'
    Dot,
    /// ','
    Comma,
    /// ':'
    Colon,
    /// ';'
    Semicolon,
    /// '>'
    Greater,
    /// '='
    Equal,
    /// '<'
    Less,
    /// '~'
    Tilde,
    /// '^'
    Caret,
    /// '#'
    Hash,
    /// '_'
    Underscore,
    /// '@'
    At,
    /// '+'
    Plus,
    /// '-'
    Minus,
    /// '*'
    Star,
    /// '/'
    Slash,
    /// '$'
    Dollar,
    /// '&'
    Ampersand,
    /// "'"
    Apostrophe,
    // Multi-char tokens:
    /// Identifier for keywords and predicate names
    Ident,
    /// All other Utf8 characters that can be used in an IRI
    UcsChars,
    /// Characters in private use areas
    Iprivate,
    /// Percent-encoded characters in IRIs
    PctEncoded,
    /// Base 10 digits
    Number,
    /// A string literal
    String,
    /// A comment, starting with `%`
    Comment,
    /// A comment, starting with `%%`
    DocComment,
    /// bool: ends_with_newline
    Whitespace,
    /// catch all token
    Illegal,
    /// signals end of file
    Eof,
}

#[cfg(test)]
mod test {
    use super::TokenKind::*;
    use crate::io::lexer::{Lexer, Span, Token};

    #[test]
    fn empty_input() {
        let mut lexer = Lexer::new("");
        assert_eq!(
            lexer.get_tokens(),
            vec![Token::new(Eof, Span::new(0, 1, ""))]
        )
    }

    #[test]
    fn base() {
        let mut lexer = Lexer::new("@base");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(At, Span::new(0, 1, "@")),
                Token::new(Ident, Span::new(1, 1, "base")),
                Token::new(Eof, Span::new(5, 1, "")),
            ]
        )
    }

    #[test]
    fn prefix() {
        let mut lexer = Lexer::new("@prefix");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(At, Span::new(0, 1, "@")),
                Token::new(Ident, Span::new(1, 1, "prefix")),
                Token::new(Eof, Span::new(7, 1, "")),
            ]
        )
    }

    #[test]
    fn output() {
        let mut lexer = Lexer::new("@output");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(At, Span::new(0, 1, "@")),
                Token::new(Ident, Span::new(1, 1, "output")),
                Token::new(Eof, Span::new(7, 1, "")),
            ]
        )
    }

    #[test]
    fn import() {
        let mut lexer = Lexer::new("@import");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(At, Span::new(0, 1, "@")),
                Token::new(Ident, Span::new(1, 1, "import")),
                Token::new(Eof, Span::new(7, 1, "")),
            ]
        )
    }

    #[test]
    fn export() {
        let mut lexer = Lexer::new("@export");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(At, Span::new(0, 1, "@")),
                Token::new(Ident, Span::new(1, 1, "export")),
                Token::new(Eof, Span::new(7, 1, "")),
            ]
        )
    }

    #[test]
    fn tokenize() {
        let mut lexer = Lexer::new("P(?X) :- A(?X).\t\n    A(Human).");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(Ident, Span::new(0, 1, "P")),
                Token::new(OpenParen, Span::new(1, 1, "(")),
                Token::new(QuestionMark, Span::new(2, 1, "?")),
                Token::new(Ident, Span::new(3, 1, "X")),
                Token::new(CloseParen, Span::new(4, 1, ")")),
                Token::new(Whitespace, Span::new(5, 1, " ")),
                Token::new(Colon, Span::new(6, 1, ":")),
                Token::new(Minus, Span::new(7, 1, "-")),
                Token::new(Whitespace, Span::new(8, 1, " ")),
                Token::new(Ident, Span::new(9, 1, "A")),
                Token::new(OpenParen, Span::new(10, 1, "(")),
                Token::new(QuestionMark, Span::new(11, 1, "?")),
                Token::new(Ident, Span::new(12, 1, "X")),
                Token::new(CloseParen, Span::new(13, 1, ")")),
                Token::new(Dot, Span::new(14, 1, ".")),
                Token::new(Whitespace, Span::new(15, 1, "\t\n")),
                Token::new(Whitespace, Span::new(17, 2, "    ")),
                Token::new(Ident, Span::new(21, 2, "A")),
                Token::new(OpenParen, Span::new(22, 2, "(")),
                Token::new(Ident, Span::new(23, 2, "Human")),
                Token::new(CloseParen, Span::new(28, 2, ")")),
                Token::new(Dot, Span::new(29, 2, ".")),
                Token::new(Eof, Span::new(30, 2, "")),
            ]
        )
    }

    #[test]
    fn comment() {
        let mut lexer = Lexer::new("% Some Comment\n");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(Comment, Span::new(0, 1, "% Some Comment\n")),
                Token::new(Eof, Span::new(15, 2, ""))
            ]
        )
    }

    #[test]
    fn pct_enc_with_comment() {
        let mut lexer = Lexer::new("%38%a3% Some Comment\n");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(PctEncoded, Span::new(0, 1, "%38")),
                Token::new(PctEncoded, Span::new(3, 1, "%a3")),
                Token::new(Comment, Span::new(6, 1, "% Some Comment\n")),
                Token::new(Eof, Span::new(21, 2, "")),
            ]
        )
    }

    #[test]
    fn ident() {
        let mut lexer = Lexer::new("some_Ident(Alice). %comment at the end of a line\n");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(Ident, Span::new(0, 1, "some_Ident")),
                Token::new(OpenParen, Span::new(10, 1, "(")),
                Token::new(Ident, Span::new(11, 1, "Alice")),
                Token::new(CloseParen, Span::new(16, 1, ")")),
                Token::new(Dot, Span::new(17, 1, ".")),
                Token::new(Whitespace, Span::new(18, 1, " ")),
                Token::new(Comment, Span::new(19, 1, "%comment at the end of a line\n")),
                Token::new(Eof, Span::new(49, 2, "")),
            ]
        )
    }

    #[test]
    #[should_panic]
    fn forbidden_ident() {
        let mut lexer = Lexer::new("_someIdent(Alice). %comment at the end of a line\n");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(Ident, Span::new(0, 1, "_someIdent")),
                Token::new(OpenParen, Span::new(10, 1, "(")),
                Token::new(Ident, Span::new(11, 1, "Alice")),
                Token::new(CloseParen, Span::new(16, 1, ")")),
                Token::new(Dot, Span::new(17, 1, ".")),
                Token::new(Whitespace, Span::new(18, 1, " ")),
                Token::new(Comment, Span::new(19, 1, "%comment at the end of a line\n")),
                Token::new(Eof, Span::new(49, 2, "")),
            ]
        )
    }

    #[test]
    fn iri() {
        let mut lexer = Lexer::new("<https://résumé.example.org/>");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(Less, Span::new(0, 1, "<")),
                Token::new(Ident, Span::new(1, 1, "https")),
                Token::new(Colon, Span::new(6, 1, ":")),
                Token::new(Slash, Span::new(7, 1, "/")),
                Token::new(Slash, Span::new(8, 1, "/")),
                Token::new(Ident, Span::new(9, 1, "résumé")),
                Token::new(Dot, Span::new(17, 1, ".")),
                Token::new(Ident, Span::new(18, 1, "example")),
                Token::new(Dot, Span::new(25, 1, ".")),
                Token::new(Ident, Span::new(26, 1, "org")),
                Token::new(Slash, Span::new(29, 1, "/")),
                Token::new(Greater, Span::new(30, 1, ">")),
                Token::new(Eof, Span::new(31, 1, "")),
            ]
        )
    }

    #[test]
    fn iri_pct_enc() {
        let mut lexer = Lexer::new("<http://r%C3%A9sum%C3%A9.example.org>\n");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(Less, Span::new(0, 1, "<")),
                Token::new(Ident, Span::new(1, 1, "http")),
                Token::new(Colon, Span::new(5, 1, ":")),
                Token::new(Slash, Span::new(6, 1, "/")),
                Token::new(Slash, Span::new(7, 1, "/")),
                Token::new(Ident, Span::new(8, 1, "r")),
                Token::new(PctEncoded, Span::new(9, 1, "%C3")),
                Token::new(PctEncoded, Span::new(12, 1, "%A9")),
                Token::new(Ident, Span::new(15, 1, "sum")),
                Token::new(PctEncoded, Span::new(18, 1, "%C3")),
                Token::new(PctEncoded, Span::new(21, 1, "%A9")),
                Token::new(Dot, Span::new(24, 1, ".")),
                Token::new(Ident, Span::new(25, 1, "example")),
                Token::new(Dot, Span::new(32, 1, ".")),
                Token::new(Ident, Span::new(33, 1, "org")),
                Token::new(Greater, Span::new(36, 1, ">")),
                Token::new(Whitespace, Span::new(37, 1, "\n")),
                Token::new(Eof, Span::new(38, 2, "")),
            ]
        )
    }

    #[test]
    fn pct_enc_comment() {
        let mut lexer = Lexer::new("%d4 this should be a comment,\n% but the lexer can't distinguish a percent encoded value\n% in an iri from a comment :(\n");
        assert_eq!(
            lexer.get_tokens(),
            vec![
                Token::new(PctEncoded, Span::new(0, 1, "%d4")),
                Token::new(Whitespace, Span::new(3, 1, " ")),
                Token::new(Ident, Span::new(4, 1, "this")),
                Token::new(Whitespace, Span::new(8, 1, " ")),
                Token::new(Ident, Span::new(9, 1, "should")),
                Token::new(Whitespace, Span::new(15, 1, " ")),
                Token::new(Ident, Span::new(16, 1, "be")),
                Token::new(Whitespace, Span::new(18, 1, " ")),
                Token::new(Ident, Span::new(19, 1, "a")),
                Token::new(Whitespace, Span::new(20, 1, " ")),
                Token::new(Ident, Span::new(21, 1, "comment")),
                Token::new(Comma, Span::new(28, 1, ",")),
                Token::new(Whitespace, Span::new(29, 1, "\n")),
                Token::new(
                    Comment,
                    Span::new(
                        30,
                        2,
                        "% but the lexer can't distinguish a percent encoded value\n"
                    )
                ),
                Token::new(Comment, Span::new(88, 3, "% in an iri from a comment :(\n")),
                Token::new(Eof, Span::new(118, 4, "")),
            ]
        )
    }
}
