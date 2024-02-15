//! Lexical tokenization of rulewerk-style rules.

use std::str::Chars;

const EOF_CHAR: char = '\0';

#[derive(Debug)]
struct Lexer<'a> {
    chars: Chars<'a>,
}

impl Lexer<'_> {
    fn new(input: &str) -> Lexer {
        Lexer {
            chars: input.chars(),
        }
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
    fn advance_token(&mut self) -> TokenKind {
        use TokenKind::*;
        let first_char = match self.bump() {
            Some(c) => c,
            None => return Eof,
        };
        match first_char {
            '%' => match (self.peek(1), self.peek(2)) {
                (n1, n2) if n1.is_digit(16) && n2.is_digit(16) => self.pct_encoded(),
                _ => self.comment(),
            },
            '\n' => Whitespace(true),
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
            _ => todo!(),
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
        TokenKind::Comment
    }
    fn whitespace(&mut self) -> TokenKind {
        self.bump_while(|c| is_whitespace(c) && c != '\n');
        if '\n' == self.peek(1) {
            self.bump();
            return TokenKind::Whitespace(true);
        }
        TokenKind::Whitespace(false)
    }
    fn ident(&mut self) -> TokenKind {
        self.bump_while(unicode_ident::is_xid_continue);
        TokenKind::Ident
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
    Utf8Chars,
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
    Whitespace(bool),
    /// catch all token
    Illegal,
    /// signals end of file
    Eof,
}

#[cfg(test)]
mod test {
    use super::TokenKind::*;
    use crate::io::lexer::Lexer;

    #[test]
    fn tokenize() {
        assert_eq!(
            {
                let mut vec = vec![];
                let mut lexer = Lexer::new("P(?X) :- A(?X).\t\n    A(Human).");
                loop {
                    let tok = lexer.advance_token();
                    vec.push(tok.clone());
                    if tok == Eof {
                        break;
                    }
                }
                vec
            },
            vec![
                Ident,
                OpenParen,
                QuestionMark,
                Ident,
                CloseParen,
                Whitespace(false),
                Colon,
                Minus,
                Whitespace(false),
                Ident,
                OpenParen,
                QuestionMark,
                Ident,
                CloseParen,
                Dot,
                Whitespace(true),
                Whitespace(false),
                Ident,
                OpenParen,
                Ident,
                CloseParen,
                Dot,
                Eof
            ]
        )
    }

    #[test]
    fn comment() {
        assert_eq!(
            {
                let mut vec = vec![];
                let mut lexer = Lexer::new("% Some Comment\n");
                loop {
                    let tok = lexer.advance_token();
                    vec.push(tok.clone());
                    if tok == Eof {
                        break;
                    }
                }
                vec
            },
            vec![Comment, Eof]
        )
    }

    #[test]
    fn pct_enc_with_comment() {
        assert_eq!(
            {
                let mut vec = vec![];
                let mut lexer = Lexer::new("%38%a3% Some Comment\n");
                loop {
                    let tok = lexer.advance_token();
                    vec.push(tok.clone());
                    if tok == Eof {
                        break;
                    }
                }
                vec
            },
            vec![PctEncoded, PctEncoded, Comment, Eof]
        )
    }

    #[test]
    fn ident() {
        assert_eq!(
            {
                let mut vec = vec![];
                let mut lexer = Lexer::new("some_Ident(Alice). %comment at the end of a line\n");
                loop {
                    let tok = lexer.advance_token();
                    vec.push(tok.clone());
                    if tok == Eof {
                        break;
                    }
                }
                vec
            },
            vec![
                Ident,
                OpenParen,
                Ident,
                CloseParen,
                Dot,
                Whitespace(false),
                Comment,
                Eof
            ]
        )
    }

    #[test]
    #[should_panic]
    fn forbidden_ident() {
        assert_eq!(
            {
                let mut vec = vec![];
                let mut lexer = Lexer::new("_someIdent(Alice). %comment at the end of a line\n");
                loop {
                    let tok = lexer.advance_token();
                    vec.push(tok.clone());
                    if tok == Eof {
                        break;
                    }
                }
                vec
            },
            vec![
                Ident,
                OpenParen,
                Ident,
                CloseParen,
                Dot,
                Whitespace(false),
                Comment,
                Eof
            ]
        )
    }

    #[test]
    fn iri() {
        assert_eq!(
            {
                let mut vec = vec![];
                let mut lexer = Lexer::new("<https://résumé.example.org/>");
                loop {
                    let tok = lexer.advance_token();
                    vec.push(tok.clone());
                    if tok == Eof {
                        break;
                    }
                }
                vec
            },
            vec![
                Less, Ident, Colon, Slash, Slash, Ident, Dot, Ident, Dot, Ident, Slash, Greater,
                Eof
            ]
        )
    }

    #[test]
    fn iri_pct_enc() {
        assert_eq!(
            {
                let mut vec = vec![];
                let mut lexer = Lexer::new("<http://r%C3%A9sum%C3%A9.example.org>\n");
                loop {
                    let tok = lexer.advance_token();
                    vec.push(tok.clone());
                    if tok == Eof {
                        break;
                    }
                }
                vec
            },
            vec![
                Less,
                Ident,
                Colon,
                Slash,
                Slash,
                Ident,
                PctEncoded,
                PctEncoded,
                Ident,
                PctEncoded,
                PctEncoded,
                Dot,
                Ident,
                Dot,
                Ident,
                Greater,
                Whitespace(true),
                Eof
            ]
        )
    }

    #[test]
    fn pct_enc_comment() {
        assert_eq!(
            {
                let mut vec = vec![];
                let mut lexer = Lexer::new("%d4 this should be a comment,\n% but the lexer can't distinguish a percent encoded value\n% in an iri from a comment :(\n");
                loop {
                    let tok = lexer.advance_token();
                    vec.push(tok.clone());
                    if tok == Eof {
                        break;
                    }
                }
                vec
            },
            vec![
                PctEncoded,
                Whitespace(false),
                Ident,
                Whitespace(false),
                Ident,
                Whitespace(false),
                Ident,
                Whitespace(false),
                Ident,
                Whitespace(false),
                Ident,
                Comma,
                Whitespace(true),
                Comment,
                Comment,
                Eof
            ]
        )
    }
}
