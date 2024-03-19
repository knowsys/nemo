//! Lexical tokenization of rulewerk-style rules.

use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, take},
    character::complete::{alpha1, alphanumeric1, digit1, line_ending, multispace1},
    combinator::{all_consuming, map, recognize},
    multi::many0,
    sequence::{delimited, pair, tuple},
    IResult,
};
use nom_locate::LocatedSpan;

pub(crate) type Span<'a> = LocatedSpan<&'a str>;

/// All the tokens the input gets parsed into.
#[derive(Debug, PartialEq, Copy, Clone)]
pub(crate) enum TokenKind {
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
    /// `:-`
    Arrow,
    /// '>'
    Greater,
    /// `>=`
    GreaterEqual,
    /// '='
    Equal,
    /// `<=`
    LessEqual,
    /// '<'
    Less,
    /// `!=`
    Unequal,
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
    // Multi-char tokens:
    /// Identifier for keywords and names
    Ident,
    /// Variable,
    Variable,
    /// IRI, delimited with `<` and `>`
    Iri,
    /// Base 10 digits
    Number,
    /// A string literal, delimited with `"`
    String,
    /// A comment, starting with `%`
    Comment,
    /// A comment, starting with `%%`
    DocComment,
    /// ` `, `\t`, `\r` or `\n`
    Whitespace,
    /// base directive keyword
    Base,
    /// prefix directive keyword
    Prefix,
    /// import directive keyword
    Import,
    /// export directive keyword
    Export,
    /// output directive keyword
    Output,
    /// catch all token
    Illegal,
    /// signals end of file
    Eof,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct Token<'a> {
    pub(crate) kind: TokenKind,
    pub(crate) span: Span<'a>,
}
impl<'a> Token<'a> {
    fn new(kind: TokenKind, span: Span<'a>) -> Token<'a> {
        Token { kind, span }
    }
}

macro_rules! syntax {
    ($func_name: ident, $tag_string: literal, $token: expr) => {
        pub(crate) fn $func_name<'a>(input: Span) -> IResult<Span, Token> {
            map(tag($tag_string), |span| Token::new($token, span))(input)
        }
    };
}

syntax!(open_paren, "(", TokenKind::OpenParen);
syntax!(close_paren, ")", TokenKind::CloseParen);
syntax!(open_bracket, "[", TokenKind::OpenBracket);
syntax!(close_bracket, "]", TokenKind::CloseBracket);
syntax!(open_brace, "{", TokenKind::OpenBrace);
syntax!(close_brace, "}", TokenKind::CloseBrace);
syntax!(dot, ".", TokenKind::Dot);
syntax!(comma, ",", TokenKind::Comma);
syntax!(colon, ":", TokenKind::Colon);
syntax!(arrow, ":-", TokenKind::Arrow);
syntax!(question_mark, "?", TokenKind::QuestionMark);
syntax!(exclamation_mark, "!", TokenKind::ExclamationMark);
syntax!(tilde, "~", TokenKind::Tilde);
syntax!(caret, "^", TokenKind::Caret);
syntax!(hash, "#", TokenKind::Hash);
syntax!(underscore, "_", TokenKind::Underscore);
syntax!(at, "@", TokenKind::At);

pub(crate) fn lex_punctuations(input: Span) -> IResult<Span, Token> {
    alt((
        arrow,
        open_paren,
        close_paren,
        open_bracket,
        close_bracket,
        open_brace,
        close_brace,
        dot,
        comma,
        colon,
        question_mark,
        exclamation_mark,
        tilde,
        caret,
        hash,
        underscore,
        at,
    ))(input)
}

syntax!(less, "<", TokenKind::Less);
syntax!(less_equal, "<=", TokenKind::LessEqual);
syntax!(equal, "=", TokenKind::Equal);
syntax!(greater_equal, ">=", TokenKind::GreaterEqual);
syntax!(greater, ">", TokenKind::Greater);
syntax!(unequal, "!=", TokenKind::Unequal);
syntax!(plus, "+", TokenKind::Plus);
syntax!(minus, "-", TokenKind::Minus);
syntax!(star, "*", TokenKind::Star);
syntax!(slash, "/", TokenKind::Slash);

pub(crate) fn lex_operators(input: Span) -> IResult<Span, Token> {
    alt((
        less_equal,
        greater_equal,
        unequal,
        less,
        equal,
        greater,
        plus,
        minus,
        star,
        slash,
    ))(input)
}

pub(crate) fn lex_ident(input: Span) -> IResult<Span, Token> {
    let (rest, result) = recognize(pair(
        alpha1,
        many0(alt((alphanumeric1, tag("_"), tag("-")))),
    ))(input)?;
    let token = match *result.fragment() {
        "base" => Token::new(TokenKind::Base, result),
        "prefix" => Token::new(TokenKind::Prefix, result),
        "import" => Token::new(TokenKind::Import, result),
        "export" => Token::new(TokenKind::Export, result),
        "output" => Token::new(TokenKind::Output, result),
        _ => Token::new(TokenKind::Ident, result),
    };
    Ok((rest, token))
}

pub(crate) fn lex_iri(input: Span) -> IResult<Span, Token> {
    recognize(delimited(tag("<"), is_not("> \n"), tag(">")))(input)
        .map(|(rest, result)| (rest, Token::new(TokenKind::Iri, result)))
}

pub(crate) fn lex_number(input: Span) -> IResult<Span, Token> {
    digit1(input).map(|(rest, result)| (rest, Token::new(TokenKind::Number, result)))
}

pub(crate) fn lex_string(input: Span) -> IResult<Span, Token> {
    recognize(delimited(tag("\""), is_not("\""), tag("\"")))(input)
        .map(|(rest, result)| (rest, Token::new(TokenKind::String, result)))
}

pub(crate) fn lex_comment(input: Span) -> IResult<Span, Token> {
    recognize(tuple((tag("%"), many0(is_not("\r\n")), line_ending)))(input)
        .map(|(rest, result)| (rest, Token::new(TokenKind::Comment, result)))
}

pub(crate) fn lex_doc_comment(input: Span) -> IResult<Span, Token> {
    recognize(tuple((tag("%%"), many0(is_not("\r\n")), line_ending)))(input)
        .map(|(rest, result)| (rest, Token::new(TokenKind::DocComment, result)))
}

pub(crate) fn lex_whitespace(input: Span) -> IResult<Span, Token> {
    multispace1(input).map(|(rest, result)| (rest, Token::new(TokenKind::Whitespace, result)))
}

pub(crate) fn lex_illegal(input: Span) -> IResult<Span, Token> {
    take(1usize)(input).map(|(rest, result)| (rest, Token::new(TokenKind::Illegal, result)))
}

pub(crate) fn lex_tokens(input: Span) -> IResult<Span, Vec<Token>> {
    all_consuming(many0(alt((
        lex_iri,
        lex_operators,
        lex_punctuations,
        lex_ident,
        lex_number,
        lex_string,
        lex_comment,
        lex_whitespace,
        lex_illegal,
    ))))(input)
    .map(|(span, mut vec)| {
        vec.append(&mut vec![Token::new(TokenKind::Eof, span)]);
        (span, vec)
    })
}

#[cfg(test)]
mod test {
    use super::TokenKind::*;
    use super::*;

    macro_rules! T {
        ($tok_kind: expr, $offset: literal, $line: literal, $str: literal) => {
            Token::new($tok_kind, unsafe {
                Span::new_from_raw_offset($offset, $line, $str, ())
            })
        };
    }

    #[test]
    fn empty_input() {
        let input = Span::new("");
        assert_eq!(lex_tokens(input).unwrap().1, vec![T!(Eof, 0, 1, "")])
    }

    #[test]
    fn base() {
        let input = Span::new("@base");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![T!(At, 0, 1, "@"), T!(Base, 1, 1, "base"), T!(Eof, 5, 1, ""),]
        )
    }

    #[test]
    fn prefix() {
        let input = Span::new("@prefix");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(At, 0, 1, "@"),
                T!(Prefix, 1, 1, "prefix"),
                T!(Eof, 7, 1, ""),
            ]
        )
    }

    #[test]
    fn output() {
        let input = Span::new("@output");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(At, 0, 1, "@"),
                T!(Output, 1, 1, "output"),
                T!(Eof, 7, 1, ""),
            ]
        )
    }

    #[test]
    fn import() {
        let input = Span::new("@import");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(At, 0, 1, "@"),
                T!(Import, 1, 1, "import"),
                T!(Eof, 7, 1, ""),
            ]
        )
    }

    #[test]
    fn export() {
        let input = Span::new("@export");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(At, 0, 1, "@"),
                T!(Export, 1, 1, "export"),
                T!(Eof, 7, 1, ""),
            ]
        )
    }

    #[test]
    fn idents_with_keyword_prefix() {
        let input = Span::new("@baseA, @prefixB, @importC, @exportD, @outputE.");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(At, 0, 1, "@"),
                T!(Ident, 1, 1, "baseA"),
                T!(Comma, 6, 1, ","),
                T!(Whitespace, 7, 1, " "),
                T!(At, 8, 1, "@"),
                T!(Ident, 9, 1, "prefixB"),
                T!(Comma, 16, 1, ","),
                T!(Whitespace, 17, 1, " "),
                T!(At, 18, 1, "@"),
                T!(Ident, 19, 1, "importC"),
                T!(Comma, 26, 1, ","),
                T!(Whitespace, 27, 1, " "),
                T!(At, 28, 1, "@"),
                T!(Ident, 29, 1, "exportD"),
                T!(Comma, 36, 1, ","),
                T!(Whitespace, 37, 1, " "),
                T!(At, 38, 1, "@"),
                T!(Ident, 39, 1, "outputE"),
                T!(Dot, 46, 1, "."),
                T!(Eof, 47, 1, ""),
            ]
        )
    }

    #[test]
    fn tokenize() {
        let input = Span::new("P(?X) :- A(?X).\t\n    A(Human).");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(Ident, 0, 1, "P"),
                T!(OpenParen, 1, 1, "("),
                T!(QuestionMark, 2, 1, "?"),
                T!(Ident, 3, 1, "X"),
                T!(CloseParen, 4, 1, ")"),
                T!(Whitespace, 5, 1, " "),
                T!(Arrow, 6, 1, ":-"),
                T!(Whitespace, 8, 1, " "),
                T!(Ident, 9, 1, "A"),
                T!(OpenParen, 10, 1, "("),
                T!(QuestionMark, 11, 1, "?"),
                T!(Ident, 12, 1, "X"),
                T!(CloseParen, 13, 1, ")"),
                T!(Dot, 14, 1, "."),
                T!(Whitespace, 15, 1, "\t\n    "),
                T!(Ident, 21, 2, "A"),
                T!(OpenParen, 22, 2, "("),
                T!(Ident, 23, 2, "Human"),
                T!(CloseParen, 28, 2, ")"),
                T!(Dot, 29, 2, "."),
                T!(Eof, 30, 2, ""),
            ]
        )
    }

    #[test]
    fn comment() {
        let input = Span::new("% Some Comment\n");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(Comment, 0, 1, "% Some Comment\n"),
                T!(Eof, 15, 2, ""),
                // T!(Comment, Span::new(0, 1, "% Some Comment\n")),
                // T!(Eof, Span::new(15, 2, ""))
            ]
        )
    }

    #[test]
    fn ident() {
        let input = Span::new("some_Ident(Alice). %comment at the end of a line\n");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(Ident, 0, 1, "some_Ident"),
                T!(OpenParen, 10, 1, "("),
                T!(Ident, 11, 1, "Alice"),
                T!(CloseParen, 16, 1, ")"),
                T!(Dot, 17, 1, "."),
                T!(Whitespace, 18, 1, " "),
                T!(Comment, 19, 1, "%comment at the end of a line\n"),
                T!(Eof, 49, 2, ""),
            ]
        )
    }

    #[test]
    fn forbidden_ident() {
        let input = Span::new("_someIdent(Alice). %comment at the end of a line\n");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(Underscore, 0, 1, "_"),
                T!(Ident, 1, 1, "someIdent"),
                T!(OpenParen, 10, 1, "("),
                T!(Ident, 11, 1, "Alice"),
                T!(CloseParen, 16, 1, ")"),
                T!(Dot, 17, 1, "."),
                T!(Whitespace, 18, 1, " "),
                T!(Comment, 19, 1, "%comment at the end of a line\n"),
                T!(Eof, 49, 2, ""),
            ]
        )
    }

    #[test]
    fn iri() {
        let input = Span::new("<https://résumé.example.org/>");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(Iri, 0, 1, "<https://résumé.example.org/>"),
                T!(Eof, 31, 1, ""),
            ]
        )
    }

    #[test]
    fn iri_pct_enc() {
        let input = Span::new("<http://r%C3%A9sum%C3%A9.example.org>\n");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(Iri, 0, 1, "<http://r%C3%A9sum%C3%A9.example.org>"),
                T!(Whitespace, 37, 1, "\n"),
                T!(Eof, 38, 2, ""),
            ]
        )
    }

    // FIXME: change the name of this test according to the correct name for `?X > 3`
    // (Constraints are Rules with an empty Head)
    #[test]
    fn constraints() {
        let input = Span::new("A(?X):-B(?X),?X<42,?X>3.");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(Ident, 0, 1, "A"),
                T!(OpenParen, 1, 1, "("),
                T!(QuestionMark, 2, 1, "?"),
                T!(Ident, 3, 1, "X"),
                T!(CloseParen, 4, 1, ")"),
                T!(Arrow, 5, 1, ":-"),
                T!(Ident, 7, 1, "B"),
                T!(OpenParen, 8, 1, "("),
                T!(QuestionMark, 9, 1, "?"),
                T!(Ident, 10, 1, "X"),
                T!(CloseParen, 11, 1, ")"),
                T!(Comma, 12, 1, ","),
                T!(QuestionMark, 13, 1, "?"),
                T!(Ident, 14, 1, "X"),
                T!(Less, 15, 1, "<"),
                T!(Number, 16, 1, "42"),
                T!(Comma, 18, 1, ","),
                T!(QuestionMark, 19, 1, "?"),
                T!(Ident, 20, 1, "X"),
                T!(Greater, 21, 1, ">"),
                T!(Number, 22, 1, "3"),
                T!(Dot, 23, 1, "."),
                T!(Eof, 24, 1, ""),
            ]
        )
    }

    #[test]
    fn pct_enc_comment() {
        let input = Span::new("%d4 this should be a comment,\n% but the lexer can't distinguish a percent encoded value\n% in an iri from a comment :(\n");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(Comment, 0, 1, "%d4 this should be a comment,\n"),
                T!(
                    Comment,
                    30,
                    2,
                    "% but the lexer can't distinguish a percent encoded value\n"
                ),
                T!(Comment, 88, 3, "% in an iri from a comment :(\n"),
                T!(Eof, 118, 4, ""),
            ]
        )
    }

    #[test]
    fn fact() {
        let input = Span::new("somePred(term1, term2).");
        assert_eq!(
            lex_tokens(input).unwrap().1,
            vec![
                T!(Ident, 0, 1, "somePred"),
                T!(OpenParen, 8, 1, "("),
                T!(Ident, 9, 1, "term1"),
                T!(Comma, 14, 1, ","),
                T!(Whitespace, 15, 1, " "),
                T!(Ident, 16, 1, "term2"),
                T!(CloseParen, 21, 1, ")"),
                T!(Dot, 22, 1, "."),
                T!(Eof, 23, 1, ""),
            ]
        )
    }
}
