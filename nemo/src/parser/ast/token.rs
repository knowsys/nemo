//! This module defines [Token].
#![allow(missing_docs)]

use enum_assoc::Assoc;

use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, take, take_till},
    character::complete::{alpha1, alphanumeric1, digit1, line_ending, multispace0, multispace1},
    combinator::{all_consuming, cut, map, opt, recognize},
    error::ParseError,
    multi::{many0, many1},
    sequence::{delimited, pair, tuple},
    IResult,
};

use crate::parser::{
    context::{context, ParserContext},
    span::ProgramSpan,
    ParserInput, ParserResult,
};

/// Enumeration of all accepted kinds of [Token]s
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn name(&self) -> &'static str)]
pub enum TokenKind {
    /// Question mark, used to mark universal variables
    #[assoc(name = "?")]
    QuestionMark,
    /// Exclamation mark, used to mark existential variables
    #[assoc(name = "!")]
    ExclamationMark,
    /// Open parenthesis
    #[assoc(name = "(")]
    OpenParenthesis,
    /// Closed parenthesis
    #[assoc(name = ")")]
    ClosedParenthesis,
    /// Open bracket
    #[assoc(name = "[")]
    OpenBracket,
    /// Closed bracket
    #[assoc(name = "]")]
    ClosedBracket,
    /// Open brace
    #[assoc(name = "{")]
    OpenBrace,
    /// Closed brace
    #[assoc(name = "}")]
    ClosedBrace,
    /// Open Chevrons
    #[assoc(name = "<")]
    OpenChevrons,
    /// Closed Chevrons
    #[assoc(name = ">")]
    ClosedChevrons,
    /// Dot
    #[assoc(name = ".")]
    Dot,
    /// Arrow, used to separate rules
    #[assoc(name = ":-")]
    Arrow,
    /// Colon
    #[assoc(name = ":")]
    Colon,
    /// Greater than
    #[assoc(name = ">")]
    Greater,
    /// Greater than or equal
    #[assoc(name = ">=")]
    GreaterEqual,
    /// Less than
    #[assoc(name = "<")]
    Less,
    /// Less than or equal
    #[assoc(name = "<=")]
    LessEqual,
    /// Equal
    #[assoc(name = "=")]
    Equal,
    /// Tilde, used for negation
    #[assoc(name = "~")]
    Tilde,
    /// Double caret
    #[assoc(name = "^^")]
    DoubleCaret,
    /// Hash, used in front of aggregates
    #[assoc(name = "#")]
    Hash,
    /// Underscore, used for anonymous variables
    #[assoc(name = "_")]
    Underscore,
    /// At, used to indicate directives
    #[assoc(name = "@")]
    At,
    /// Plus
    #[assoc(name = "+")]
    Plus,
    /// Minus
    #[assoc(name = "-")]
    Minus,
    /// Star
    #[assoc(name = "*")]
    Star,
    /// Division
    #[assoc(name = "/")]
    Division,
    /// True
    #[assoc(name = "true")]
    True,
    /// False
    #[assoc(name = "false")]
    False,
    /// Quote
    #[assoc(name = "\"")]
    Quote,
    /// Blank node label
    #[assoc(name = "_:")]
    BlankNodeLabel,
    /// Name
    #[assoc(name = "name")]
    Name,
    /// Digits
    #[assoc(name = "digits")]
    Digits,
    /// Exponent (lower case)
    #[assoc(name = "e")]
    ExponentLower,
    /// Exponent (upper case)
    #[assoc(name = "E")]
    ExponentUpper,
    /// Marker float
    #[assoc(name = "f")]
    TypeMarkerFloat,
    /// Marker double
    #[assoc(name = "d")]
    TypeMarkerDouble,
    /// IRI
    #[assoc(name = "iri")]
    Iri,
    /// A comment (as single token)
    #[assoc(name = "comment")]
    Comment,
    /// A doc comment attached to e.g. a rule
    #[assoc(name = "doc-comment")]
    DocComment,
    /// Toplevel comment describing the rule file
    #[assoc(name = "top-level-comment")]
    TopLevelComment,
    /// White spaces
    #[assoc(name = "whitespace")]
    Whitespace,
    /// End of file
    #[assoc(name = "end-of-file")]
    EndOfFile,
    /// Token that captures errors
    #[assoc(name = "error")]
    Error,
}

/// A token is the smallest unit recognized by the parser
/// that is used to built up more complex expressions
#[derive(Debug)]
pub struct Token<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// The kind of token
    kind: TokenKind,
}

/// Macro for generating token parser functions
macro_rules! string_token {
    ($func_name: ident, $token: expr) => {
        /// Parse this token.
        pub fn $func_name(input: ParserInput<'a>) -> ParserResult<'a, Token> {
            map(
                context(ParserContext::Token { kind: $token }, tag($token.name())),
                |input: ParserInput| Token {
                    span: input.span,
                    kind: $token,
                },
            )(input)
        }
    };
}

impl<'a> Token<'a> {
    /// Return the [TokenKind] of this token.
    pub fn kind(&self) -> TokenKind {
        self.kind
    }

    /// Parse [TokenKind::Name].
    pub fn name(input: ParserInput<'a>) -> ParserResult<'a, Token> {
        context(
            ParserContext::token(TokenKind::Name),
            recognize(pair(
                alpha1,
                many0(alt((alphanumeric1, tag("_"), tag("-")))),
            )),
        )(input)
        .map(|(rest_input, result)| {
            (
                rest_input,
                Token {
                    span: result.span,
                    kind: TokenKind::Name,
                },
            )
        })
    }

    /// Parse [TokenKind::Iri].
    pub fn iri(input: ParserInput<'a>) -> ParserResult<'a, Token> {
        context(
            ParserContext::token(TokenKind::Iri),
            recognize(delimited(tag("<"), is_not("> \n"), cut(tag(">")))),
        )(input)
        .map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::Iri,
                },
            )
        })
    }

    /// Parse [TokenKind::Digits].
    pub fn digits(input: ParserInput<'a>) -> ParserResult<'a, Token> {
        context(ParserContext::token(TokenKind::Digits), digit1)(input).map(
            |(rest_input, result)| {
                (
                    rest_input,
                    Token {
                        span: result.span,
                        kind: TokenKind::Digits,
                    },
                )
            },
        )
    }

    string_token!(open_parenthesis, TokenKind::OpenParenthesis);
    string_token!(closed_parenthesis, TokenKind::ClosedParenthesis);
    string_token!(open_brace, TokenKind::OpenBrace);
    string_token!(closed_brace, TokenKind::ClosedBrace);
    string_token!(open_chevrons, TokenKind::OpenChevrons);
    string_token!(closed_chevrons, TokenKind::ClosedChevrons);
    string_token!(open_bracket, TokenKind::OpenBracket);
    string_token!(closed_bracket, TokenKind::ClosedBrace);
    string_token!(question_mark, TokenKind::QuestionMark);
    string_token!(exclamation_mark, TokenKind::ExclamationMark);
    string_token!(dot, TokenKind::Dot);
    string_token!(greater, TokenKind::Greater);
    string_token!(greater_equal, TokenKind::GreaterEqual);
    string_token!(less, TokenKind::Less);
    string_token!(less_equal, TokenKind::LessEqual);
    string_token!(equal, TokenKind::Equal);
    string_token!(tilde, TokenKind::Tilde);
    string_token!(double_caret, TokenKind::DoubleCaret);
    string_token!(hash, TokenKind::Hash);
    string_token!(underscore, TokenKind::Underscore);
    string_token!(at, TokenKind::At);
    string_token!(plus, TokenKind::Plus);
    string_token!(minus, TokenKind::Minus);
    string_token!(star, TokenKind::Star);
    string_token!(division, TokenKind::Division);
    string_token!(quote, TokenKind::Quote);
    string_token!(blank_node_label, TokenKind::BlankNodeLabel);
    string_token!(exponent_lower, TokenKind::ExponentLower);
    string_token!(exponent_upper, TokenKind::ExponentUpper);
    string_token!(type_marker_double, TokenKind::TypeMarkerDouble);
    string_token!(type_marker_float, TokenKind::TypeMarkerFloat);
}
