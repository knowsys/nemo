//! This module defines [Token].
#![allow(missing_docs)]

use enum_assoc::Assoc;

use nom::{
    branch::alt,
    bytes::complete::{is_not, tag},
    character::complete::{alpha1, alphanumeric1, digit1, multispace1},
    combinator::{map, recognize},
    multi::many0,
    sequence::pair,
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
    /// Comma
    #[assoc(name = ",")]
    Comma,
    /// Arrow, used to separate rules
    #[assoc(name = ":-")]
    Arrow,
    /// Colon
    #[assoc(name = ":")]
    Colon,
    /// Double Colon
    #[assoc(name = "::")]
    DoubleColon,
    /// Semicolon
    #[assoc(name = ";")]
    Semicolon,
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
    /// Unequal
    #[assoc(name = "!=")]
    Unequal,
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
    /// Blank node prefix
    #[assoc(name = "_:")]
    BlankNodePrefix,
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
    /// String
    #[assoc(name = "string")]
    String,
    /// Token marking a normal comment
    #[assoc(name = "//")]
    Comment,
    /// Token marking the beginning of a closed comment
    #[assoc(name = "/*")]
    OpenComment,
    /// Token marking the beginning of a closed comment
    #[assoc(name = "*/")]
    CloseComment,
    /// Token marking a doc comment attached to e.g. a rule
    #[assoc(name = "///")]
    DocComment,
    /// Token marking the top level comment
    #[assoc(name = "//!")]
    TopLevelComment,
    /// Token for the base directive
    #[assoc(name = "base")]
    BaseDirective,
    /// Token for the declare directive
    #[assoc(name = "declare")]
    DeclareDirective,
    /// Token for the export directive
    #[assoc(name = "export")]
    ExportDirective,
    /// Token for the import directive
    #[assoc(name = "import")]
    ImportDirective,
    /// Token for the output directive
    #[assoc(name = "output")]
    OutputDirective,
    /// Token for the prefix directive
    #[assoc(name = "prefix")]
    PrefixDirective,
    /// White spaces
    #[assoc(name = "whitespace")]
    Whitespace,
    /// Double new line
    #[assoc(name = "double newline")]
    DoubleNewline,
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

impl<'a> Token<'a> {
    /// Return a copy of the underlying text
    pub fn to_string(&self) -> String {
        self.span.0.to_string()
    }
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
    /// Return the [ProgramSpan] of this token.
    pub fn span(&self) -> ProgramSpan<'a> {
        self.span
    }

    /// Return the [TokenKind] of this token.
    pub fn kind(&self) -> TokenKind {
        self.kind
    }

    /// Parse [TokenKind::Name].
    pub fn name(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
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
    pub fn iri(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        is_not("> \n")(input).map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::Iri,
                },
            )
        })
    }

    /// Parse [TokenKind::String].
    pub fn string(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        is_not("\"")(input).map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::String,
                },
            )
        })
    }

    /// Parse [TokenKind::Digits].
    pub fn digits(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
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

    /// Parse [TokenKind::Whitespace].
    pub fn whitespace(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(ParserContext::token(TokenKind::Whitespace), multispace1)(input).map(
            |(rest_input, result)| {
                (
                    rest_input,
                    Token {
                        span: result.span,
                        kind: TokenKind::Whitespace,
                    },
                )
            },
        )
    }

    /// Parse [TokenKind::DoubleNewline].
    pub fn double_newline(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(
            ParserContext::token(TokenKind::DoubleNewline),
            alt((tag("\n\n"), tag("\r\n\r\n"), tag("\r\r"))),
        )(input)
        .map(|(rest_input, result)| {
            (
                rest_input,
                Token {
                    span: result.span,
                    kind: TokenKind::DoubleNewline,
                },
            )
        })
    }

    /// Create [TokenKind::Error].
    pub fn error(span: ProgramSpan<'a>) -> Token<'a> {
        Token {
            span,
            kind: TokenKind::Error,
        }
    }

    string_token!(open_parenthesis, TokenKind::OpenParenthesis);
    string_token!(closed_parenthesis, TokenKind::ClosedParenthesis);
    string_token!(open_brace, TokenKind::OpenBrace);
    string_token!(closed_brace, TokenKind::ClosedBrace);
    string_token!(open_chevrons, TokenKind::OpenChevrons);
    string_token!(closed_chevrons, TokenKind::ClosedChevrons);
    string_token!(open_bracket, TokenKind::OpenBracket);
    string_token!(closed_bracket, TokenKind::ClosedBracket);
    string_token!(question_mark, TokenKind::QuestionMark);
    string_token!(exclamation_mark, TokenKind::ExclamationMark);
    string_token!(dot, TokenKind::Dot);
    string_token!(comma, TokenKind::Comma);
    string_token!(arrow, TokenKind::Arrow);
    string_token!(colon, TokenKind::Colon);
    string_token!(double_colon, TokenKind::DoubleColon);
    string_token!(semicolon, TokenKind::Semicolon);
    string_token!(greater, TokenKind::Greater);
    string_token!(greater_equal, TokenKind::GreaterEqual);
    string_token!(less, TokenKind::Less);
    string_token!(less_equal, TokenKind::LessEqual);
    string_token!(equal, TokenKind::Equal);
    string_token!(unequal, TokenKind::Unequal);
    string_token!(tilde, TokenKind::Tilde);
    string_token!(double_caret, TokenKind::DoubleCaret);
    string_token!(hash, TokenKind::Hash);
    string_token!(underscore, TokenKind::Underscore);
    string_token!(at, TokenKind::At);
    string_token!(plus, TokenKind::Plus);
    string_token!(minus, TokenKind::Minus);
    string_token!(star, TokenKind::Star);
    string_token!(division, TokenKind::Division);
    string_token!(boolean_true, TokenKind::True);
    string_token!(boolean_false, TokenKind::False);
    string_token!(comment, TokenKind::Comment);
    string_token!(open_comment, TokenKind::OpenComment);
    string_token!(close_comment, TokenKind::CloseComment);
    string_token!(doc_comment, TokenKind::DocComment);
    string_token!(toplevel_comment, TokenKind::TopLevelComment);
    string_token!(quote, TokenKind::Quote);
    string_token!(blank_node_prefix, TokenKind::BlankNodePrefix);
    string_token!(exponent_lower, TokenKind::ExponentLower);
    string_token!(exponent_upper, TokenKind::ExponentUpper);
    string_token!(type_marker_double, TokenKind::TypeMarkerDouble);
    string_token!(type_marker_float, TokenKind::TypeMarkerFloat);
    string_token!(directive_base, TokenKind::BaseDirective);
    string_token!(directive_declare, TokenKind::DeclareDirective);
    string_token!(directive_export, TokenKind::ExportDirective);
    string_token!(directive_import, TokenKind::ImportDirective);
    string_token!(directive_output, TokenKind::OutputDirective);
    string_token!(directive_prefix, TokenKind::PrefixDirective);
}
