//! This module defines [Token].
#![allow(missing_docs)]

use std::fmt::Display;

use enum_assoc::Assoc;

use nom::{
    branch::alt,
    bytes::complete::{is_a, is_not, tag},
    character::complete::{alpha1, alphanumeric1, digit1, multispace1, space0, space1},
    combinator::{map, opt, recognize, verify},
    multi::many0,
    sequence::pair,
};

use crate::{
    parser::{
        context::{context, ParserContext},
        span::Span,
        ParserInput, ParserResult,
    },
    syntax::{
        self, comment,
        datavalues::{self, boolean, iri, map, string, tuple, RDF_DATATYPE_INDICATOR},
        directive,
        expression::{aggregate, atom, format_string, operation, variable},
        operator, rule,
    },
};

/// Enumeration of all accepted kinds of [Token]s
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn name(&self) -> &'static str)]
pub enum TokenKind {
    /// Opening parenthesis for parenthesised arithmitic terms
    #[assoc(name = "(")]
    OpenParenthesis,
    /// Closing parenthesis for parenthesised arithmitic terms
    #[assoc(name = ")")]
    ClosedParenthesis,
    /// Opening delimiter for maps
    #[assoc(name = map::OPEN)]
    MapOpen,
    /// Closing delimiter for maps
    #[assoc(name = map::CLOSE)]
    MapClose,
    /// Opening delimiter for operations
    #[assoc(name = operation::OPEN)]
    OperationOpen,
    /// Closing delimiter for operations
    #[assoc(name = operation::CLOSE)]
    OperationClose,
    /// Opening delimiter for tuples
    #[assoc(name = tuple::OPEN)]
    TupleOpen,
    /// Closing delimiter for tuples
    #[assoc(name = tuple::CLOSE)]
    TupleClose,
    /// [UNIVERSAL_INDICATOR](variable::UNIVERSAL_INDICATOR), used to mark universal variables
    #[assoc(name = variable::UNIVERSAL_INDICATOR)]
    UniversalIndicator,
    /// [EXISTENTIAL_INDICATOR](variable::EXISTENTIAL_INDICATOR), used to mark existential variables
    #[assoc(name = variable::EXISTENTIAL_INDICATOR)]
    ExistentialIndicator,
    /// Opening delimiter for term sequence of atoms
    #[assoc(name = atom::OPEN)]
    AtomOpen,
    /// Closing delimiter for term sequence of atoms
    #[assoc(name = atom::CLOSE)]
    AtomClose,
    /// Opening delimiter for IRIs
    #[assoc(name = iri::OPEN)]
    IriOpen,
    /// Closing delimiter for IRIs
    #[assoc(name = iri::CLOSE)]
    IriClose,
    /// Separator for namespaces as defined in [NAMESPACE_SEPARATOR](directive::NAMESPACE_SEPARATOR)
    #[assoc(name = directive::NAMESPACE_SEPARATOR)]
    NamespaceSeparator,
    /// Sequence separator as defined in [SEQUENCE_SEPARATOR](syntax::SEQUENCE_SEPARATOR)
    #[assoc(name = syntax::SEQUENCE_SEPARATOR)]
    SequenceSeparator,
    /// Map key value assignment as defined in [KEY_VALUE_ASSIGN](map::KEY_VALUE_ASSIGN)
    #[assoc(name = map::KEY_VALUE_ASSIGN)]
    KeyValueAssignment,
    /// Arrow, used to separate rules as defined in [ARROW](rule::ARROW)
    #[assoc(name = rule::ARROW)]
    RuleArrow,
    /// Greater than as defined in [GREATER](operator::GREATER)
    #[assoc(name = operator::GREATER)]
    Greater,
    /// Greater than or equal as defined in [GREATER_EQUAL](operator::GREATER_EQUAL)
    #[assoc(name = operator::GREATER_EQUAL)]
    GreaterEqual,
    /// Less than as defined in [LESS](operator::LESS)
    #[assoc(name = operator::LESS)]
    Less,
    /// Less than or equal as defined in [LESS_EQUAL](operator::LESS_EQUAL)
    #[assoc(name = operator::LESS_EQUAL)]
    LessEqual,
    /// Equal as defined in [EQUAL](operator::EQUAL)
    #[assoc(name = operator::EQUAL)]
    Equal,
    /// Unequal as defined in [UNEQUAL](operator::UNEQUAL)
    #[assoc(name = operator::UNEQUAL)]
    Unequal,
    /// Tilde, used for negation
    #[assoc(name = atom::NEG)]
    Neg,
    /// Double caret
    #[assoc(name = RDF_DATATYPE_INDICATOR)]
    RdfDatatypeIndicator,
    /// Hash, used in front of aggregates as defined in [INDICATOR](aggregate::INDICATOR)
    #[assoc(name = aggregate::INDICATOR)]
    AggregateIndicator,
    /// Aggregate open
    #[assoc(name = aggregate::OPEN)]
    AggregateOpen,
    /// Aggregate close
    #[assoc(name = aggregate::CLOSE)]
    AggregateClose,
    /// Distinct Variable Separator
    #[assoc(name = aggregate::SEPARATOR_DISTINCT)]
    AggregateDistinctSeparator,
    /// Underscore, used for anonymous values as defined in [ANONYMOUS](datavalues::ANONYMOUS)
    #[assoc(name = datavalues::ANONYMOUS)]
    AnonVal,
    /// Plus as defined in [PLUS](operator::PLUS)
    #[assoc(name = operator::PLUS)]
    Plus,
    /// Minus as defined in [MINUS](operator::MINUS)
    #[assoc(name = operator::MINUS)]
    Minus,
    /// Star as defined in [MUL](operator::MUL)
    #[assoc(name = operator::MUL)]
    Multiplication,
    /// Division as defined in [DIV](operator::DIV)
    #[assoc(name = operator::DIV)]
    Division,
    /// True
    #[assoc(name = boolean::TRUE)]
    True,
    /// False
    #[assoc(name = boolean::FALSE)]
    False,
    /// Dot for numbers
    #[assoc(name = datavalues::DOT)]
    Dot,
    /// Quote
    #[assoc(name = "\"")]
    Quote,
    /// Format string open
    #[assoc(name = format_string::OPEN)]
    FormatStringOpen,
    /// Format string close
    #[assoc(name = format_string::CLOSE)]
    FormatStringClose,
    /// Format string open
    #[assoc(name = format_string::EXPRESSION_START)]
    FormatStringExpressionStart,
    /// Format string close
    #[assoc(name = format_string::EXPRESSION_END)]
    FormatStringExpressionEnd,
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
    /// String
    #[assoc(name = "format-string")]
    FormatString,
    /// Token marking language tag
    #[assoc(name = string::LANG_TAG)]
    LangTagIndicator,
    /// Token marking a normal comment as defined in [COMMENT](comment::COMMENT)
    #[assoc(name = comment::COMMENT)]
    Comment,
    /// Token marking the beginning of a closed comment (can be multiple lines) as defined
    /// in [CLOSED_OPEN](comment::CLOSED_OPEN)
    #[assoc(name = comment::CLOSED_OPEN)]
    OpenComment,
    /// Token marking the beginning of a closed comment (can be multiple lines) as defined
    /// in [CLOSED_CLOSE](comment::CLOSED_CLOSE)
    #[assoc(name = comment::CLOSED_CLOSE)]
    CloseComment,
    /// Token marking a doc comment attached to e.g. a rule as defined in [DOC_COMMENT](comment::DOC_COMMENT)
    #[assoc(name = comment::DOC_COMMENT)]
    DocComment,
    /// Token marking the top level comment as defined in [TOP_LEVEL](comment::TOP_LEVEL)
    #[assoc(name = comment::TOP_LEVEL)]
    TopLevelComment,
    /// Directive keyword indicator as defined in [INDICATOR_TOKEN](directive::INDICATOR_TOKEN)
    #[assoc(name = directive::INDICATOR_TOKEN)]
    DirectiveIndicator,
    /// Token for the base directive
    #[assoc(name = directive::BASE)]
    BaseDirective,
    /// Token for the declare directive
    #[assoc(name = directive::DECLARE)]
    DeclareDirective,
    /// Token for the export directive
    #[assoc(name = directive::EXPORT)]
    ExportDirective,
    /// Token for the import directive
    #[assoc(name = directive::IMPORT)]
    ImportDirective,
    /// Token for the output directive
    #[assoc(name = directive::OUTPUT)]
    OutputDirective,
    /// Token for the prefix directive
    #[assoc(name = directive::PREFIX)]
    PrefixDirective,
    /// Token for the import assignment
    #[assoc(name = directive::IMPORT_ASSIGNMENT)]
    ImportAssignment,
    /// Token for the export assignment
    #[assoc(name = directive::EXPORT_ASSIGNMENT)]
    ExportAssignment,
    /// Token for the prefix assignment
    #[assoc(name = directive::PREFIX_ASSIGNMENT)]
    PrefixAssignment,
    /// Token for separating names from data types
    #[assoc(name = directive::NAME_DATATYPE_SEPARATOR)]
    NameDatatypeSeparator,
    /// Opening token for attributes
    #[assoc(name = rule::OPEN_ATTRIBUTE)]
    OpenAttribute,
    /// Closing token for attributes
    #[assoc(name = rule::CLOSE_ATTRIBUTE)]
    CloseAttribute,
    /// Space (space, tab)
    #[assoc(name = "space")]
    Space,
    /// White spaces (space, tab, newlines)
    #[assoc(name = "whitespace")]
    Whitespace,
    /// Double new line
    #[assoc(name = "double newline")]
    DoubleNewline,
    /// Token that captures errors
    #[assoc(name = "error")]
    Error,
    /// Empty token
    #[assoc(name = "empty")]
    Empty,
}

/// A token is the smallest unit recognized by the parser
/// that is used to built up more complex expressions
#[derive(Debug)]
pub struct Token<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// The kind of token
    kind: TokenKind,
}

impl Display for Token<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.span.fmt(f)
    }
}

/// Macro for generating token parser functions
macro_rules! string_token {
    ($func_name: ident, $token: expr) => {
        /// Parse this token.
        pub fn $func_name(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
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
    /// Return the [Span] of this token.
    pub fn span(&self) -> Span<'a> {
        self.span
    }

    /// Return the [TokenKind] of this token.
    pub fn kind(&self) -> TokenKind {
        self.kind
    }

    /// Parse [TokenKind::Empty].
    pub fn empty(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        let beginning = input.span.empty();

        ParserResult::Ok((
            input,
            Token {
                span: beginning,
                kind: TokenKind::Empty,
            },
        ))
    }

    /// Parse [TokenKind::Name].
    pub fn name(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(
            ParserContext::token(TokenKind::Name),
            recognize(pair(
                alpha1,
                many0(alt((alphanumeric1, tag("_"), tag("%")))),
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

    /// Parse arbitrary characters excluding the ones given as a paramater.
    fn parse_character_sequence(
        input: ParserInput<'a>,
        exclude: &str,
    ) -> ParserResult<'a, Token<'a>> {
        is_not(exclude)(input).map(|(rest, result)| {
            (
                rest.clone(),
                Token {
                    span: result.span,
                    kind: TokenKind::String,
                },
            )
        })
    }

    /// Parse [TokenKind::String].
    pub fn string(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        Self::parse_character_sequence(input, "\"")
    }

    /// Parse [TokenKind::FormatString].
    pub fn fstring(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        let excluded = format!(
            "\"{}{}",
            format_string::EXPRESSION_START,
            format_string::EXPRESSION_END
        );

        Self::parse_character_sequence(input, &excluded)
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

    /// Parse [TokenKind::Space], zero or more
    pub fn space0(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(ParserContext::token(TokenKind::Space), space0)(input).map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::Space,
                },
            )
        })
    }

    /// Parse [TokenKind::Space], one or more
    pub fn space1(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(ParserContext::token(TokenKind::Space), space1)(input).map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::Space,
                },
            )
        })
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

    pub fn directive_base(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(
            ParserContext::token(TokenKind::BaseDirective),
            // This should get parsed like this and not with `tag("base")`, because
            // @baseerror would get matched and rest would be "error" and that will cause an
            // error. The desired behaviour is, that "baseerror" gets matched as a whole and
            // produces an [UnknownDirective].
            verify(Self::name, |tag| tag.span.fragment() == directive::BASE),
        )(input)
        .map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::BaseDirective,
                },
            )
        })
    }

    pub fn directive_declare(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(
            ParserContext::token(TokenKind::DeclareDirective),
            // The reasoning behind using `verify` is the same as in the `directive_base` function.
            verify(Self::name, |tag| tag.span.fragment() == directive::DECLARE),
        )(input)
        .map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::DeclareDirective,
                },
            )
        })
    }
    pub fn directive_export(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(
            ParserContext::token(TokenKind::ExportDirective),
            // The reasoning behind using `verify` is the same as in the `directive_base` function.
            verify(Self::name, |tag| tag.span.fragment() == directive::EXPORT),
        )(input)
        .map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::ExportDirective,
                },
            )
        })
    }
    pub fn directive_import(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(
            ParserContext::token(TokenKind::ImportDirective),
            // The reasoning behind using `verify` is the same as in the `directive_base` function.
            verify(Self::name, |tag| tag.span.fragment() == directive::IMPORT),
        )(input)
        .map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::ImportDirective,
                },
            )
        })
    }
    pub fn directive_output(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(
            ParserContext::token(TokenKind::OutputDirective),
            // The reasoning behind using `verify` is the same as in the `directive_base` function.
            verify(Self::name, |tag| tag.span.fragment() == directive::OUTPUT),
        )(input)
        .map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::OutputDirective,
                },
            )
        })
    }
    pub fn directive_prefix(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(
            ParserContext::token(TokenKind::PrefixDirective),
            // The reasoning behind using `verify` is the same as in the `directive_base` function.
            verify(Self::name, |tag| tag.span.fragment() == directive::PREFIX),
        )(input)
        .map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::PrefixDirective,
                },
            )
        })
    }

    pub fn comment(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        context(
            ParserContext::token(TokenKind::Comment),
            verify(
                alt((
                    recognize(pair(
                        tag(comment::COMMENT_LONG),
                        opt(is_a(comment::COMMENT_EXT)),
                    )),
                    tag(comment::DOC_COMMENT),
                    tag(comment::COMMENT),
                )),
                |result: &ParserInput| result.span.fragment() != comment::DOC_COMMENT,
            ),
        )(input)
        .map(|(rest, result)| {
            (
                rest,
                Token {
                    span: result.span,
                    kind: TokenKind::Comment,
                },
            )
        })
    }

    /// Create [TokenKind::Error].
    pub fn error(span: Span<'a>) -> Token<'a> {
        Token {
            span,
            kind: TokenKind::Error,
        }
    }

    string_token!(directive_indicator, TokenKind::DirectiveIndicator);
    string_token!(open_parenthesis, TokenKind::OpenParenthesis);
    string_token!(closed_parenthesis, TokenKind::ClosedParenthesis);
    string_token!(open_iri, TokenKind::IriOpen);
    string_token!(close_iri, TokenKind::IriClose);
    string_token!(dot, TokenKind::Dot);
    string_token!(seq_sep, TokenKind::SequenceSeparator);
    string_token!(arrow, TokenKind::RuleArrow);
    string_token!(greater, TokenKind::Greater);
    string_token!(greater_equal, TokenKind::GreaterEqual);
    string_token!(less, TokenKind::Less);
    string_token!(less_equal, TokenKind::LessEqual);
    string_token!(equal, TokenKind::Equal);
    string_token!(unequal, TokenKind::Unequal);
    string_token!(tilde, TokenKind::Neg);
    string_token!(double_caret, TokenKind::RdfDatatypeIndicator);
    string_token!(aggregate_indicator, TokenKind::AggregateIndicator);
    string_token!(aggregate_open, TokenKind::AggregateOpen);
    string_token!(aggregate_close, TokenKind::AggregateClose);
    string_token!(
        aggregate_distinct_separator,
        TokenKind::AggregateDistinctSeparator
    );
    string_token!(underscore, TokenKind::AnonVal);
    string_token!(plus, TokenKind::Plus);
    string_token!(minus, TokenKind::Minus);
    string_token!(star, TokenKind::Multiplication);
    string_token!(division, TokenKind::Division);
    string_token!(boolean_true, TokenKind::True);
    string_token!(boolean_false, TokenKind::False);
    string_token!(open_comment, TokenKind::OpenComment);
    string_token!(close_comment, TokenKind::CloseComment);
    string_token!(doc_comment, TokenKind::DocComment);
    string_token!(toplevel_comment, TokenKind::TopLevelComment);
    string_token!(quote, TokenKind::Quote);
    string_token!(fstring_open, TokenKind::FormatStringOpen);
    string_token!(fstring_close, TokenKind::FormatStringClose);
    string_token!(
        fstring_expression_start,
        TokenKind::FormatStringExpressionStart
    );
    string_token!(fstring_expression_end, TokenKind::FormatStringExpressionEnd);
    string_token!(blank_node_prefix, TokenKind::BlankNodePrefix);
    string_token!(exponent_lower, TokenKind::ExponentLower);
    string_token!(exponent_upper, TokenKind::ExponentUpper);
    string_token!(type_marker_double, TokenKind::TypeMarkerDouble);
    string_token!(type_marker_float, TokenKind::TypeMarkerFloat);
    string_token!(import_assignment, TokenKind::ImportAssignment);
    string_token!(export_assignment, TokenKind::ExportAssignment);
    string_token!(prefix_assignment, TokenKind::PrefixAssignment);
    string_token!(key_value_assignment, TokenKind::KeyValueAssignment);
    string_token!(atom_open, TokenKind::AtomOpen);
    string_token!(atom_close, TokenKind::AtomClose);
    string_token!(map_open, TokenKind::MapOpen);
    string_token!(map_close, TokenKind::MapClose);
    string_token!(operation_open, TokenKind::OperationOpen);
    string_token!(operation_close, TokenKind::OperationClose);
    string_token!(tuple_open, TokenKind::TupleOpen);
    string_token!(tuple_close, TokenKind::TupleClose);
    string_token!(namespace_separator, TokenKind::NamespaceSeparator);
    string_token!(open_attribute, TokenKind::OpenAttribute);
    string_token!(close_attribute, TokenKind::CloseAttribute);
    string_token!(rule_arrow, TokenKind::RuleArrow);
    string_token!(universal_indicator, TokenKind::UniversalIndicator);
    string_token!(existential_indicator, TokenKind::ExistentialIndicator);
    string_token!(lang_tag_indicator, TokenKind::LangTagIndicator);
    string_token!(name_datatype_separator, TokenKind::NameDatatypeSeparator);
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::ParserState;

    use super::*;

    #[test]
    fn comment() {
        let test = [
            ("%", Ok("%")),
            ("%%%", Err("%%%")),
            ("%%%%", Ok("%%%%")),
            ("%%%%%", Ok("%%%%%")),
            ("%%%%%%%%%%%%%", Ok("%%%%%%%%%%%%%")),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Token::comment)(parser_input);

            match result {
                Ok(_) => assert_eq!(result.is_ok(), expected.is_ok()),
                Err(_) => assert_eq!(result.is_err(), expected.is_err()),
            }
        }
    }
}
