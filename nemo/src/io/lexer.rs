//! Lexical tokenization of rulewerk-style rules.

use std::{cell::RefCell, ops::Range};

use super::parser::context;
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
use nom_locate::LocatedSpan;
use nom_supreme::{context::ContextError, error::GenericErrorTree};
use tower_lsp::lsp_types::SymbolKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Context {
    Tag(&'static str),
    Exponent,
    Punctuations,
    Operators,
    Identifier,
    Iri,
    Number,
    String,
    Comment,
    DocComment,
    TlDocComment,
    Comments,
    Whitespace,
    Illegal,
    Program,
    Fact,
    Rule,
    RuleHead,
    RuleBody,
    Directive,
    DirectiveBase,
    DirectivePrefix,
    DirectiveImport,
    DirectiveExport,
    DirectiveOutput,
    List,
    HeadAtoms,
    BodyAtoms,
    PositiveAtom,
    NegativeAtom,
    InfixAtom,
    Tuple,
    NamedTuple,
    Map,
    Pair,
    Term,
    TermPrivimitive,
    TermBinary,
    TermAggregation,
    TermTuple,
    TermMap,
    RdfLiteral,
    PrefixedConstant,
    Decimal,
    Integer,
    ArithmeticProduct,
    ArithmeticFactor,
    Blank,
    UniversalVariable,
    ExistentialVariable,
}
impl std::fmt::Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Context::Tag(c) => write!(f, "{}", c),
            Context::Exponent => write!(f, "exponent"),
            Context::Punctuations => write!(f, "punctuations"),
            Context::Operators => write!(f, "operators"),
            Context::Identifier => write!(f, "identifier"),
            Context::Iri => write!(f, "lex iri"),
            Context::Number => write!(f, "lex number"),
            Context::String => write!(f, "lex string"),
            Context::Comment => write!(f, "lex comment"),
            Context::DocComment => write!(f, "lex documentation comment"),
            Context::TlDocComment => write!(f, "lex top level documentation comment"),
            Context::Comments => write!(f, "comments"),
            Context::Whitespace => write!(f, "lex whitespace"),
            Context::Illegal => write!(f, "lex illegal character"),
            Context::Program => write!(f, "program"),
            Context::Fact => write!(f, "fact"),
            Context::Rule => write!(f, "rule"),
            Context::RuleHead => write!(f, "rule head"),
            Context::RuleBody => write!(f, "rule body"),
            Context::Directive => write!(f, "directive"),
            Context::DirectiveBase => write!(f, "base directive"),
            Context::DirectivePrefix => write!(f, "prefix directive"),
            Context::DirectiveImport => write!(f, "import directive"),
            Context::DirectiveExport => write!(f, "export directive"),
            Context::DirectiveOutput => write!(f, "output directive"),
            Context::List => write!(f, "list"),
            Context::HeadAtoms => write!(f, "head atoms"),
            Context::BodyAtoms => write!(f, "body atoms"),
            Context::PositiveAtom => write!(f, "positive atom"),
            Context::NegativeAtom => write!(f, "negative atom"),
            Context::InfixAtom => write!(f, "infix atom"),
            Context::Tuple => write!(f, "tuple"),
            Context::NamedTuple => write!(f, "named tuple"),
            Context::Map => write!(f, "map"),
            Context::Pair => write!(f, "pair"),
            Context::Term => write!(f, "term"),
            Context::TermPrivimitive => write!(f, "primitive term"),
            Context::TermBinary => write!(f, "binary term"),
            Context::TermAggregation => write!(f, "aggreation term"),
            Context::TermTuple => write!(f, "tuple term"),
            Context::TermMap => write!(f, "map term"),
            Context::RdfLiteral => write!(f, "rdf literal"),
            Context::PrefixedConstant => write!(f, "prefixed constant"),
            Context::Decimal => write!(f, "decimal"),
            Context::Integer => write!(f, "integer"),
            Context::ArithmeticProduct => write!(f, "arithmetic product"),
            Context::ArithmeticFactor => write!(f, "arithmetic factor"),
            Context::Blank => write!(f, "blank"),
            Context::UniversalVariable => write!(f, "universal variable"),
            Context::ExistentialVariable => write!(f, "existential variable"),
        }
    }
}

pub(crate) type ErrorTree<I> =
    GenericErrorTree<I, &'static str, Context, Box<dyn std::error::Error + Send + Sync + 'static>>;

use super::parser::{
    ast::{AstNode, Position},
    types::{Input, ToRange},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Error {
    pub pos: Position,
    pub msg: String,
    pub context: Vec<Context>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ParserState<'a> {
    pub(crate) errors: &'a RefCell<Vec<Error>>,
}
impl ParserState<'_> {
    pub fn report_error(&self, error: Error) {
        self.errors.borrow_mut().push(error);
    }
}

pub(crate) type Span<'a> = LocatedSpan<&'a str>;

impl ToRange for Span<'_> {
    fn to_range(&self) -> Range<usize> {
        let start = self.location_offset();
        let end = start + self.fragment().len();
        start..end
    }
}
impl AstNode for Span<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        None
    }

    fn span(&self) -> Span {
        *self
    }

    fn is_leaf(&self) -> bool {
        true
    }

    fn name(&self) -> String {
        self.fragment().to_string()
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        todo!()
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        todo!()
    }

    fn lsp_range_to_rename(&self) -> Option<super::parser::ast::Range> {
        todo!()
    }
}

pub(crate) fn to_range(span: Span<'_>) -> Range<usize> {
    let start = span.location_offset();
    let end = start + span.fragment().len();
    start..end
}

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
    /// 'e' or 'E'
    Exponent,
    // Multi-char tokens:
    /// Identifier for keywords and names
    Ident,
    /// Identifier with a prefix, like `xsd:decimal`
    PrefixedIdent,
    /// Variable like `?var`
    Variable,
    /// Existential Variable like `!var`
    Existential,
    /// Aggregate identifier like `#sum`
    Aggregate,
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
    /// A comment, starting with `%!`
    TlDocComment,
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
    /// Ident for prefixes
    PrefixIdent,
    /// catch all token
    Illegal,
    /// signals end of file
    Eof,
    /// signals an error
    Error,
}
impl std::fmt::Display for TokenKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenKind::QuestionMark => write!(f, "QuestionMark"),
            TokenKind::ExclamationMark => write!(f, "ExclamationMark"),
            TokenKind::OpenParen => write!(f, "OpenParen"),
            TokenKind::CloseParen => write!(f, "CloseParen"),
            TokenKind::OpenBracket => write!(f, "OpenBracket"),
            TokenKind::CloseBracket => write!(f, "CloseBracket"),
            TokenKind::OpenBrace => write!(f, "OpenBrace"),
            TokenKind::CloseBrace => write!(f, "CloseBrace"),
            TokenKind::Dot => write!(f, "Dot"),
            TokenKind::Comma => write!(f, "Comma"),
            TokenKind::Colon => write!(f, "Colon"),
            TokenKind::Arrow => write!(f, "Arrow"),
            TokenKind::Greater => write!(f, "Greater"),
            TokenKind::GreaterEqual => write!(f, "GreaterEqual"),
            TokenKind::Equal => write!(f, "Equal"),
            TokenKind::LessEqual => write!(f, "LessEqual"),
            TokenKind::Less => write!(f, "Less"),
            TokenKind::Unequal => write!(f, "Unequal"),
            TokenKind::Tilde => write!(f, "Tilde"),
            TokenKind::Caret => write!(f, "Caret"),
            TokenKind::Hash => write!(f, "Hash"),
            TokenKind::Underscore => write!(f, "Underscore"),
            TokenKind::At => write!(f, "At"),
            TokenKind::Plus => write!(f, "Plus"),
            TokenKind::Minus => write!(f, "Minus"),
            TokenKind::Star => write!(f, "Star"),
            TokenKind::Slash => write!(f, "Slash"),
            TokenKind::Exponent => write!(f, "Exponent"),
            TokenKind::Ident => write!(f, "Ident"),
            TokenKind::PrefixedIdent => write!(f, "Prefixed Ident"),
            TokenKind::Variable => write!(f, "Variable"),
            TokenKind::Existential => write!(f, "Existential"),
            TokenKind::Aggregate => write!(f, "Aggregate"),
            TokenKind::Iri => write!(f, "Iri"),
            TokenKind::Number => write!(f, "Number"),
            TokenKind::String => write!(f, "String"),
            TokenKind::Comment => write!(f, "Comment"),
            TokenKind::DocComment => write!(f, "DocComment"),
            TokenKind::TlDocComment => write!(f, "TlDocComment"),
            TokenKind::Whitespace => write!(f, "Whitespace"),
            TokenKind::Base => write!(f, "Base"),
            TokenKind::Prefix => write!(f, "Prefix"),
            TokenKind::Import => write!(f, "Import"),
            TokenKind::Export => write!(f, "Export"),
            TokenKind::Output => write!(f, "Output"),
            TokenKind::PrefixIdent => write!(f, "PrefixIdent"),
            TokenKind::Illegal => write!(f, "Illegal"),
            TokenKind::Eof => write!(f, "Eof"),
            TokenKind::Error => write!(f, "\x1b[1;31mError\x1b[0m"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Token<'a> {
    pub(crate) kind: TokenKind,
    pub(crate) span: Span<'a>,
}
impl<'a> Token<'a> {
    pub(crate) fn new(kind: TokenKind, span: Span<'a>) -> Token<'a> {
        Token { kind, span }
    }
}
impl std::fmt::Display for Token<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let offset = self.span.location_offset();
        let line = self.span.location_line();
        let column = self.span.get_utf8_column();
        let fragment = self.span.fragment();
        if self.span.extra == () {
            write!(
                f,
                // "T!{{{0}, S!({offset}, {line}, {fragment:?})}}",
                "\x1b[93mTOKEN {0} \x1b[34m@{line}:{column} ({offset}) \x1b[93m{fragment:?}\x1b[0m",
                self.kind
            )
        } else {
            write!(
                f,
                // "T!{{{0}, S!({offset}, {line}, {fragment:?}, {1:?})}}",
                "\x1b[93mTOKEN {0} \x1b[34m@{line}:{column} ({offset}) \x1b[93m{fragment:?}\x1b[0m, {1:?}\x1b[0m",
                self.kind, self.span.extra
            )
        }
    }
}
impl<'a> AstNode for Token<'a> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        None::<Vec<_>>
    }

    fn span(&self) -> Span {
        self.span
    }

    fn is_leaf(&self) -> bool {
        true
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        None
    }

    fn lsp_range_to_rename(&self) -> Option<super::parser::ast::Range> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        None
    }

    fn name(&self) -> String {
        String::from("Token")
    }
}

// pub(crate) fn map_err<'a, 's, O, E: ParseError<Input<'a, 's>>>(
//     mut f: impl nom::Parser<Input<'a, 's>, O, E>,
//     mut op: impl FnMut(E) -> NewParseError,
// ) -> impl FnMut(Input<'a, 's>) -> IResult<Input<'a, 's>, O> {
//     move |input| {
//         f.parse(input).map_err(|e| match e {
//             nom::Err::Incomplete(err) => nom::Err::Incomplete(err),
//             nom::Err::Error(err) => nom::Err::Error(op(err)),
//             nom::Err::Failure(err) => nom::Err::Error(op(err)),
//         })
//     }
// }

macro_rules! syntax {
    ($func_name: ident, $tag_str: literal, $token: expr) => {
        pub(crate) fn $func_name<'a, 's, E>(
            input: Input<'a, 's>,
        ) -> IResult<Input<'a, 's>, Span<'a>, E>
        where
            E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
        {
            map(
                context(Context::Tag($tag_str), tag($tag_str)),
                |input: Input| input.input,
            )(input)
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
syntax!(exp_lower, "e", TokenKind::Exponent);
syntax!(exp_upper, "E", TokenKind::Exponent);

pub(crate) fn exp<'a, 's, E>(input: Input<'a, 's>) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(Context::Exponent, alt((exp_lower, exp_upper)))(input)
}

pub(crate) fn lex_punctuations<'a, 's, E>(
    input: Input<'a, 's>,
) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(
        Context::Punctuations,
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
        )),
    )(input)
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

pub(crate) fn lex_operators<'a, 's, E>(input: Input<'a, 's>) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(
        Context::Operators,
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
        )),
    )(input)
}

/// This function lexes the name of a predicate or a map, called the tag of the predicate/map.
pub(crate) fn lex_tag<'a, 's, E>(input: Input<'a, 's>) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(
        Context::Identifier,
        recognize(pair(
            alpha1,
            many0(alt((alphanumeric1, tag("_"), tag("-")))),
        )),
    )(input)
    .map(|(rest_input, ident)| (rest_input, ident.input))
}

pub(crate) fn lex_prefixed_ident<'a, 's, E>(
    input: Input<'a, 's>,
) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    recognize(tuple((opt(lex_tag), colon, lex_tag)))(input)
        .map(|(rest_input, prefixed_ident)| (rest_input, prefixed_ident.input))
}

pub(crate) fn lex_iri<'a, 's, E>(input: Input<'a, 's>) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(
        Context::Iri,
        recognize(delimited(tag("<"), is_not("> \n"), cut(tag(">")))),
    )(input)
    .map(|(rest, result)| (rest, result.input))
}

pub(crate) fn lex_number<'a, 's, E>(input: Input<'a, 's>) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(Context::Number, digit1)(input).map(|(rest_input, result)| (rest_input, result.input))
}

pub(crate) fn lex_string<'a, 's, E>(input: Input<'a, 's>) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(
        Context::String,
        recognize(delimited(tag("\""), is_not("\""), cut(tag("\"")))),
    )(input)
    .map(|(rest, result)| (rest, result.input))
}

pub(crate) fn lex_comment<'a, 's, E>(input: Input<'a, 's>) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(
        Context::Comment,
        recognize(tuple((tag("%"), many0(is_not("\n")), line_ending))),
    )(input)
    .map(|(rest, result)| (rest, result.input))
}

pub(crate) fn lex_doc_comment<'a, 's, E>(
    input: Input<'a, 's>,
) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(
        Context::DocComment,
        recognize(many1(tuple((tag("%%"), many0(is_not("\n")), line_ending)))),
    )(input)
    .map(|(rest, result)| (rest, result.input))
}

pub(crate) fn lex_toplevel_doc_comment<'a, 's, E>(
    input: Input<'a, 's>,
) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(
        Context::TlDocComment,
        recognize(many1(tuple((tag("%%%"), many0(is_not("\n")), line_ending)))),
    )(input)
    .map(|(rest, result)| (rest, result.input))
}

pub(crate) fn lex_comments<'a, 's, E>(input: Input<'a, 's>) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(
        Context::Comments,
        alt((lex_toplevel_doc_comment, lex_doc_comment, lex_comment)),
    )(input)
}

pub(crate) fn lex_whitespace<'a, 's, E>(input: Input<'a, 's>) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(Context::Whitespace, multispace1)(input).map(|(rest, result)| (rest, result.input))
}

pub(crate) fn lex_illegal<'a, 's, E>(input: Input<'a, 's>) -> IResult<Input<'a, 's>, Span<'a>, E>
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    context(Context::Illegal, take(1usize))(input).map(|(rest, result)| (rest, result.input))
}

pub(crate) fn skip_to_statement_end<'a, 's, E>(input: Input<'a, 's>) -> (Input<'a, 's>, Span<'a>)
where
    E: ParseError<Input<'a, 's>> + ContextError<Input<'a, 's>, Context>,
{
    let (rest_input, error_input) = recognize(tuple((
        take_till::<_, Input<'_, '_>, nom::error::Error<_>>(|c| c == '.'),
        opt(tag(".")),
        multispace0,
    )))(input)
    .expect("Skipping to the next dot should not fail!");
    (rest_input, error_input.input)
}

#[cfg(test)]
mod tests {
    use super::ErrorTree;

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
    fn skip_to_statement_end() {
        let input = Span::new("some ?broken :- rule). A(Fact).");
        let refcell = RefCell::new(Vec::new());
        let errors = ParserState { errors: &refcell };
        let input = Input {
            input,
            parser_state: errors,
        };
        dbg!(super::skip_to_statement_end::<ErrorTree<_>>(input));
    }
}
