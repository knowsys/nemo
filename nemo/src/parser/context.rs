//! This module defines [ParserContext].
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nom_supreme::context::ContextError;

use super::{ast::token::TokenKind, error::ParserErrorTree, ParserInput, ParserResult};

/// Context, in which a particular parse error occurred
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn name(&self) -> &'static str)]
pub enum ParserContext {
    /// Token
    #[assoc(name = _kind.name())]
    Token { kind: TokenKind },
    /// Data type
    #[assoc(name = "datatype")]
    DataType,
    /// Number
    #[assoc(name = "number")]
    Number,
    /// Encoded number
    #[assoc(name = "number")]
    EncodedNumber,
    /// Variable
    #[assoc(name = "variable")]
    Variable,
    /// String
    #[assoc(name = "string")]
    String,
    /// Format String
    #[assoc(name = "format-string")]
    FormatString,
    /// Iri
    #[assoc(name = "iri")]
    Iri,
    /// Rdf Literal
    #[assoc(name = "rdf-literal")]
    RdfLiteral,
    /// Blank node
    #[assoc(name = "blank")]
    Blank,
    /// Constant
    #[assoc(name = "constant")]
    Constant,
    /// Boolean
    #[assoc(name = "boolean")]
    Boolean,
    /// Attribute
    #[assoc(name = "attribute")]
    Attribute,
    /// Base directive
    #[assoc(name = "base directive")]
    Base,
    /// body of base directive
    #[assoc(name = "base body")]
    BaseBody,
    /// Declare directive
    #[assoc(name = "declare directive")]
    Declare,
    /// body of declare directive
    #[assoc(name = "declare body")]
    DeclareBody,
    /// Export directive
    #[assoc(name = "export directive")]
    Export,
    /// body of export directive
    #[assoc(name = "export body")]
    ExportBody,
    /// Import directive
    #[assoc(name = "import directive")]
    Import,
    /// body of import directive
    #[assoc(name = "import body")]
    ImportBody,
    /// Output directive
    #[assoc(name = "output directive")]
    Output,
    /// Prefix directive
    #[assoc(name = "prefix directive")]
    Prefix,
    /// body of prefix directive
    #[assoc(name = "prefix body")]
    PrefixBody,
    /// Unknown directive
    #[assoc(name = "unknown directive")]
    UnknownDirective,
    /// Name type pairs in declare directive
    #[assoc(name = "name type pair")]
    DeclareNameTypePair,
    /// Expression
    #[assoc(name = "expression")]
    Expression,
    /// Guard
    #[assoc(name = "expression")] // Guard seems like a technical name
    Guard,
    /// Parenthesized expression
    #[assoc(name = "expression")]
    ParenthesizedExpression,
    /// Tuple
    #[assoc(name = "tuple")]
    Tuple,
    /// Map
    #[assoc(name = "map")]
    Map,
    /// Key value pairs in maps
    #[assoc(name = "key value pair")]
    KeyValuePair,
    /// Sequence
    #[assoc(name = "sequence")]
    Sequence,
    /// Arithmetic expression
    #[assoc(name = "arithmetic expression")]
    Arithmetic,
    /// Atom
    #[assoc(name = "atom")]
    Atom,
    /// Tag
    #[assoc(name = "tag")]
    StructureTag,
    /// Aggregate tag
    #[assoc(name = "aggregate name")]
    AggregationTag,
    /// Operation tag
    #[assoc(name = "operation name")]
    OperationTag,
    /// Operation
    #[assoc(name = "operation")]
    Operation,
    /// Aggregation
    #[assoc(name = "aggregation")]
    Aggregation,
    /// Negation
    #[assoc(name = "negation")]
    Negation,
    /// Infix
    #[assoc(name = "expression")] // TODO: Is there a better name? -- "infix expression"?
    Infix,
    /// Comment
    #[assoc(name = "comment")]
    Comment,
    /// Doc-comment
    #[assoc(name = "doc-comment")]
    DocComment,
    /// Top-level comment
    #[assoc(name = "top-level-comment")]
    TopLevelComment,
    /// Rule
    #[assoc(name = "rule")]
    Rule,
    /// Directive
    #[assoc(name = "directive")]
    Directive,
    /// Statement
    #[assoc(name = "statement")]
    Statement,
    /// Statement kind
    #[assoc(name = "statement kind")]
    StatementKind,
    /// Program
    #[assoc(name = "program")]
    Program,
    /// Error
    #[assoc(name = "error")]
    Error,
}

impl ParserContext {
    /// Create a [ParserContext] from a [TokenKind].
    pub fn token(kind: TokenKind) -> Self {
        Self::Token { kind }
    }
}

/// Add context to an input parser.
pub(crate) fn context<'a, Output, NomParser>(
    context: ParserContext,
    mut f: NomParser,
) -> impl FnMut(ParserInput<'a>) -> ParserResult<'a, Output>
where
    NomParser: nom::Parser<ParserInput<'a>, Output, ParserErrorTree<'a>>,
{
    move |i| match f.parse(i.clone()) {
        Ok(o) => Ok(o),
        Err(nom::Err::Incomplete(i)) => Err(nom::Err::Incomplete(i)),
        Err(nom::Err::Error(e)) => {
            Err(nom::Err::Error(ParserErrorTree::add_context(i, context, e)))
        }
        Err(nom::Err::Failure(e)) => Err(nom::Err::Failure(ParserErrorTree::add_context(
            i, context, e,
        ))),
    }
}
