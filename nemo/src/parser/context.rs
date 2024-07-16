//! This module defines [ParserContext].
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nom_supreme::context::ContextError;

use super::{ast::token::TokenKind, error::ParserErrorTree, ParserInput, ParserResult};

/// Context, in which a particular parse error occurred
#[derive(Assoc, Debug, Clone, Copy)]
#[func(pub fn name(&self) -> &'static str)]
pub enum ParserContext {
    /// Token
    #[assoc(name = _kind.name())]
    Token { kind: TokenKind },
    /// Number
    #[assoc(name = "number")]
    Number,
    /// Variable
    #[assoc(name = "variable")]
    Variable,
    /// String
    #[assoc(name = "string")]
    String,
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
    /// Expression
    #[assoc(name = "expression")]
    Expression,
    /// Tuple
    #[assoc(name = "tuple")]
    Tuple,
    /// Atom
    #[assoc(name = "atom")]
    Atom,
    /// Rule
    #[assoc(name = "rule")]
    Rule,
    /// Program
    #[assoc(name = "program")]
    Program,
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
        Err(nom::Err::Error(e)) => Err(nom::Err::Error(ParserErrorTree::add_context(
            i,
            context.clone(),
            e,
        ))),
        Err(nom::Err::Failure(e)) => Err(nom::Err::Failure(ParserErrorTree::add_context(
            i,
            context.clone(),
            e,
        ))),
    }
}
