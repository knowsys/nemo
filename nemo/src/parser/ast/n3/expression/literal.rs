//! This module defines [N3Literal].

use nom::{branch::alt, combinator::map};

use crate::parser::{
    ParserResult,
    ast::{
        ProgramAST,
        expression::basic::{
            boolean::Boolean, number::Number, rdf_literal::RdfLiteral, string::StringLiteral,
        },
    },
    context::{Notation3Context, ParserContext, context},
    input::ParserInput,
    span::Span,
};

/// A Notation3 variable
#[derive(Clone, Debug)]
pub enum N3Literal<'a> {
    /// An RDF literal
    Rdf(RdfLiteral<'a>),
    /// A numeric literal
    Numeric(Number<'a>),
    /// A boolean literal
    Boolean(Boolean<'a>),
    /// A bare string literal
    String(StringLiteral<'a>),
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Literal);

impl N3Literal<'_> {
    /// Return the [ParserContext] of the underlying literal type.
    pub fn context_type(&self) -> ParserContext {
        match self {
            Self::Rdf(rdf_literal) => rdf_literal.context(),
            Self::Numeric(number) => number.context(),
            Self::Boolean(boolean) => boolean.context(),
            Self::String(string_literal) => string_literal.context(),
        }
    }
}

impl<'a> ProgramAST<'a> for N3Literal<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        match self {
            Self::Rdf(rdf_literal) => rdf_literal.children(),
            Self::Numeric(number) => number.children(),
            Self::Boolean(boolean) => boolean.children(),
            Self::String(string_literal) => string_literal.children(),
        }
    }

    fn span(&self) -> Span<'a> {
        match self {
            Self::Rdf(rdf_literal) => rdf_literal.span(),
            Self::Numeric(number) => number.span(),
            Self::Boolean(boolean) => boolean.span(),
            Self::String(string_literal) => string_literal.span(),
        }
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        context(
            CONTEXT,
            alt((
                map(Boolean::parse, Self::Boolean),
                map(Number::parse, Self::Numeric),
                map(RdfLiteral::parse, Self::Rdf),
                map(StringLiteral::parse, Self::String),
            )),
        )(input)
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}
