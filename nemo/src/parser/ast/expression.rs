//! This module defines [Expression].

pub mod basic;
pub mod complex;
pub mod sequence;

use basic::{
    blank::Blank, boolean::Boolean, iri::Iri, number::Number, rdf_literal::RdfLiteral,
    string::StringLiteral, variable::Variable,
};
use complex::{atom::Atom, tuple::Tuple};
use nom::{branch::alt, combinator::map};

use crate::parser::{
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

use super::ProgramAST;

/// An expression that is the building block of rules.
#[derive(Debug)]
pub enum Expression<'a> {
    /// Atom
    Atom(Atom<'a>),
    /// Blank
    Blank(Blank<'a>),
    /// Boolean
    Boolean(Boolean<'a>),
    /// Iri
    Iri(Iri<'a>),
    /// Number
    Number(Number<'a>),
    /// Rdf literal
    RdfLiteral(RdfLiteral<'a>),
    /// String
    String(StringLiteral<'a>),
    /// Tuple
    Tuple(Tuple<'a>),
    /// Variable
    Variable(Variable<'a>),
}

impl<'a> ProgramAST<'a> for Expression<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        match self {
            Expression::Atom(expression) => expression.children(),
            Expression::Blank(expression) => expression.children(),
            Expression::Boolean(expression) => expression.children(),
            Expression::Iri(expression) => expression.children(),
            Expression::Number(expression) => expression.children(),
            Expression::RdfLiteral(expression) => expression.children(),
            Expression::String(expression) => expression.children(),
            Expression::Tuple(expression) => expression.children(),
            Expression::Variable(expression) => expression.children(),
        }
    }

    fn span(&self) -> ProgramSpan {
        match self {
            Expression::Atom(expression) => expression.span(),
            Expression::Blank(expression) => expression.span(),
            Expression::Boolean(expression) => expression.span(),
            Expression::Iri(expression) => expression.span(),
            Expression::Number(expression) => expression.span(),
            Expression::RdfLiteral(expression) => expression.span(),
            Expression::String(expression) => expression.span(),
            Expression::Tuple(expression) => expression.span(),
            Expression::Variable(expression) => expression.span(),
        }
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        context(
            ParserContext::Expression,
            alt((
                map(Atom::parse, Self::Atom),
                map(Tuple::parse, Self::Tuple),
                map(Blank::parse, Self::Blank),
                map(Boolean::parse, Self::Boolean),
                map(Iri::parse, Self::Iri),
                map(Number::parse, Self::Number),
                map(RdfLiteral::parse, Self::RdfLiteral),
                map(StringLiteral::parse, Self::String),
                map(Variable::parse, Self::Variable),
            )),
        )(input)
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::{expression::Expression, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_expression_tuple() {
        let test = vec!["(1,2)"];

        for input in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Expression::parse)(parser_input);

            println!("{:?}", result);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert!(matches!(result.1, Expression::Tuple(_)));
        }
    }

    #[test]
    fn parse_expression_atom() {
        let test = vec!["abc(1,2)"];

        for input in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Expression::parse)(parser_input);

            println!("{:?}", result);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert!(matches!(result.1, Expression::Atom(_)));
        }
    }
}
