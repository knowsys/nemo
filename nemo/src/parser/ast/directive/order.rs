//! This module defines the [Order] directive.

use nom::sequence::{preceded, separated_pair, tuple};

use crate::parser::{
    ast::{
        comment::wsoc::WSoC, expression::complex::atom::Atom, guard::Guard, sequence::Sequence,
        token::Token, ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Declare directive, associating atom positions with names and data types
#[derive(Debug)]
pub struct Order<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Atom that is preferred
    atom_left: Atom<'a>,
    /// Atom that is preferred
    atom_right: Atom<'a>,

    /// Condition under which `atom_left` dominates `atom_right`
    condition: Sequence<'a, Guard<'a>>,
}

impl<'a> Order<'a> {
    /// Return the left atom.
    pub fn atom_left(&self) -> &Atom<'a> {
        &self.atom_left
    }

    /// Return the right atom.
    pub fn atom_right(&self) -> &Atom<'a> {
        &self.atom_right
    }

    /// Return each an iterator over the conditions.
    pub fn condition(&self) -> impl Iterator<Item = &Guard<'a>> {
        self.condition.iter()
    }

    fn parse_declaration(input: ParserInput<'a>) -> ParserResult<'a, (Atom<'a>, Atom<'a>)> {
        context(
            ParserContext::Order, // TODO: More specific context
            separated_pair(
                Atom::parse,
                tuple((WSoC::parse, Token::order_compare, WSoC::parse)),
                Atom::parse,
            ),
        )(input)
    }
}

const CONTEXT: ParserContext = ParserContext::Declare;

impl<'a> ProgramAST<'a> for Order<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result = Vec::<&dyn ProgramAST>::new();
        result.push(&self.atom_left);
        result.push(&self.atom_right);

        for guard in self.condition.iter() {
            result.push(guard);
        }

        result
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(
            CONTEXT,
            preceded(
                tuple((
                    Token::directive_indicator,
                    Token::directive_order,
                    WSoC::parse,
                )),
                separated_pair(
                    Self::parse_declaration,
                    tuple((WSoC::parse, Token::order_assignment, WSoC::parse)),
                    Sequence::<Guard>::parse,
                ),
            ),
        )(input)
        .map(|(rest, ((atom_left, atom_right), condition))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    atom_left,
                    atom_right,
                    condition,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

// TODO: Tests
