//! This module defines [Atom].

use nom::sequence::{delimited, pair};

use crate::parser::{
    ast::{
        comment::wsoc::WSoC, expression::Expression, sequence::simple::ExpressionSequenceSimple,
        tag::structure::StructureTag, token::Token, ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// A possibly tagged sequence of [Expression]s.
#[derive(Debug)]
pub struct Atom<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Tag of this Atom
    tag: StructureTag<'a>,
    /// List of underlying expressions
    expressions: ExpressionSequenceSimple<'a>,
}

impl<'a> Atom<'a> {
    /// Return an iterator over the underlying [Expression]s.
    pub fn expressions(&self) -> impl Iterator<Item = &Expression<'a>> {
        self.expressions.iter()
    }

    /// Return the tag of this atom.
    pub fn tag(&self) -> &StructureTag<'a> {
        &self.tag
    }
}

const CONTEXT: ParserContext = ParserContext::Atom;

impl<'a> ProgramAST<'a> for Atom<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        let mut result = Vec::<&dyn ProgramAST>::new();
        result.push(&self.tag);

        for expression in &self.expressions {
            result.push(expression)
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
            pair(
                StructureTag::parse,
                delimited(
                    pair(Token::atom_open, WSoC::parse),
                    ExpressionSequenceSimple::parse,
                    pair(WSoC::parse, Token::atom_close),
                ),
            ),
        )(input)
        .map(|(rest, (tag, expressions))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    tag,
                    expressions,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::{expression::complex::atom::Atom, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_atom() {
        let test = vec![
            ("abc(1)", ("abc".to_string(), 1)),
            ("abc(1,2)", ("abc".to_string(), 2)),
            ("abc( 1 )", ("abc".to_string(), 1)),
            ("abc( 1 , 2 )", ("abc".to_string(), 2)),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Atom::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(
                expected,
                (result.1.tag().to_string(), result.1.expressions().count())
            );
        }
    }
}
