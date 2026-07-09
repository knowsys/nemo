//! This module defines [N3Formula].

use nom::{bytes::complete::tag, multi::many0, sequence::delimited};

use crate::parser::{
    ParserResult,
    ast::{
        ProgramAST,
        expression::complex::atom::Atom,
        guard::Guard,
        n3::{comment::WSoC, statement::N3Statement},
        token::Token,
    },
    context::{Notation3Context, ParserContext, context},
    input::ParserInput,
    span::Span,
};

#[derive(Clone, Debug)]
/// A Notation3 formula.
pub struct N3Formula<'a> {
    span: Span<'a>,

    content: Vec<N3Statement<'a>>,
}

impl<'a> N3Formula<'a> {
    /// An iterator over the statements in this formula.
    pub fn iter(&'a self) -> impl Iterator<Item = &'a N3Statement<'a>> + use<'a> {
        self.content.iter()
    }

    /// The statements in this formula.
    pub fn statements(&'a self) -> Vec<N3Statement<'a>> {
        self.content.clone()
    }

    /// Convert this formula into a list of [Guard]s.
    pub fn try_into_guards(self) -> Vec<Guard<'a>> {
        self.content
            .into_iter()
            .flat_map(|statement| {
                statement
                    .try_into_triples()
                    .expect("formula contains only triples")
                    .triples()
                    .into_iter()
                    .map(|triple| Guard::from_atom(Atom::from(triple)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Formula);

impl<'a> ProgramAST<'a> for N3Formula<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        for child in &self.content {
            result.push(child);
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
            delimited(
                delimited(WSoC::parse, tag("{"), WSoC::parse),
                many0(delimited(
                    WSoC::parse,
                    N3Statement::parse,
                    delimited(WSoC::parse, Token::dot, WSoC::parse),
                )),
                delimited(WSoC::parse, tag("}"), WSoC::parse),
            ),
        )(input)
        .map(|(rest, content)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    content,
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
    use std::assert_matches;

    use nom::combinator::all_consuming;

    use crate::parser::{
        ParserState,
        ast::{ProgramAST, n3::expression::formula::N3Formula},
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_paths() {
        let formula = r#"{ ?x a :Person }"#;

        let parser_input = ParserInput::new(formula, ParserState::default());
        let result = all_consuming(N3Formula::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }
}
