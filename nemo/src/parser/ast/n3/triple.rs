//! This module defines [N3Triple].

use nom::sequence::{delimited, tuple};

use crate::parser::{
    ParserResult,
    ast::{ProgramAST, expression::Expression, n3::comment::WSoC},
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

/// A Triple of Notation3.
#[derive(Debug)]
pub struct N3Triple<'a> {
    subject: Expression<'a>,
    predicate: Expression<'a>,
    object: Expression<'a>,

    span: Span<'a>,
}

const CONTEXT: ParserContext = ParserContext::Atom; // TODO(mx): add custom context

impl<'a> ProgramAST<'a> for N3Triple<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        vec![&self.subject, &self.predicate, &self.object]
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
            tuple((
                delimited(WSoC::parse, Expression::parse_basic, WSoC::parse),
                delimited(WSoC::parse, Expression::parse_basic, WSoC::parse),
                delimited(WSoC::parse, Expression::parse_basic, WSoC::parse),
            )),
        )(input)
        .map(|(rest, (subject, predicate, object))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    subject,
                    predicate,
                    object,
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
    use std::assert_matches;
    use test_log::test;

    use crate::parser::{
        ParserState,
        ast::{ProgramAST, n3::triple::N3Triple},
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_triple() {
        // from the N3 Test suite
        let triple = r#":doerthe a :Person"#;

        let parser_input = ParserInput::new(triple, ParserState::default());
        let result = all_consuming(N3Triple::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }
}
