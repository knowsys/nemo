//! This module defines [KeyValuePair].

use nom::sequence::{separated_pair, tuple};

use crate::parser::{
    ast::{comment::wsoc::WSoC, expression::Expression, token::Token, ProgramAST},
    context::ParserContext,
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Pairs of Expressions, separated by [KEY_VALUE_ASSIGN][nemo_physical::datavalues::syntax::map::KEY_VALUE_ASSIGN]
#[derive(Debug)]
pub struct KeyValuePair<'a> {
    /// [Span] associated with this node
    span: Span<'a>,
    /// Key
    key: Expression<'a>,
    /// Value
    value: Expression<'a>,
}

impl<'a> KeyValuePair<'a> {
    /// Return the key value pair
    pub fn pair(&self) -> (&Expression<'a>, &Expression<'a>) {
        (&self.key, &self.value)
    }
}

impl<'a> ProgramAST<'a> for KeyValuePair<'a> {
    fn children(&'a self) -> Vec<&'a dyn ProgramAST<'a>> {
        vec![&self.key, &self.value]
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;
        separated_pair(
            Expression::parse,
            tuple((WSoC::parse, Token::key_value_assignment, WSoC::parse)),
            Expression::parse,
        )(input)
        .map(|(rest, (key, value))| {
            let rest_span = rest.span;
            (
                rest,
                KeyValuePair {
                    span: input_span.until_rest(&rest_span),
                    key,
                    value,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        ParserContext::KeyValuePair
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::{
            sequence::{key_value::KeyValuePair, Sequence},
            ProgramAST,
        },
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_expression_sequence_simple() {
        let test = vec![
            ("", 0),
            ("?x:3", 1),
            ("?x: 7, ?y: ?z, ?z: 1", 3),
            ("x:3,     ?x:12, ?x :    7", 3),
            ("x:3, ?x : 2, 2 : 5", 3),
            ("x:3  ,   ?x     : 12,   2:  1", 3),
            ("x:POW(1,2)", 1),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Sequence::<KeyValuePair>::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.into_iter().count());
        }
    }
}
