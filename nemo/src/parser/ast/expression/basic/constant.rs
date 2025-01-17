//! This module defines [Constant]

use crate::parser::{
    ast::{tag::structure::StructureTag, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// AST node representing a constant
#[derive(Debug)]
pub struct Constant<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// The constant
    constant: StructureTag<'a>,
}

impl<'a> Constant<'a> {
    /// Return the [StructureTag] representing the constant.
    pub fn tag(&self) -> &StructureTag<'a> {
        &self.constant
    }
}

const CONTEXT: ParserContext = ParserContext::Constant;

impl<'a> ProgramAST<'a> for Constant<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        Vec::default()
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(CONTEXT, StructureTag::parse)(input).map(|(rest, constant)| {
            let rest_span = rest.span;

            (
                rest,
                Constant {
                    span: input_span.until_rest(&rest_span),
                    constant,
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
        ast::{expression::basic::constant::Constant, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_constant() {
        let test = vec![
            ("abc", "abc".to_string()),
            ("abc:def", "abc:def".to_string()),
            ("<http://example.com>", "http://example.com".to_string()),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Constant::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.tag().to_string());
        }
    }
}
