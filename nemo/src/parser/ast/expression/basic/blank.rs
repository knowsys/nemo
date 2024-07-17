//! This module defines [Blank].

use nom::{branch::alt, sequence::pair};

use crate::parser::{
    ast::{token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

/// AST node representing a blank node
#[derive(Debug)]
pub struct Blank<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Name of the blank node
    name: Token<'a>,
}

impl<'a> Blank<'a> {
    /// Return the name of the blank node.
    pub fn name(&self) -> String {
        self.name.to_string()
    }

    /// Parse name of the blank node.
    fn parse_name(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        alt((Token::name, Token::digits))(input)
    }
}

const CONTEXT: ParserContext = ParserContext::Blank;

impl<'a> ProgramAST<'a> for Blank<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        Vec::default()
    }

    fn span(&self) -> ProgramSpan {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(CONTEXT, pair(Token::blank_node_prefix, Self::parse_name))(input).map(
            |(rest, (_, name))| {
                let rest_span = rest.span;

                (
                    rest,
                    Blank {
                        span: input_span.until_rest(&rest_span),
                        name,
                    },
                )
            },
        )
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::{expression::basic::blank::Blank, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_blank() {
        let test = vec![("_:a", "a".to_string()), ("_:123", "123".to_string())];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Blank::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.name());
        }
    }
}
