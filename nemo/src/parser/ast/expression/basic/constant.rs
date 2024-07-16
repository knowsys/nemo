//! This module defines [Constant]

use nom::{branch::alt, combinator::map};

use crate::parser::{
    ast::{token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

use super::iri::Iri;

// Type of constants
#[derive(Debug)]
enum ConstantKind<'a> {
    /// Plain constant
    Plain(Token<'a>),
    /// Iri constant
    Iri(Iri<'a>),
}

/// AST node representing a constant
#[derive(Debug)]
pub struct Constant<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// The constant
    constant: ConstantKind<'a>,
}

impl<'a> Constant<'a> {
    /// Return the name of the constant.
    pub fn name(&self) -> String {
        match &self.constant {
            ConstantKind::Plain(token) => token.to_string(),
            ConstantKind::Iri(iri) => iri.content(),
        }
    }
}

impl<'a> ProgramAST<'a> for Constant<'a> {
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

        context(
            ParserContext::Constant,
            alt((
                map(Token::name, ConstantKind::Plain),
                map(Iri::parse, ConstantKind::Iri),
            )),
        )(input)
        .map(|(rest, constant)| {
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
            ("<http://example.com>", "http://example.com".to_string()),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Constant::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.name());
        }
    }
}
