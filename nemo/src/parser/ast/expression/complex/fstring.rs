//! This module defines [FormatString].

use nom::{branch::alt, combinator::map, multi::many0, sequence::delimited};

use crate::parser::{
    ParserResult,
    ast::{ProgramAST, expression::Expression, token::Token},
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

/// Elements that make up a [FormatString]
#[derive(Debug)]
pub enum FormatStringElement<'a> {
    /// String
    String(Token<'a>),
    /// Expression
    Expression(Expression<'a>),
}

/// A string which may include sub expressions
#[derive(Debug)]
pub struct FormatString<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// List of [FormatStringElement]
    elements: Vec<FormatStringElement<'a>>,
}

impl<'a> FormatString<'a> {
    /// Return an iterator over the underlying [Expression]s.
    pub fn elements(&self) -> impl Iterator<Item = &FormatStringElement<'a>> {
        self.elements.iter()
    }

    /// Parse an [Expression] surrounded by fstring start and end tokens.
    fn parse_expression(input: ParserInput<'a>) -> ParserResult<'a, Expression<'a>> {
        delimited(
            Token::fstring_expression_start,
            Expression::parse,
            Token::fstring_expression_end,
        )(input)
    }

    /// Parse [FormatStringElement] by parsing either a string or an expression element.
    fn parse_element(input: ParserInput<'a>) -> ParserResult<'a, FormatStringElement<'a>> {
        alt((
            map(Token::fstring, FormatStringElement::String),
            map(Self::parse_expression, FormatStringElement::Expression),
        ))(input)
    }

    /// Parse a multi-line [FormatStringElement] by parsing either a string or an expression element.
    fn parse_multiline_element(
        input: ParserInput<'a>,
    ) -> ParserResult<'a, FormatStringElement<'a>> {
        alt((
            map(Token::multiline_fstring, FormatStringElement::String),
            map(Self::parse_expression, FormatStringElement::Expression),
        ))(input)
    }
}

const CONTEXT: ParserContext = ParserContext::FormatString;

impl<'a> ProgramAST<'a> for FormatString<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        for element in &self.elements {
            match element {
                FormatStringElement::String(_token) => {}
                FormatStringElement::Expression(expression) => result.push(expression),
            }
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
            alt((
                delimited(
                    Token::fstring_multiline_open,
                    many0(Self::parse_multiline_element),
                    Token::fstring_multiline_close,
                ),
                delimited(
                    Token::fstring_open,
                    many0(Self::parse_element),
                    Token::fstring_close,
                ),
            )),
        )(input)
        .map(|(rest, elements)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    elements,
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
        ParserState,
        ast::{ProgramAST, expression::complex::fstring::FormatString},
        input::ParserInput,
    };

    #[test]
    fn parse_format_string() {
        let test = vec![
            (r#"f"""#, 0),
            (r#"f"string""#, 1),
            (r#"f"""string""""#, 1),
            (r#"f"{?x + 1}""#, 1),
            (r#"f"result: {?x + 1}""#, 2),
            (r#"f"{?x} + {?y} = {?x + ?y}""#, 5),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(FormatString::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap().1;
            assert_eq!(expected, result.elements().count());
        }
    }
}
