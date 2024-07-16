//! This module defines [Atom].

use nom::{
    combinator::opt,
    sequence::{delimited, pair},
};

use crate::parser::{
    ast::{
        expression::{sequence::simple::ExpressionSequenceSimple, Expression},
        tag::Tag,
        token::Token,
        ProgramAST,
    },
    context::{context, ParserContext},
    span::ProgramSpan,
};

/// A possibly tagged sequence of [Expression]s.
#[derive(Debug)]
pub struct Atom<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Tag of this Atom
    tag: Tag<'a>,
    /// List of underlying expressions
    expressions: ExpressionSequenceSimple<'a>,
}

impl<'a> Atom<'a> {
    /// Return an iterator over the underlying [Expression]s.
    pub fn expressions(&self) -> impl Iterator<Item = &Expression<'a>> {
        self.expressions.iter()
    }

    /// Return the tag of this atom.
    pub fn tag(&self) -> &Tag<'a> {
        &self.tag
    }
}

impl<'a> ProgramAST<'a> for Atom<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        let mut result: Vec<&dyn ProgramAST> = vec![];
        for expression in &self.expressions {
            result.push(expression)
        }

        result
    }

    fn span(&self) -> ProgramSpan {
        self.span
    }

    fn parse(input: crate::parser::input::ParserInput<'a>) -> crate::parser::ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(
            ParserContext::Atom,
            pair(
                Tag::parse,
                delimited(
                    pair(Token::open_parenthesis, opt(Token::whitespace)),
                    ExpressionSequenceSimple::parse,
                    pair(opt(Token::whitespace), Token::closed_parenthesis),
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
