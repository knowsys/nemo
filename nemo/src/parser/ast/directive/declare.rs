//! This module defines the [Declare] directive.

use nom::sequence::{delimited, pair, preceded, separated_pair, tuple};

use crate::parser::{
    ast::{
        comment::wsoc::WSoC,
        sequence::{declare::NameTypePair, Sequence},
        tag::structure::StructureTag,
        token::Token,
        ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Declare directive, associating atom positions with names and data types
#[derive(Debug)]
pub struct Declare<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Predicate this statement applies to
    predicate: StructureTag<'a>,
    /// The declaration
    declaration: Sequence<'a, NameTypePair<'a>>,
}

impl<'a> Declare<'a> {
    /// Return the predicate this statement applies to.
    pub fn predicate(&self) -> &StructureTag<'a> {
        &self.predicate
    }

    /// Return an iterator over the name-type pairs.
    pub fn name_type_pairs(&self) -> impl Iterator<Item = NameTypePair> + '_ {
        self.declaration.clone().into_iter()
    }

    pub fn parse_body(
        input: ParserInput<'a>,
    ) -> ParserResult<'a, (StructureTag<'a>, Sequence<'a, NameTypePair<'a>>)> {
        context(
            ParserContext::DeclareBody,
            separated_pair(
                StructureTag::parse,
                WSoC::parse,
                delimited(
                    pair(Token::atom_open, WSoC::parse),
                    Sequence::<NameTypePair>::parse,
                    pair(WSoC::parse, Token::atom_close),
                ),
            ),
        )(input)
    }
}

const CONTEXT: ParserContext = ParserContext::Declare;

impl<'a> ProgramAST<'a> for Declare<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        let mut result = Vec::<&dyn ProgramAST>::new();
        result.push(&self.predicate);

        for pair in self.declaration.iter() {
            result.push(pair);
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
                    Token::directive_declare,
                    WSoC::parse,
                )),
                Self::parse_body,
            ),
        )(input)
        .map(|(rest, (predicate, declaration))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    predicate,
                    declaration,
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
        ast::{directive::declare::Declare, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_declare() {
        let test = vec![(
            "@declare test(_: any, a: int, _: float, b: string)",
            ("test".to_string(), 4),
        )];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Declare::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(
                expected,
                (
                    result.1.predicate().to_string(),
                    result.1.name_type_pairs().count()
                )
            );
        }
    }
}
