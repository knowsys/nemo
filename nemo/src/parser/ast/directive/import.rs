//! This module defines the [Import] directive.

use nom::{
    combinator::opt,
    sequence::{delimited, preceded, tuple},
};

use crate::parser::{
    ast::{
        comment::wsoc::WSoC, expression::complex::map::Map, guard::Guard, sequence::Sequence,
        tag::structure::StructureTag, token::Token, ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Import directive
#[derive(Debug)]
pub struct Import<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Predicate that is being imported
    predicate: StructureTag<'a>,
    /// Map of instructions
    instructions: Map<'a>,
    /// Additional variable bindings
    guards: Option<Sequence<'a, Guard<'a>>>,
}

impl<'a> Import<'a> {
    /// Return the predicate.
    pub fn predicate(&self) -> &StructureTag<'a> {
        &self.predicate
    }

    /// Return the instructions.
    pub fn instructions(&self) -> &Map<'a> {
        &self.instructions
    }

    /// Return the variable bindings.
    pub fn guards(&self) -> &Option<Sequence<Guard>> {
        &self.guards
    }

    pub fn parse_body(
        input: ParserInput<'a>,
    ) -> ParserResult<'a, (StructureTag<'a>, Map<'a>, Option<Sequence<'a, Guard<'a>>>)> {
        context(
            ParserContext::ImportBody,
            tuple((
                StructureTag::parse,
                WSoC::parse,
                Token::import_assignment,
                WSoC::parse,
                Map::parse,
                opt(preceded(
                    delimited(WSoC::parse, Token::seq_sep, WSoC::parse),
                    Sequence::<Guard>::parse,
                )),
            )),
        )(input)
        .map(|(rest, (predicate, _, _, _, instructions, guards))| {
            (rest, (predicate, instructions, guards))
        })
    }
}

const CONTEXT: ParserContext = ParserContext::Import;

impl<'a> ProgramAST<'a> for Import<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        vec![&self.predicate, &self.instructions]
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
                Token::directive_indicator,
                Token::directive_import,
                WSoC::parse,
                Self::parse_body,
            )),
        )(input)
        .map(|(rest, (_, _, _, (predicate, instructions, guards)))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    predicate,
                    instructions,
                    guards,
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
        ast::{directive::import::Import, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_import() {
        let test = vec![(
            "@import predicate :- csv { resource = \"test.csv\" }",
            ("predicate".to_string(), "csv".to_string()),
        )];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Import::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(
                expected,
                (
                    result.1.predicate().to_string(),
                    result.1.instructions().tag().unwrap().to_string()
                )
            );
        }
    }
}
