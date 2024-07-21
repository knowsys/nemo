//! This module defines the [Import] directive.

use nom::sequence::{preceded, separated_pair, tuple};

use crate::parser::{
    ast::{
        comment::wsoc::WSoC, expression::complex::map::Map, tag::structure::StructureTag,
        token::Token, ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

/// Import directive
#[derive(Debug)]
pub struct Import<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Predicate that is being Imported
    predicate: StructureTag<'a>,
    /// Map of instructions
    instructions: Map<'a>,
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

    /// Parse the left part of the import directive.
    fn parse_left_part(input: ParserInput<'a>) -> ParserResult<'a, StructureTag<'a>> {
        preceded(
            tuple((
                Token::at,
                Token::directive_import,
                WSoC::parse_whitespace_comment,
                WSoC::parse,
            )),
            StructureTag::parse,
        )(input)
    }

    /// Parse the right part of the import directive.
    fn parse_right_part(input: ParserInput<'a>) -> ParserResult<'a, Map<'a>> {
        Map::parse(input)
    }
}

const CONTEXT: ParserContext = ParserContext::Import;

impl<'a> ProgramAST<'a> for Import<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        vec![&self.predicate, &self.instructions]
    }

    fn span(&self) -> ProgramSpan<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(
            CONTEXT,
            separated_pair(
                Self::parse_left_part,
                tuple((WSoC::parse, Token::arrow, WSoC::parse)),
                Self::parse_right_part,
            ),
        )(input)
        .map(|(rest, (predicate, instructions))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    predicate,
                    instructions,
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
            "@import predicate :- csv { resource: \"test.csv\" }",
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
