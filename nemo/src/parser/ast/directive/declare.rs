//! This module defines the [Declare] directive.

use nom::sequence::{delimited, pair, preceded, tuple};

use crate::{
    parser::{
        ast::{
            comment::wsoc::WSoC,
            sequence::declare::DeclareSequence,
            tag::{parameter::Parameter, structure::StructureTag},
            token::Token,
            ProgramAST,
        },
        context::{context, ParserContext},
        input::ParserInput,
        span::ProgramSpan,
        ParserResult,
    },
    rule_model::components::datatype::DataType,
};

/// Declare directive, associating atom positions with names and data types
#[derive(Debug)]
pub struct Declare<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Predicate this statement applies to
    predicate: StructureTag<'a>,
    /// The declaration
    declaration: DeclareSequence<'a>,
}

impl<'a> Declare<'a> {
    /// Return the predicate this statement applies to.
    pub fn predicate(&self) -> &StructureTag<'a> {
        &self.predicate
    }

    /// Return an iterator over the name-type pairs.
    pub fn name_type_pairs(&self) -> impl Iterator<Item = (Parameter, DataType)> + '_ {
        self.declaration
            .iter()
            .map(|(parameter_name, tag_datatype)| {
                (parameter_name.parameter().clone(), tag_datatype.data_type())
            })
    }
}

const CONTEXT: ParserContext = ParserContext::Declare;

impl<'a> ProgramAST<'a> for Declare<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        let mut result = Vec::<&dyn ProgramAST>::new();
        result.push(&self.predicate);

        for (parameter, data_type) in self.declaration.iter() {
            result.push(parameter);
            result.push(data_type);
        }

        result
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
            preceded(
                tuple((
                    Token::at,
                    Token::directive_declare,
                    WSoC::parse_whitespace_comment,
                    WSoC::parse,
                )),
                pair(
                    StructureTag::parse,
                    delimited(
                        tuple((WSoC::parse, Token::open_parenthesis, WSoC::parse)),
                        DeclareSequence::parse,
                        tuple((WSoC::parse, Token::closed_parenthesis, WSoC::parse)),
                    ),
                ),
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
