//! This module defines [Operation].

use nom::sequence::{delimited, pair};

use crate::{
    parser::{
        ast::{
            comment::wsoc::WSoC, expression::Expression,
            sequence::simple::ExpressionSequenceSimple, tag::operation::OperationTag, token::Token,
            ProgramAST,
        },
        context::{context, ParserContext},
        input::ParserInput,
        span::ProgramSpan,
        ParserResult,
    },
    rule_model::components::term::operation::operation_kind::OperationKind,
};

/// A known operation applied to a series of [Expression]s.
///
/// This has the same structure as an [Operation].
#[derive(Debug)]
pub struct Operation<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Type of operation
    tag: OperationTag<'a>,
    /// List of underlying expressions
    expressions: ExpressionSequenceSimple<'a>,
}

impl<'a> Operation<'a> {
    /// Return an iterator over the underlying [Expression]s.
    pub fn expressions(&self) -> impl Iterator<Item = &Expression<'a>> {
        self.expressions.iter()
    }

    /// Return the type of this operation.
    pub fn kind(&self) -> OperationKind {
        self.tag.operation()
    }
}

const CONTEXT: ParserContext = ParserContext::Operation;

impl<'a> ProgramAST<'a> for Operation<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        let mut result = Vec::<&dyn ProgramAST>::new();
        result.push(&self.tag);

        for expression in &self.expressions {
            result.push(expression)
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
            pair(
                OperationTag::parse,
                delimited(
                    pair(Token::open_parenthesis, WSoC::parse),
                    ExpressionSequenceSimple::parse,
                    pair(WSoC::parse, Token::closed_parenthesis),
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

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;
    use strum::IntoEnumIterator;

    use crate::{
        parser::{
            ast::{expression::complex::operation::Operation, ProgramAST},
            input::ParserInput,
            ParserState,
        },
        rule_model::components::term::operation::operation_kind::OperationKind,
    };

    #[test]
    fn parse_operation() {
        println!(
            "{:?}",
            OperationKind::iter()
                .map(|kind| kind.name())
                .collect::<Vec<_>>()
        );

        let test = vec![
            ("SUM(1)", OperationKind::NumericSum),
            ("strlen(1)", OperationKind::StringLength),
            ("StrRev( 1 )", OperationKind::StringReverse),
            ("IsNumeric( 1 , 2 )", OperationKind::CheckIsNumeric),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Operation::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.kind());
        }
    }
}
