//! This module defines [Program].

use crate::parser::{
    context::{context, ParserContext},
    input::ParserInput,
    ParserResult,
};

use super::{basic::number::Number, ProgramAST};

///
#[derive(Debug)]
pub struct Program<'a> {
    number: Number<'a>,
}

impl<'a> ProgramAST<'a> for Program<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        todo!()
    }

    fn span(&self) -> crate::parser::span::ProgramSpan {
        todo!()
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        context(ParserContext::Program, Number::parse)(input)
            .map(|(rest, result)| (rest, Program { number: result }))
    }
}
