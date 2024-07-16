//! This module defines [Program].

use crate::parser::{
    context::{context, ParserContext},
    input::ParserInput,
    ParserResult,
};

use super::{rule::Rule, ProgramAST};

/// AST representation of a nemo program
#[derive(Debug)]
pub struct Program<'a> {
    rules: Rule<'a>,
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
        context(ParserContext::Program, Rule::parse)(input)
            .map(|(rest, result)| (rest, Program { rules: result }))
    }
}
