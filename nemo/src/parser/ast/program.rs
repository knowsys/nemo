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
    statements: Rule<'a>,
}

impl<'a> Program<'a> {
    /// Return an iterator of statements in the program.
    pub fn statements(&self) -> &Rule<'a> {
        // TODO: This is simply a rule now
        &self.statements
    }
}

impl<'a> ProgramAST<'a> for Program<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        // TODO: Fix this once we have statements
        let mut result = Vec::<&dyn ProgramAST>::new();
        result.push(&self.statements);

        result
    }

    fn span(&self) -> crate::parser::span::ProgramSpan {
        // TODO: Fix this once we have statements
        self.statements.span()
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        context(ParserContext::Program, Rule::parse)(input)
            .map(|(rest, result)| (rest, Program { statements: result }))
    }
}
