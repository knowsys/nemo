//! This module defines [Program].

use nom::{
    combinator::opt,
    multi::separated_list0,
    sequence::{delimited, pair},
};

use crate::parser::{
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

use super::{
    comment::{toplevel::TopLevelComment, wsoc::WSoC},
    statement::Statement,
    ProgramAST,
};

/// AST representation of a nemo program
#[derive(Debug)]
pub struct Program<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Top level comment
    comment: Option<TopLevelComment<'a>>,
    /// Statements
    statements: Vec<Statement<'a>>,
}

impl<'a> Program<'a> {
    /// Return the top-level comment attached to this program,
    /// if there is any
    pub fn comment(&self) -> Option<&TopLevelComment<'a>> {
        self.comment.as_ref()
    }

    /// Return an iterator of statements in the program.
    pub fn statements(&self) -> impl Iterator<Item = &Statement<'a>> {
        self.statements.iter()
    }
}

const CONTEXT: ParserContext = ParserContext::Program;

impl<'a> ProgramAST<'a> for Program<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        // TODO: Fix this once we have statements
        let mut result = Vec::<&dyn ProgramAST>::new();

        if let Some(comment) = self.comment() {
            result.push(comment);
        }

        for statement in self.statements() {
            result.push(statement);
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
                opt(TopLevelComment::parse),
                delimited(
                    WSoC::parse,
                    separated_list0(WSoC::parse, Statement::parse),
                    WSoC::parse,
                ),
            ), // pair(
               //     TopLevelComment::parse,
               //     WSoC::parse,
               //     // terminated(many0(preceded(WSoC::parse, Statement::parse)), WSoC::parse),
               // ),
        )(input)
        .map(|(rest, (comment, statements))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    comment,
                    statements,
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

    use crate::parser::{ast::ProgramAST, input::ParserInput, ParserState, Program};

    #[test]
    fn parse_program() {
        let program = "//! Top-level comment\n\
            // Declarations:\n\
            @declare a(_: int, _: int) .\n\
            @declare b(_: int, _: int) .\n\
            /// A fact\n\
            a(1, 2) .\n\
            \n\
            // Rules:\n\
            \n\
            /// A rule\n\
            b(?y, ?x) :- a(?x, ?y) .\n\
            \n\
            // Some more comments
        ";

        let parser_input = ParserInput::new(program, ParserState::default());
        let result = all_consuming(Program::parse)(parser_input);

        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.1.comment().is_some());
        assert_eq!(result.1.statements.len(), 4);
    }
}
