//! This module defines [Program].

use ascii_tree::write_tree;

use nom::{
    combinator::opt,
    multi::many0,
    sequence::{delimited, pair},
};

use crate::parser::{
    ast::ast_to_ascii_tree,
    context::{context, ParserContext},
    error::{recover, report_error},
    input::ParserInput,
    span::Span,
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
    /// [Span] associated with this node
    span: Span<'a>,

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

    /// Return a formatted ascii tree to pretty print the AST
    pub fn ascii_tree(&self) -> String {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self)).unwrap();
        output.to_string()
    }
}

const CONTEXT: ParserContext = ParserContext::Program;

impl<'a> ProgramAST<'a> for Program<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        if let Some(comment) = self.comment() {
            result.push(comment);
        }

        for statement in self.statements() {
            result.push(statement);
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
            pair(
                opt(TopLevelComment::parse),
                many0(delimited(
                    WSoC::parse,
                    recover(report_error(Statement::parse)),
                    WSoC::parse,
                )),
            ),
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

impl std::fmt::Display for Program<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.ascii_tree())
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{ast::ProgramAST, input::ParserInput, ParserState, Program};

    #[test]
    fn parse_program() {
        let program = "%! Top-level comment\n\
            % Declarations:\n\
            @declare a(_: int, _: int) .\n\
            @declare b(_: int, _: int) .\n\
            %%% A fact\n\
            a(1, 2) .\n\
            \n\
            % Rules:\n\
            \n\
            %%% A rule\n\
            b(?y, ?x) :- a(?x, ?y) .\n\
            \n\
            % Some more comments
        ";

        let parser_input = ParserInput::new(program, ParserState::default());
        let result = all_consuming(Program::parse)(parser_input);

        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.1.comment().is_some());
        assert_eq!(result.1.statements.len(), 4);
    }

    // TODO: This test cases causes a warning in miri
    #[test]
    #[cfg_attr(miri, ignore)]
    fn parser_recover() {
        let program = "%! Top-level comment\n\
            % Declarations:\n\
            @declare oops a(_: int, _: int) .\n\
            @declare b(_: int, _: int) .\n\
            %%% A fact\n\
            a(1, 2) \n\
            \n\
            % Rules:\n\
            \n\
            %%% A rule\n\
            b(?y, ?x) <- a(?x, ?y) .\n\
            \n\
            c(?y, ?x) :- a(?x, ?y) .\n\
            % Some more comments
        ";

        let parser_input = ParserInput::new(program, ParserState::default());
        let result = Program::parse(parser_input.clone())
            .expect("This should not fail")
            .1;

        assert!(result.comment.is_some());
        assert_eq!(result.statements.len(), 4);
        assert_eq!(parser_input.state.errors.borrow().len(), 2);
    }
}
