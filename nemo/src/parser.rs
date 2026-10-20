//! The parser: source text into program components.
//!
//! A hand-written lexer turns the whole input into a token buffer once,
//! every byte of the input ending up in a token.
//! A recursive descent parser with Pratt expression parsing consumes the buffer linearly.
//! It decides by peeking at the next tokens and never backtracks.
//! The program parser recovers per statement:
//! a broken statement becomes a diagnostic, the parser resumes after the next ` .`,
//! and every other statement still parses.
//!
//! The public surface is `parse` on the components.
//! [Program::parse] returns the program with all diagnostics.
//! The component forms ([Rule], [Fact], [Atom], [Term], [Statement], [ModuleName])
//! parse one component that must span the whole input.

mod combinators;
mod cursor;
mod directive;
mod lexer;
mod name;
mod statement;
mod term;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::ParseError,
    model::{Atom, Fact, FileId, ModuleName, Program, Rule, Statement, Term},
    parser::cursor::{Parser, excerpt},
};

pub(crate) use self::lexer::is_bare_name;

impl Program {
    /// Parses a whole source file.
    ///
    /// A statement that does not parse becomes a diagnostic,
    /// and every statement that parses is returned in the program.
    pub fn parse(source: &str, file: FileId) -> (Program, Vec<Diagnostic>) {
        let (statements, diagnostics) = Parser::program(source, file);
        (Program::new(statements), diagnostics)
    }
}

impl Statement {
    /// Parses one statement spanning the whole input.
    ///
    /// # Errors
    ///
    /// Returns the first problem with the input.
    pub fn parse(source: &str) -> Result<Self, Diagnostic> {
        Parser::component(source, |parser| parser.statement())
    }
}

impl Rule {
    /// Parses one rule spanning the whole input.
    ///
    /// # Errors
    ///
    /// Returns the first problem with the input, or an error if the statement is not a rule.
    pub fn parse(source: &str) -> Result<Self, Diagnostic> {
        match Statement::parse(source)? {
            Statement::Rule(rule) => Ok(rule),
            _ => Err(other_statement("a rule", source)),
        }
    }
}

impl Fact {
    /// Parses one fact spanning the whole input.
    ///
    /// # Errors
    ///
    /// Returns the first problem with the input, or an error if the statement is not a fact.
    pub fn parse(source: &str) -> Result<Self, Diagnostic> {
        match Statement::parse(source)? {
            Statement::Fact(fact) => Ok(fact),
            _ => Err(other_statement("a fact", source)),
        }
    }

    /// Parses one fact written with or without its terminator.
    ///
    /// Facts given on a command line or through a language binding
    /// often leave out the closing ` .`.
    ///
    /// # Errors
    ///
    /// Returns the first problem with the input, or an error if the statement is not a fact.
    pub fn parse_loose(source: &str) -> Result<Self, Diagnostic> {
        let text = source.trim();
        let statement = if text.ends_with('.') {
            Statement::parse(text)?
        } else {
            Statement::parse(&format!("{text} ."))
                .map_err(|problem| at_end_of_text(problem, text.len()))?
        };
        match statement {
            Statement::Fact(fact) => Ok(fact),
            _ => Err(other_statement("a fact", text)),
        }
    }
}

/// Returns the error for a statement that parses but is not of the `expected` kind,
/// quoting the start of its text.
fn other_statement(expected: &str, source: &str) -> Diagnostic {
    ParseError::Expected {
        expected: expected.to_owned(),
        found: format!("`{}`", excerpt(source.trim())),
    }
    .diagnostic()
}

/// Returns the problem of a text of the given length parsed with ` .` appended,
/// where the appended terminator, found in place of something else,
/// is reported as the end of the text that was given.
fn at_end_of_text(problem: Diagnostic, length: usize) -> Diagnostic {
    let at_terminator = problem
        .labels()
        .first()
        .is_some_and(|label| label.span.start as usize > length)
        && problem.field("found") == Some("`.`");
    let expected = problem
        .field("expected")
        .filter(|_| at_terminator)
        .map(str::to_owned);
    match expected {
        Some(expected) => ParseError::Expected {
            expected,
            found: String::from("the end of the input"),
        }
        .diagnostic(),
        None => problem,
    }
}

impl Atom {
    /// Parses one atom spanning the whole input.
    ///
    /// # Errors
    ///
    /// Returns the first problem with the input.
    pub fn parse(source: &str) -> Result<Self, Diagnostic> {
        Parser::component(source, |parser| parser.atom())
    }
}

impl Term {
    /// Parses one term spanning the whole input.
    ///
    /// # Errors
    ///
    /// Returns the first problem with the input.
    pub fn parse(source: &str) -> Result<Self, Diagnostic> {
        Parser::component(source, |parser| parser.term())
    }
}

impl ModuleName {
    /// Parses one module name spanning the whole input, as a `@use` writes it.
    ///
    /// # Errors
    ///
    /// Returns the first problem with the input.
    pub fn parse(source: &str) -> Result<Self, Diagnostic> {
        Parser::component(source, |parser| parser.module_name())
    }
}

#[cfg(test)]
mod tests;
