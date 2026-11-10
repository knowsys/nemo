//! The parser, which turns source text into program components.
//!
//! A hand-written lexer first turns the whole input into a buffer of tokens.
//! Every byte of the input other than whitespace and comments belongs to a token,
//! and a text that the lexer rejects becomes an error token.
//! A recursive descent parser, with Pratt parsing for operators,
//! then reads the buffer from start to end.
//! It decides what to parse by looking at the next tokens, and it never backtracks.
//! The parser of a whole program recovers from errors statement by statement.
//! A statement that does not parse becomes a diagnostic, the parser resumes after the next ` .`,
//! and the other statements still parse.
//!
//! The public interface of the parser is the `parse` function of each component type.
//! [Program::parse] returns the program together with all diagnostics.
//! The other `parse` functions ([Rule], [Fact], [Atom], [Term], [Statement], [ModuleName])
//! parse one component, which must span the whole input, and fail at its first problem.

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

impl Program {
    /// Parses a whole source text into a program, and returns it with the diagnostics.
    ///
    /// A statement that does not parse becomes a diagnostic,
    /// and the program contains every statement that parses.
    /// The spans of the components and diagnostics have the file id 0,
    /// as those of the other `parse` functions do.
    /// [parse_with_file](Self::parse_with_file) parses a text that is registered with an
    /// [Engine](crate::engine::Engine).
    pub fn parse(source: &str) -> (Program, Vec<Diagnostic>) {
        Self::parse_with_file(source, FileId::new(0))
    }

    /// Parses a whole source text into a program as [parse](Self::parse) does,
    /// with `file` as the file id of the spans.
    ///
    /// `file` is the id under which the text is registered,
    /// as [Engine::add_source](crate::engine::Engine::add_source) returns it.
    /// The spans of the components and diagnostics then point into the registered text.
    pub fn parse_with_file(source: &str, file: FileId) -> (Program, Vec<Diagnostic>) {
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
    /// Returns the first problem with the input, or an error if the input is another kind of
    /// statement.
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
    /// Returns the first problem with the input, or an error if the input is another kind of
    /// statement.
    pub fn parse(source: &str) -> Result<Self, Diagnostic> {
        match Statement::parse(source)? {
            Statement::Fact(fact) => Ok(fact),
            _ => Err(other_statement("a fact", source)),
        }
    }

    /// Parses one fact spanning the whole input, with or without its closing ` .`.
    ///
    /// Whitespace around the fact is ignored.
    /// A fact written outside a program, such as on a command line,
    /// often leaves out the closing ` .`.
    ///
    /// # Errors
    ///
    /// Returns the first problem with the input, or an error if the input is another kind of
    /// statement.
    /// If the input ends where the parser expected more, the error says so,
    /// even though the parser was given the input with ` .` appended.
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

/// Returns the error for a statement that parses but is not of the `expected` kind.
///
/// The error quotes the start of the statement's text.
fn other_statement(expected: &str, source: &str) -> Diagnostic {
    ParseError::Expected {
        expected: expected.to_owned(),
        found: format!("`{}`", excerpt(source.trim())),
    }
    .diagnostic()
}

/// Returns the error for a text that was parsed with ` .` appended to it.
///
/// `problem` is the error of the parse, and `length` is the length of the text before
/// the ` .` was appended.
/// If the parser found the appended ` .` where it expected something else,
/// the returned error says that it found the end of the input instead, without a location.
/// Any other error is returned unchanged.
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
    /// Parses one module name spanning the whole input, written as in a `@use` directive.
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
