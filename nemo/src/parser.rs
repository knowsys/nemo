//! This module defines [Parser], which is responsible for parsing nemo programs.

pub mod ast;
pub mod context;
pub mod error;
pub mod input;
pub mod span;

use std::{cell::RefCell, fmt::Display, rc::Rc};

use ast::{program::Program, ProgramAST};
use error::{ParserError, ParserErrorTree};
use input::ParserInput;

use nom::IResult;

use crate::{
    error::{context::ContextError, report::ProgramReport},
    rule_file::RuleFile,
};

/// State of the parser
#[derive(Debug, Clone, Default)]
pub struct ParserState {
    /// Collection of all errors that occurred during parsing
    errors: Rc<RefCell<Vec<ParserError>>>,
}

impl ParserState {
    /// Append a [ParserError] to the current list of errors.
    pub(crate) fn report_error(&self, error: ParserError) {
        self.errors.borrow_mut().push(error);
    }
}

/// Output of a nom parser function
pub type ParserResult<'a, Output> = IResult<ParserInput<'a>, Output, ParserErrorTree<'a>>;

/// Parser for the nemo rule language
#[derive(Debug)]
pub struct Parser<'a> {
    /// Reference to the text that is going to be parsed
    input: &'a str,
    /// State of the parser
    state: ParserState,
}

/// Contains all errors that occurred during parsing
#[derive(Debug)]
pub struct ParserErrorReport {
    /// List of [ParserError]s
    errors: Vec<ParserError>,
}

impl Display for ParserErrorReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, error) in self.errors.iter().enumerate() {
            if index > 0 {
                writeln!(f)?;
            }

            write!(f, "{error}")?;
        }

        Ok(())
    }
}

impl ParserErrorReport {
    /// Construct an empty report, indicating there is some unspecified error.
    pub fn empty() -> Self {
        Self {
            errors: Vec::default(),
        }
    }

    /// Convert this report to a list of [ContextError]s.
    pub fn context_errors(self) -> impl Iterator<Item = (ContextError, bool)> {
        self.errors.into_iter().map(|error| {
            let range = error.position.range();
            (ContextError::new(error, range), false)
        })
    }

    /// Convert this report to a [ProgramReport].
    pub fn program_report(self, file: RuleFile) -> ProgramReport {
        let mut report = ProgramReport::new(file);
        report.merge_parser(self);
        report
    }

    /// Return raw [ParserError]s.
    pub fn errors(&self) -> &Vec<ParserError> {
        &self.errors
    }
}

impl From<Vec<ParserError>> for ParserErrorReport {
    fn from(errors: Vec<ParserError>) -> Self {
        Self { errors }
    }
}

impl<'a> Parser<'a> {
    /// Initialize the parser.
    pub fn initialize(input: &'a str) -> Self {
        Self {
            input,
            state: ParserState::default(),
        }
    }

    /// Parse the input.
    pub fn parse(self) -> Result<Program<'a>, (Box<Program<'a>>, ParserErrorReport)> {
        let parser_input = ParserInput::new(self.input, self.state.clone());

        let (_, program) = Program::parse(parser_input).expect("parsing should always succeed");

        if self.state.errors.borrow().is_empty() {
            Ok(program)
        } else {
            Err((
                Box::new(program),
                ParserErrorReport {
                    errors: Rc::try_unwrap(self.state.errors)
                        .expect("there should only be one owner now")
                        .into_inner(),
                },
            ))
        }
    }
}
