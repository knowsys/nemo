//! This module defines [Parser], which is responsible for parsing nemo programs.

pub mod ast;
pub mod context;
pub mod error;
pub mod input;
pub mod span;

use std::{cell::RefCell, ops::Range, rc::Rc};

use ariadne::{Color, Config, Label, Report, ReportKind, Source};
use ast::{program::Program, ProgramAST};
use error::{ParserError, ParserErrorTree};
use input::ParserInput;

use nom::IResult;

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
    /// Label of the input text, usually a path of the input file
    label: String,
    /// State of the parser
    state: ParserState,
}

/// Contains all errors that occurred during parsing
pub struct ParserErrorReport<'a> {
    /// Reference to the text that is going to be parsed
    input: &'a str,
    /// Label of the input text, usually a path of the input file
    label: String,
    /// List of [ParserError]s
    errors: Vec<ParserError>,
}

impl<'a> ParserErrorReport<'a> {
    /// Print the given reports.
    pub fn eprint(&self) -> Result<(), std::io::Error> {
        let reports = self.build_reports();

        for report in reports {
            report.eprint((self.label.clone(), Source::from(self.input)))?;
        }

        Ok(())
    }

    /// Write this report to a given writer.
    pub fn write(&self, writer: &mut impl std::io::Write) -> Result<(), std::io::Error> {
        let reports = self.build_reports();

        for report in reports {
            report.write((self.label.clone(), Source::from(self.input)), &mut *writer)?
        }

        Ok(())
    }

    /// Build a [Report] for each error.
    pub fn build_reports(&'a self) -> impl Iterator<Item = Report<'a, (String, Range<usize>)>> {
        self.errors.iter().map(move |error| {
            let message = error.to_string();

            Report::build(ReportKind::Error, self.label.clone(), error.position.offset)
                .with_message(message.clone())
                .with_label(
                    Label::new((self.label.clone(), error.position.range()))
                        .with_message(message)
                        .with_color(Color::Red),
                )
                .with_config(Config::default().with_index_type(ariadne::IndexType::Byte))
                .finish()
        })
    }

    /// Return raw [ParserError]s.
    pub fn errors(&self) -> &Vec<ParserError> {
        &self.errors
    }
}

impl std::fmt::Debug for ParserErrorReport<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reports = self.build_reports();

        for report in reports {
            report.fmt(f)?;
        }

        Ok(())
    }
}

impl std::fmt::Display for ParserErrorReport<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buffer = Vec::new();
        if self.write(&mut buffer).is_err() {
            return Err(std::fmt::Error);
        }

        write!(f, "{}", String::from_utf8(buffer).expect("invalid string"))
    }
}

impl<'a> Parser<'a> {
    /// Initialize the parser.
    pub fn initialize(input: &'a str, label: String) -> Self {
        Self {
            input,
            label,
            state: ParserState::default(),
        }
    }

    /// Parse the input.
    pub fn parse(self) -> Result<Program<'a>, (Box<Program<'a>>, ParserErrorReport<'a>)> {
        let parser_input = ParserInput::new(self.input, self.state.clone());

        let (_, program) = Program::parse(parser_input).expect("parsing should always succeed");

        if self.state.errors.borrow().is_empty() {
            Ok(program)
        } else {
            Err((
                Box::new(program),
                ParserErrorReport {
                    input: self.input,
                    label: self.label,
                    errors: Rc::try_unwrap(self.state.errors)
                        .expect("there should only be one owner now")
                        .into_inner(),
                },
            ))
        }
    }
}
