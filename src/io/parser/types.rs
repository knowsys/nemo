use std::num::{ParseFloatError, ParseIntError};

use nom::{
    error::{ErrorKind, FromExternalError},
    IResult,
};
use nom_locate::LocatedSpan;
use thiserror::Error;

/// A [`LocatedSpan`] over the input.
pub(super) type Span<'a> = LocatedSpan<&'a str>;

/// An intermediate parsing result
pub(super) type IntermediateResult<'a, T> = IResult<Span<'a>, T, LocatedParseError>;

/// The result of a parse
pub type ParseResult<'a, T> = Result<T, LocatedParseError>;

/// A [`ParseError`] at a certain location
#[derive(Debug, Error)]
#[error("Parse error on line {}, column {}: {}\nContext:\n{}", .line, .offset, .source, .context.iter().map(|ctx| ctx.to_string()).intersperse("\n".to_string()).collect::<String>())]
pub struct LocatedParseError {
    #[source]
    pub(super) source: ParseError,
    pub(super) line: u32,
    pub(super) offset: usize,
    pub(super) context: Vec<LocatedParseError>,
}

impl LocatedParseError {
    /// Append another [`LocatedParseError`] as context to this error.
    pub fn append(&mut self, other: LocatedParseError) {
        self.context.push(other)
    }
}

/// Errors that can occur during parsing.
#[derive(Debug, Error)]
pub enum ParseError {
    /// An external error during parsing.
    #[error(transparent)]
    ExternalError(#[from] Box<crate::error::Error>),
    /// A syntax error. Note that we cannot take [&'a str] here, as
    /// bounds on [std::error::Error] require ['static] lifetime.
    #[error("Syntax error: {0}")]
    SyntaxError(String),
    /// More input needed.
    #[error("Expected further input: {0}")]
    MissingInput(String),
    /// Use of an undeclared prefix.
    #[error(r#"Undeclared prefix "{0}""#)]
    UndeclaredPrefix(String),
    /// Re-declared prefix
    #[error(r#"Prefix "{0}" re-declared"#)]
    RedeclaredPrefix(String),
    /// An existentially quantified variable occurs in the body of a rule.
    #[error(r#"Variable "{0}" occurs existentially quantified in the rule body."#)]
    BodyExistential(String),
    /// A variable only occurs in negative literals in the rule body.
    #[error(r#"Variable "{0}" only occurs unsafely in negative literals in the rule body."#)]
    UnsafeNegatedVariable(String),
    /// The universal variable does not occur in a positive body literal.
    #[error(r#"The universal variable "{0}" does not occur in a positive body literal."#)]
    UnsafeHeadVariable(String),
    /// A variable used in a comparison does not occur in a positive body literal.
    #[error(
        r#"The variable "{0}" used in a comparison does not occur in a positive body literal."#
    )]
    UnsafeFilterVariable(String),
    /// A variable is both existentially and universally quantified
    #[error(r#"Variable "{0}" occurs with existential and universal quantification"#)]
    BothQuantifiers(String),
    /// An RDF data source declaration has arity != 3.
    #[error(
        r#"RDF data source for predicate "{0}" (from "{1}") has invalid arity {2}, should be 3"#
    )]
    RdfSourceInvalidArity(String, String, usize),
    /// A SPARQL query data source has an arity that doesn't match the number of variables given.
    #[error(
        r#"SPARQL data source for predicate "{0}" has arity {1}, but {2} variables are given"#
    )]
    SparqlSourceInvalidArity(String, usize, usize),
    /// Expected a dot.
    #[error(r#"Expected ".""#)]
    ExpectedDot,
}

impl ParseError {
    /// Locate this error by adding a position.
    pub fn at(self, position: Span) -> LocatedParseError {
        LocatedParseError {
            source: self,
            line: position.location_line(),
            offset: position.location_offset(),
            context: Vec::new(),
        }
    }
}

impl From<nom::Err<LocatedParseError>> for LocatedParseError {
    fn from(err: nom::Err<LocatedParseError>) -> Self {
        match err {
            nom::Err::Incomplete(_) => todo!(),
            nom::Err::Error(_) => todo!(),
            nom::Err::Failure(_) => todo!(),
        }
    }
}

impl From<nom::Err<LocatedParseError>> for crate::error::Error {
    fn from(err: nom::Err<LocatedParseError>) -> Self {
        crate::error::Error::ParseError(LocatedParseError::from(err))
    }
}

impl nom::error::ParseError<Span<'_>> for LocatedParseError {
    fn from_error_kind(input: Span, kind: ErrorKind) -> Self {
        ParseError::SyntaxError(kind.description().to_string()).at(input)
    }

    fn append(input: Span, kind: ErrorKind, other: Self) -> Self {
        let mut error = ParseError::SyntaxError(kind.description().to_string()).at(input);
        error.append(other);
        error
    }
}

impl FromExternalError<Span<'_>, crate::error::Error> for LocatedParseError {
    fn from_external_error(input: Span, _kind: ErrorKind, e: crate::error::Error) -> Self {
        ParseError::ExternalError(Box::new(e)).at(input)
    }
}

impl FromExternalError<Span<'_>, ParseIntError> for LocatedParseError {
    fn from_external_error(input: Span, _kind: ErrorKind, e: ParseIntError) -> Self {
        ParseError::ExternalError(Box::new(e.into())).at(input)
    }
}

impl FromExternalError<Span<'_>, ParseFloatError> for LocatedParseError {
    fn from_external_error(input: Span, _kind: ErrorKind, e: ParseFloatError) -> Self {
        ParseError::ExternalError(Box::new(e.into())).at(input)
    }
}
