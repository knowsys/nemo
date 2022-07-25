use nom::{error, Err, IResult};
use std::num::{ParseFloatError, ParseIntError};
use thiserror::Error;

use crate::error::Error;

/// An intermediate parsing result
pub(super) type IntermediateResult<'a, T> = IResult<&'a str, T, Error>;

/// The result of a parse
pub type ParseResult<'a, T> = Result<T, Error>;

/// Errors that can occur during parsing.
#[derive(Debug, Error)]
pub enum ParseError {
    /// A syntax error. Note that we cannot take [&'a str] here, as
    /// bounds on [std::error::Error] require ['static] lifetime.
    #[error("Syntax error: {0}")]
    SyntaxError(#[from] error::Error<String>),
    /// Use of an undeclared prefix
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
    /// A variable is both existentially and universally quantified
    #[error(r#"Variable "{0}" occurs with existential and universal quantification"#)]
    BothQuantifiers(String),
}

impl<'a> From<error::Error<&'a str>> for Error {
    fn from(err: error::Error<&'a str>) -> Self {
        let error::Error { input, code } = err;
        Self::ParseError(
            error::Error::<String> {
                input: input.to_owned(),
                code,
            }
            .into(),
        )
    }
}

impl From<ParseError> for Err<Error> {
    fn from(e: ParseError) -> Self {
        Err::Error(Error::ParseError(e))
    }
}

impl<'a> error::ParseError<&'a str> for Error {
    fn from_error_kind(input: &'a str, kind: error::ErrorKind) -> Self {
        let nom_error: error::Error<String> = error::make_error(input.to_owned(), kind);
        Self::ParseError(nom_error.into())
    }

    fn append(input: &'a str, kind: error::ErrorKind, other: Self) -> Self {
        Self::ParseError(ParseError::SyntaxError(match other {
            Self::ParseError(ParseError::SyntaxError(err)) => {
                error::Error::append(input.to_owned(), kind, err)
            }
            _ => error::make_error(input.to_owned(), kind),
        }))
    }
}

impl<I> error::FromExternalError<I, Error> for Error {
    fn from_external_error(_input: I, _kind: error::ErrorKind, e: Error) -> Self {
        e
    }
}

impl<I> error::FromExternalError<I, ParseIntError> for Error {
    fn from_external_error(_input: I, _kind: error::ErrorKind, e: ParseIntError) -> Self {
        Self::ParseInt(e)
    }
}

impl<I> error::FromExternalError<I, ParseFloatError> for Error {
    fn from_external_error(_input: I, _kind: error::ErrorKind, e: ParseFloatError) -> Self {
        Self::ParseFloat(e)
    }
}
