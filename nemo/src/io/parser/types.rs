use std::num::{ParseFloatError, ParseIntError};

use nom::{
    error::{ErrorKind, FromExternalError},
    IResult,
};
use nom_locate::LocatedSpan;
use thiserror::Error;

use crate::model::PrimitiveType;

/// A [`LocatedSpan`] over the input.
pub(super) type Span<'a> = LocatedSpan<&'a str>;

/// Create a [`Span`][nom_locate::LocatedSpan] over the input.
pub fn span_from_str(input: &str) -> Span<'_> {
    Span::new(input)
}

/// An intermediate parsing result
pub(super) type IntermediateResult<'a, T> = IResult<Span<'a>, T, LocatedParseError>;

/// The result of a parse
pub type ParseResult<'a, T> = Result<T, LocatedParseError>;

/// A [`ParseError`] at a certain location
#[derive(Debug, Error)]
#[error("Parse error on line {}, column {}: {}\nat {}{}", .line, .column, .source, .fragment, format_parse_error_context(.context))]
pub struct LocatedParseError {
    #[source]
    pub(super) source: ParseError,
    pub(super) line: u32,
    pub(super) column: usize,
    pub(super) fragment: String,
    pub(super) context: Vec<LocatedParseError>,
}

impl LocatedParseError {
    /// Append another [`LocatedParseError`] as context to this error.
    pub fn append(&mut self, other: LocatedParseError) {
        self.context.push(other)
    }
}

fn format_parse_error_context(context: &[LocatedParseError]) -> String {
    let mut fragments = Vec::new();

    for error in context {
        let error_string = format!("{error}");
        for line in error_string.split('\n') {
            fragments.push(format!("{}{line}", " ".repeat(2)));
        }
    }

    if fragments.is_empty() {
        String::new()
    } else {
        format!("\nContext:\n{}", fragments.join("\n"))
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
    /// Unknown logical type name in program.
    #[error(
        "A predicate declaration used an unknown type ({0}). The known types are: {}",
        PrimitiveType::type_representations().join(", ")
    )]
    ParseUnknownType(String),
    /// Expected a dot.
    #[error(r#"Expected "{0}""#)]
    ExpectedToken(String),
    /// Expected an Iriref.
    #[error("Expected an IRI")]
    ExpectedIriref,
    /// Expected a base declaration.
    #[error(r#"Expected a "@base" declaration"#)]
    ExpectedBaseDeclaration,
    /// Expected a prefix declaration.
    #[error(r#"Expected a "@prefix" declaration"#)]
    ExpectedPrefixDeclaration,
    /// Expected a predicate declaration.
    #[error(r#"Expected a "@declare" type declaration"#)]
    ExpectedPredicateDeclaration,
    /// Expected a prefix.
    #[error(r#"Expected a prefix"#)]
    ExpectedPnameNs,
    /// Expected a logical type name.
    #[error("Expected a type name")]
    ExpectedLogicalTypeName,
    /// Expected a data source declaration.
    #[error(r#"Expected a "@source" declaration"#)]
    ExpectedDataSourceDeclaration,
    /// Expected an output declaration.
    #[error(r#"Expected an "@output" declaration"#)]
    ExpectedOutputDeclaration,
    /// Expected a string literal.
    #[error("Expected a string literal")]
    ExpectedStringLiteral,
    /// Expected a statement.
    #[error("Expected a statement (i.e., either a fact or a rule)")]
    ExpectedStatement,
    /// Expected a fact.
    #[error("Expected a fact")]
    ExpectedFact,
    /// Expected a rule.
    #[error("Expected a rule")]
    ExpectedRule,
    /// Expected a prefixed name.
    #[error("Expected a prefixed name")]
    ExpectedPrefixedName,
    /// Expected a blank node label.
    #[error("Expected a blank node label")]
    ExpectedBlankNodeLabel,
    /// Expected an IRI as a predicate name.
    #[error("Expected an IRI as a predicate name")]
    ExpectedIriPred,
    /// Expected an IRI as a constant.
    #[error("Expected an IRI as a constant name")]
    ExpectedIriConstant,
    /// Expected a predicate name.
    #[error("Expected a predicate name")]
    ExpectedPredicateName,
    /// Expected a bare predicate name.
    #[error("Expected a bare name")]
    ExpectedBareName,
    /// Expected a ground term.
    #[error("Expected a ground term")]
    ExpectedGroundTerm,
    /// Expected an atom.
    #[error("Expected an atom")]
    ExpectedAtom,
    /// Expected a term.
    #[error("Expected a term")]
    ExpectedTerm,
    /// Expected a variable.
    #[error("Expected a variable")]
    ExpectedVariable,
    /// Expected a universally quantified variable.
    #[error("Expected a universally quantified variable")]
    ExpectedUniversalVariable,
    /// Expected an existentially quantified variable.
    #[error("Expected an existentially quantified variable")]
    ExpectedExistentialVariable,
    /// Expected a variable name.
    #[error("Expected a variable name")]
    ExpectedVariableName,
    /// Expected a literal.
    #[error("Expected a literal")]
    ExpectedLiteral,
    /// Expected a positive literal.
    #[error("Expected a positive literal")]
    ExpectedPositiveLiteral,
    /// Expected a negative literal.
    #[error("Expected a negative literal")]
    ExpectedNegativeLiteral,
    /// Expected a filter operator.
    #[error("Expected a filter operator")]
    ExpectedFilterOperator,
    /// Expected a filter expression.
    #[error("Expected a filter expression")]
    ExpectedFilterExpression,
    /// Expected a body expression.
    #[error("Expected a literal or a filter expression")]
    ExpectedBodyExpression,
    /// Expected an arithmetic expression.
    #[error("Expected an arithmetic expression")]
    ExpectedArithmeticExpression,
    /// Expected an arithmetic product expression.
    #[error("Expected an arithmetic product expression")]
    ExpectedArithmeticProduct,
    /// Expected an arithmetic factor expression.
    #[error("Expected an arithmetic factor expression")]
    ExpectedArithmeticFactor,
    /// Expected an arithmetic parenthesised expression.
    #[error("Expected an arithmetic parenthesised expression")]
    ExpectedArithmeticParens,
    /// Encountered a base declaration after any other directive.
    #[error("A @base declaration can only be the first statement in the program")]
    LateBaseDeclaration,
    /// Encountered a prefix declaration after any non-base non-prefix directive.
    #[error("A @prefix declaration must occur before any non-@base non-@prefix declarations.")]
    LatePrefixDeclaration,
}

impl ParseError {
    /// Locate this error by adding a position.
    pub fn at(self, position: Span) -> LocatedParseError {
        // miri doesn't like nom_locate, cf. https://github.com/fflorent/nom_locate/issues/88
        let column = if cfg!(not(miri)) {
            position.naive_get_utf8_column()
        } else {
            0
        };
        let fragment = if position.is_empty() {
            String::new()
        } else {
            let line = if cfg!(not(miri)) {
                String::from_utf8(position.get_line_beginning().to_vec())
                    .expect("input is valid UTF-8")
            } else {
                String::new()
            };
            format!("\"{line}\"\n{}^", "-".repeat(3 + column))
        };

        LocatedParseError {
            source: self,
            line: position.location_line(),
            column,
            fragment,
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

impl FromExternalError<Span<'_>, ParseError> for LocatedParseError {
    fn from_external_error(input: Span<'_>, kind: ErrorKind, e: ParseError) -> Self {
        let mut err = <Self as nom::error::ParseError<Span<'_>>>::from_error_kind(input, kind);
        err.append(e.at(input));
        err
    }
}

impl FromExternalError<Span<'_>, ParseIntError> for LocatedParseError {
    fn from_external_error(input: Span, _kind: ErrorKind, e: ParseIntError) -> Self {
        ParseError::ExternalError(Box::new(crate::error::ReadingError::into(e.into()))).at(input)
    }
}

impl FromExternalError<Span<'_>, ParseFloatError> for LocatedParseError {
    fn from_external_error(input: Span, _kind: ErrorKind, e: ParseFloatError) -> Self {
        ParseError::ExternalError(Box::new(crate::error::ReadingError::into(e.into()))).at(input)
    }
}

impl FromExternalError<Span<'_>, crate::error::ReadingError> for LocatedParseError {
    fn from_external_error(
        input: Span<'_>,
        _kind: ErrorKind,
        e: crate::error::ReadingError,
    ) -> Self {
        ParseError::ExternalError(Box::new(e.into())).at(input)
    }
}
