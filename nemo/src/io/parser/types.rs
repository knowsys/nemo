use std::{
    num::{ParseFloatError, ParseIntError},
    ops::Range,
    str::{CharIndices, Chars},
};

use nemo_physical::datavalues::DataValueCreationError;
use nom::{
    error::{ErrorKind, FromExternalError},
    AsBytes, IResult, InputIter, InputLength, InputTake, InputTakeAtPosition,
};
use nom_locate::LocatedSpan;
use thiserror::Error;

use crate::{
    io::formats::import_export::ImportExportError,
    io::lexer::ParserState,
    model::rule_model::{Aggregate, Constraint, Literal, Term},
};

use super::{ast::Position, Variable};

/// A [LocatedSpan] over the input.
pub(super) type Span<'a> = LocatedSpan<&'a str>;

/// Create a [Span][nom_locate::LocatedSpan] over the input.
pub fn span_from_str(input: &str) -> Span<'_> {
    Span::new(input)
}

/// An intermediate parsing result
pub(super) type IntermediateResult<'a, T> = IResult<Span<'a>, T, LocatedParseError>;

/// The result of a parse
pub type ParseResult<'a, T> = Result<T, LocatedParseError>;

/// A [ParseError] at a certain location
#[derive(Debug, Error)]
#[error("Parse error on line {}, column {}: {}\nat {}{}", .line, .column, .source, .fragment, format_parse_error_context(.context))]
pub struct LocatedParseError {
    #[source]
    pub source: ParseError,
    pub line: u32,
    pub column: usize,
    pub fragment: String,
    pub context: Vec<LocatedParseError>,
}

impl LocatedParseError {
    /// Append another [LocatedParseError] as context to this error.
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

/// Body may contain literals or filter expressions
#[derive(Debug, Clone)]
pub enum BodyExpression {
    /// Literal
    Literal(Literal),
    /// Constraint
    Constraint(Constraint),
}

/// Different operators allows in a constraint.
/// Has one entry for every variant in [Constraint].
#[derive(Debug, Clone, Copy)]
pub enum ConstraintOperator {
    /// Two terms are equal.
    Equals,
    /// Two terms are unequal.
    Unequals,
    /// Value of the left term is less than the value of the right term.
    LessThan,
    /// Value of the left term is greater than the value of the right term.
    GreaterThan,
    /// Value of the left term is less than or equal to the value of the right term.
    LessThanEq,
    /// Value of the left term is greater than or equal to the value of the right term.
    GreaterThanEq,
}

impl ConstraintOperator {
    /// Turn operator into [Constraint].
    pub(crate) fn into_constraint(self, left: Term, right: Term) -> Constraint {
        match self {
            ConstraintOperator::Equals => Constraint::Equals(left, right),
            ConstraintOperator::Unequals => Constraint::Unequals(left, right),
            ConstraintOperator::LessThan => Constraint::LessThan(left, right),
            ConstraintOperator::GreaterThan => Constraint::GreaterThan(left, right),
            ConstraintOperator::LessThanEq => Constraint::LessThanEq(left, right),
            ConstraintOperator::GreaterThanEq => Constraint::GreaterThanEq(left, right),
        }
    }
}

/// Defines arithmetic operators
#[derive(Debug, Clone, Copy)]
pub(super) enum ArithmeticOperator {
    Addition,
    Subtraction,
    Multiplication,
    Division,
}

/// Errors that can occur during parsing.
#[derive(Debug, Error)]
pub enum ParseError {
    /// An external error during parsing.
    #[error(transparent)]
    ExternalError(#[from] Box<crate::error::Error>),
    /// An error related to a file format.
    #[error(r#"unknown file format "{0}""#)]
    FileFormatError(String),
    /// A syntax error. Note that we cannot take [&'a str] here, as
    /// bounds on [std::error::Error] require ['static] lifetime.
    #[error("syntax error: {0}")]
    SyntaxError(String),
    /// More input needed.
    #[error("expected further input: {0}")]
    MissingInput(String),
    /// Use of an undeclared prefix.
    #[error(r#"undeclared prefix "{0}""#)]
    UndeclaredPrefix(String),
    /// Re-declared prefix
    #[error(r#"prefix "{0}" re-declared"#)]
    RedeclaredPrefix(String),
    /// An existentially quantified variable occurs in the body of a rule.
    #[error(r#"variable "{0}" occurs existentially quantified in the rule body"#)]
    BodyExistential(Variable),
    /// A wildcard pattern was used inside of the rule head.
    #[error(r#"rule head must not contain unnamed variables "_""#)]
    UnnamedInHead,
    /// The universal variable is not safe or derived.
    #[error(r#"variable "{0}" appears in the head but cannot be derived from the body"#)]
    UnsafeHeadVariable(Variable),
    /// Complex term uses an unsafe variable.
    #[error(r#"the value of variable "{1}" contained in term "{0}" cannot be derived"#)]
    UnsafeComplexTerm(String, Variable),
    /// The unsafe variable appears in multiple negative body literals.
    #[error(r#"the unsafe variable "{0}" appears in multiple negative body literals"#)]
    UnsafeVariableInMultipleNegativeLiterals(Variable),
    /// Constraint on unsafe unsafe variable may only use variables from that negated literal
    #[error(r#"Term "{0}" uses variable {1} from negated literal {2} but also the variable {3}, which does not appear in it."#)]
    ConstraintOutsideVariable(String, Variable, String, Variable),
    /// An aggregate term occurs in the body of a rule.
    #[error(r#"An aggregate term ("{0}") occurs in the body of a rule"#)]
    AggregateInBody(Aggregate),
    /// Multiple aggregates in one rule
    #[error("Currently, only one aggregate per rule is supported.")]
    MultipleAggregates,
    /// Aggregates cannot be used within existential rules
    #[error("Aggregates may not appear in existential rules.")]
    AggregatesPlusExistentials,
    /// A variable is both existentially and universally quantified
    #[error(r#"variables named "{0}" occur with existential and universal quantification"#)]
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
    /// SPARQL query data sources are currently not supported.
    #[error(r#"SPARQL data source for predicate "{0}" is not yet implemented"#)]
    UnsupportedSparqlSource(String),
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
    /// Expected an IRI identifier.
    #[error("Expected an IRI identifier (for e.g. predicate names or functions in term trees)")]
    ExpectedIriIdentifier,
    /// Expected an IRI as a constant.
    #[error("Expected an IRI as a constant name")]
    ExpectedIriConstant,
    /// Expected an IRI-like identifier.
    #[error(
        "Expected an IRI-like identifier (for e.g. predicate names or functions in term trees)."
    )]
    ExpectedIriLikeIdentifier,
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
    /// Expected a constraint.
    #[error("Expected a constraint")]
    ExpectedConstraint,
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
    /// Encountered a base declaration after any other directive.
    #[error("A @base declaration can only be the first statement in the program")]
    LateBaseDeclaration,
    /// Encountered a prefix declaration after any non-base non-prefix directive.
    #[error("A @prefix declaration must occur before any non-@base non-@prefix declarations.")]
    LatePrefixDeclaration,
    /// Expected a function term
    #[error("Expected a function term")]
    ExpectedFunctionTerm,
    /// Expected a known unary function
    #[error("Expected a known unary function")]
    ExpectedUnaryFunction,
    /// Expected a term tree (i.e. a term that can additionally involve e.g. arithmetic operations)
    #[error("Expected a term tree (i.e. a term that can additionally involve e.g. arithmetic operations)")]
    ExpectedPrimitiveTerm,
    /// Expected an aggregate
    #[error("Expected an aggregate term")]
    ExpectedAggregate,
    /// Expected an parenthesised expression.
    #[error("Expected an parenthesised expression")]
    ExpectedParenthesisedExpression,
    /// Expected an parenthesised term tree.
    #[error("Expected an parenthesised term tree")]
    ExpectedParenthesisedTerm,
    /// Unknown aggregate operation
    #[error(r#"Aggregate operation "{0}" is not known"#)]
    UnknownAggregateOperation(String),
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

impl FromExternalError<Span<'_>, ImportExportError> for LocatedParseError {
    fn from_external_error(input: Span<'_>, _kind: ErrorKind, e: ImportExportError) -> Self {
        ParseError::ExternalError(Box::new(e.into())).at(input)
    }
}

impl FromExternalError<Span<'_>, DataValueCreationError> for LocatedParseError {
    fn from_external_error(input: Span<'_>, _kind: ErrorKind, e: DataValueCreationError) -> Self {
        ParseError::ExternalError(Box::new(e.into())).at(input)
    }
}

use crate::io::lexer::Token;

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct Tokens<'a> {
    pub(crate) tok: &'a [Token<'a>],
}
impl<'a> Tokens<'a> {
    fn new(vec: &'a [Token]) -> Tokens<'a> {
        Tokens { tok: vec }
    }
}
impl<'a> AsBytes for Tokens<'a> {
    fn as_bytes(&self) -> &[u8] {
        todo!()
    }
}
impl<'a, T> nom::Compare<T> for Tokens<'a> {
    fn compare(&self, t: T) -> nom::CompareResult {
        todo!()
    }

    fn compare_no_case(&self, t: T) -> nom::CompareResult {
        todo!()
    }
}
// impl<'a> nom::ExtendInto for Tokens<'a> {
//     type Item;

//     type Extender;

//     fn new_builder(&self) -> Self::Extender {
//         todo!()
//     }

//     fn extend_into(&self, acc: &mut Self::Extender) {
//         todo!()
//     }
// }
impl<'a, T> nom::FindSubstring<T> for Tokens<'a> {
    fn find_substring(&self, substr: T) -> Option<usize> {
        todo!()
    }
}
impl<'a, T> nom::FindToken<T> for Tokens<'a> {
    fn find_token(&self, token: T) -> bool {
        todo!()
    }
}
impl<'a> InputIter for Tokens<'a> {
    type Item = &'a Token<'a>;

    type Iter = std::iter::Enumerate<std::slice::Iter<'a, Token<'a>>>;

    type IterElem = std::slice::Iter<'a, Token<'a>>;

    fn iter_indices(&self) -> Self::Iter {
        self.tok.iter().enumerate()
    }

    fn iter_elements(&self) -> Self::IterElem {
        self.tok.iter()
    }

    fn position<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Item) -> bool,
    {
        self.tok.iter().position(predicate)
    }

    fn slice_index(&self, count: usize) -> Result<usize, nom::Needed> {
        if self.tok.len() >= count {
            Ok(count)
        } else {
            Err(nom::Needed::Unknown)
        }
    }
}
impl<'a> InputLength for Tokens<'a> {
    fn input_len(&self) -> usize {
        self.tok.len()
    }
}
impl<'a> InputTake for Tokens<'a> {
    fn take(&self, count: usize) -> Self {
        Tokens {
            tok: &self.tok[0..count],
        }
    }

    fn take_split(&self, count: usize) -> (Self, Self) {
        (
            Tokens {
                tok: &self.tok[count..self.tok.len()],
            },
            Tokens {
                tok: &self.tok[0..count],
            },
        )
    }
}
impl<'a> InputTakeAtPosition for Tokens<'a> {
    type Item = &'a Token<'a>;

    fn split_at_position<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        todo!()
    }

    fn split_at_position1<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
        e: ErrorKind,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        todo!()
    }

    fn split_at_position_complete<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        todo!()
    }

    fn split_at_position1_complete<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
        e: ErrorKind,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        todo!()
    }
}
impl<'a> nom::Offset for Tokens<'a> {
    fn offset(&self, second: &Self) -> usize {
        todo!()
    }
}
impl<'a, R> nom::ParseTo<R> for Tokens<'a> {
    fn parse_to(&self) -> Option<R> {
        todo!()
    }
}
impl<'a, R> nom::Slice<R> for Tokens<'a> {
    fn slice(&self, range: R) -> Self {
        todo!()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Input<'a, 's> {
    pub(crate) input: crate::io::lexer::Span<'a>,
    pub(crate) parser_state: ParserState<'s>,
}
impl<'a, 's> Input<'a, 's> {
    fn new(input: &'a str, errors: ParserState<'s>) -> Input<'a, 's> {
        Input {
            input: Span::new(input),
            parser_state: errors,
        }
    }
}
impl ToRange for Input<'_, '_> {
    fn to_range(&self) -> Range<usize> {
        self.input.to_range()
    }
}

impl AsBytes for Input<'_, '_> {
    fn as_bytes(&self) -> &[u8] {
        self.input.fragment().as_bytes()
    }
}

impl<'a, 's> nom::Compare<Input<'a, 's>> for Input<'a, 's> {
    fn compare(&self, t: Input) -> nom::CompareResult {
        self.input.compare(t.as_bytes())
    }

    fn compare_no_case(&self, t: Input) -> nom::CompareResult {
        self.input.compare_no_case(t.as_bytes())
    }
}
impl nom::Compare<&str> for Input<'_, '_> {
    fn compare(&self, t: &str) -> nom::CompareResult {
        self.input.compare(t)
    }

    fn compare_no_case(&self, t: &str) -> nom::CompareResult {
        self.input.compare_no_case(t)
    }
}

impl nom::ExtendInto for Input<'_, '_> {
    type Item = char;

    type Extender = String;

    fn new_builder(&self) -> Self::Extender {
        self.input.new_builder()
    }

    fn extend_into(&self, acc: &mut Self::Extender) {
        self.input.extend_into(acc)
    }
}

impl nom::FindSubstring<&str> for Input<'_, '_> {
    fn find_substring(&self, substr: &str) -> Option<usize> {
        self.input.find_substring(substr)
    }
}

impl<'a, 'e, T> nom::FindToken<T> for Input<'a, 'e>
where
    &'a str: nom::FindToken<T>,
{
    fn find_token(&self, token: T) -> bool {
        self.input.find_token(token)
    }
}

impl<'a, 's> InputIter for Input<'a, 's> {
    type Item = char;

    type Iter = CharIndices<'a>;

    type IterElem = Chars<'a>;

    fn iter_indices(&self) -> Self::Iter {
        todo!()
    }

    fn iter_elements(&self) -> Self::IterElem {
        todo!()
    }

    fn position<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Item) -> bool,
    {
        todo!()
    }

    fn slice_index(&self, count: usize) -> Result<usize, nom::Needed> {
        self.input.slice_index(count)
    }
}

impl InputLength for Input<'_, '_> {
    fn input_len(&self) -> usize {
        self.input.input_len()
    }
}

impl InputTake for Input<'_, '_> {
    fn take(&self, count: usize) -> Self {
        Input {
            input: self.input.take(count),
            parser_state: self.parser_state,
        }
    }

    fn take_split(&self, count: usize) -> (Self, Self) {
        let (first, second) = self.input.take_split(count);
        (
            Input {
                input: first,
                parser_state: self.parser_state,
            },
            Input {
                input: second,
                parser_state: self.parser_state,
            },
        )
    }
}

impl InputTakeAtPosition for Input<'_, '_> {
    type Item = char;

    fn split_at_position<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        match self.input.position(predicate) {
            Some(n) => Ok(self.take_split(n)),
            None => Err(nom::Err::Incomplete(nom::Needed::new(1))),
        }
    }

    fn split_at_position1<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
        e: ErrorKind,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        todo!()
    }

    fn split_at_position_complete<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        match self.split_at_position(predicate) {
            Err(nom::Err::Incomplete(_)) => Ok(self.take_split(self.input_len())),
            res => res,
        }
    }

    fn split_at_position1_complete<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
        e: ErrorKind,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        match self.input.fragment().position(predicate) {
            Some(0) => Err(nom::Err::Error(E::from_error_kind(*self, e))),
            Some(n) => Ok(self.take_split(n)),
            None => {
                if self.input.fragment().input_len() == 0 {
                    Err(nom::Err::Error(E::from_error_kind(*self, e)))
                } else {
                    Ok(self.take_split(self.input_len()))
                }
            }
        }
    }
}

impl nom::Offset for Input<'_, '_> {
    fn offset(&self, second: &Self) -> usize {
        self.input.offset(&second.input)
    }
}

impl<R> nom::ParseTo<R> for Input<'_, '_> {
    fn parse_to(&self) -> Option<R> {
        todo!()
    }
}

impl<'a, 'e, R> nom::Slice<R> for Input<'a, 'e>
where
    &'a str: nom::Slice<R>,
{
    fn slice(&self, range: R) -> Self {
        Input {
            input: self.input.slice(range),
            parser_state: self.parser_state,
        }
    }
}

impl nom_greedyerror::Position for Input<'_, '_> {
    fn position(&self) -> usize {
        nom_greedyerror::Position::position(&self.input)
    }
}

impl std::fmt::Display for Input<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "line {}, column {}",
            self.input.location_line(),
            self.input.get_utf8_column()
        )
    }
}

pub(crate) trait ToRange {
    fn to_range(&self) -> Range<usize>;
}
