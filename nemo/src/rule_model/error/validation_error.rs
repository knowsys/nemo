//! This module defines [ValidationErrorKind].
#![allow(missing_docs)]

use enum_assoc::Assoc;
use thiserror::Error;

use crate::rule_model::components::term::primitive::variable::Variable;

/// Types of errors that occur while building the logical rule model
#[derive(Assoc, Error, Debug)]
#[func(pub fn note(&self) -> Option<&'static str>)]
#[func(pub fn code(&self) -> usize)]
pub enum ValidationErrorKind {
    /// An existentially quantified variable occurs in the body of a rule.
    #[error(r#"existential variable used in rule body: `{0}`"#)]
    #[assoc(code = 201)]
    BodyExistential(Variable),
    /// Unsafe variable used in the head of the rule.
    #[error(r#"unsafe variable used in rule head: `{0}`"#)]
    #[assoc(
        note = "every universal variable in the head must occur at a safe position in the body"
    )]
    #[assoc(code = 202)]
    HeadUnsafe(Variable),
    /// Anonymous variable used in the head of the rule.
    #[error(r#"anonymous variable used in rule head"#)]
    #[assoc(code = 203)]
    HeadAnonymous,
    /// Operation with unsafe variable
    #[error(r#"unsafe variable used in operation: `{0}`"#)]
    #[assoc(
        note = "every universal variable used in an operation must occur at a safe position in the body"
    )]
    #[assoc(code = 204)]
    OperationUnsafe(Variable),
    /// Unsafe variable used in multiple negative literals
    #[error(r#"unsafe variable used in multiple negative literals: `{0}`"#)]
    #[assoc(code = 205)]
    MultipleNegativeLiteralsUnsafe(Variable),
    /// Aggregate is used in body
    #[error(r#"aggregate used in rule body"#)]
    #[assoc(code = 206)]
    BodyAggregate,
    /// A variable is both universally and existentially quantified
    #[error(r#"variable is both universal and existential: `{0}`"#)]
    #[assoc(code = 207)]
    VariableMultipleQuantifiers(String),
    /// Fact contains non-ground term
    #[error(r#"non-ground term used in fact"#)]
    #[assoc(code = 208)]
    FactNonGround,
    /// Invalid variable name was used
    #[error(r#"variable name is invalid: `{0}`"#)]
    #[assoc(code = 209)]
    #[assoc(note = "variable names may not start with double underscore")]
    InvalidVariableName(String),
    /// Invalid function term name was used
    #[error(r#"function name is invalid: `{0}`"#)]
    #[assoc(code = 210)]
    #[assoc(note = "function names may not start with double underscore")]
    InvalidTermTag(String),
    /// Invalid predicate name was used
    #[error(r#"predicate name is invalid: `{0}`"#)]
    #[assoc(code = 211)]
    #[assoc(note = "predicate names may not start with double underscore")]
    InvalidPredicateName(String),
    /// Invalid value type for aggregate
    #[error(r#"used aggregate term of type `{found}`, expected `{expected}`"#)]
    #[assoc(code = 212)]
    AggregateInvalidValueType { found: String, expected: String },
    /// Empty function term
    #[error(r#"function term without arguments"#)]
    #[assoc(code = 213)]
    FunctionTermEmpty,
    /// Wrong number of arguments for function
    #[error(r#"operation used with {used} arguments, expected {expected}"#)]
    #[assoc(code = 214)]
    OperationArgumentNumber { used: usize, expected: String },
    /// Anonymous variable used in operation
    #[error(r#"anonymous variable used in operation"#)]
    #[assoc(code = 215)]
    OperationAnonymous,

    /// Unsupported feature: Multiple aggregates in one rule
    #[error(r#"multiple aggregates in one rule is currently unsupported"#)]
    #[assoc(code = 999)]
    UnsupportedAggregateMultiple,
    /// Unsupported feature: Aggregates combined with existential rules
    #[error(r#"aggregates in existential rules in one rule is currently unsupported"#)]
    #[assoc(code = 998)]
    UnsupportedAggregatesAndExistentials,
    /// Atom used without any arguments
    #[assoc(code = 997)]
    #[error(r#"atoms without arguments are currently unsupported"#)]
    UnsupportedAtomEmpty,
    /// Non-primitive terms are currently unsupported
    #[assoc(code = 996)]
    #[error(r#"complex terms are currently unsupported"#)]
    UnsupportedComplexTerm,
}
