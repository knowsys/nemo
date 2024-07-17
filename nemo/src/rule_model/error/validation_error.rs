//! This module defines [ValidationErrorKind].
#![allow(missing_docs)]

use enum_assoc::Assoc;
use thiserror::Error;

use crate::rule_model::components::term::{
    aggregate::Aggregate, primitive::variable::Variable, Term,
};

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
    #[error(r#"unsafe variable used in computation: `{0}`"#)]
    #[assoc(code = 204)]
    OperationUnsafe(Variable),
    /// Unsafe variable used in multiple negative literals
    #[error(r#"unsafe variable used in multiple negative literals: `{0}`"#)]
    #[assoc(code = 205)]
    MultipleNegativeLiteralsUnsafe(Variable),
    /// Aggregate is used in body
    #[error(r#"aggregate used in rule body: `{0}`"#)]
    #[assoc(code = 206)]
    BodyAggregate(Aggregate),
    /// A variable is both universally and existentially quantified
    #[error(r#"variable is both universal and existential: `{0}`"#)]
    #[assoc(code = 207)]
    VariableMultipleQuantifiers(String),
    /// Fact contains non-ground term
    #[error(r#"non-ground term used in fact: `{0}`"#)]
    #[assoc(code = 208)]
    FactNonGround(Term),
    /// Invalid variable name was used
    #[assoc(code = 209)]
    #[error(r#"variable name is invalid: `{0}`"#)]
    InvalidVariableName(String),
    /// Invalid tag was used
    #[assoc(code = 210)]
    #[error(r#"tag is invalid: `{0}`"#)]
    InvalidTermTag(String),
    /// Invalid predicate name was used
    #[assoc(code = 211)]
    #[error(r#"predicate name is invalid: `{0}"#)]
    InvalidPredicateName(String),
    /// Unsupported feature: Multiple aggregates in one rule
    #[error(r#"multiple aggregates in one rule is currently unsupported"#)]
    #[assoc(code = 999)]
    AggregateMultiple,
    /// Unsupported feature: Aggregates combined with existential rules
    #[error(r#"aggregates and existential variables in one rule is currently unsupported"#)]
    #[assoc(code = 998)]
    AggregatesAndExistentials,
    /// Atom used without any arguments
    #[assoc(code = 997)]
    #[error(r#"atoms without arguments are currently unsupported"#)]
    AtomNoArguments,
    /// Non-primitive terms are currently unsupported
    #[assoc(code = 996)]
    #[error(r#"complex terms are currently unsupported"#)]
    ComplexTerm,
}
