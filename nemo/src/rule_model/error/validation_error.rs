//! This module defines [ValidationErrorKind].

use thiserror::Error;

use crate::rule_model::components::term::{
    aggregate::Aggregate, primitive::variable::Variable, Term,
};

/// Types of errors that occur while building the logical rule model
#[derive(Error, Debug)]
pub enum ValidationErrorKind {
    /// An existentially quantified variable occurs in the body of a rule.
    #[error(r#"existential variable used in rule body: `{0}`"#)]
    BodyExistential(Variable),
    /// Unsafe variable used in the head of the rule.
    #[error(r#"unsafe variable used in rule head: `{0}`"#)]
    HeadUnsafe(Variable),
    /// Anonymous variable used in the head of the rule.
    #[error(r#"anonymous variable used in rule head"#)]
    HeadAnonymous,
    /// Operation with unsafe variable
    #[error(r#"unsafe variable used in computation: `{0}`"#)]
    OperationUnsafe(Variable),
    /// Unsafe variable used in multiple negative literals
    #[error(r#"unsafe variable used in multiple negative literals: `{0}`"#)]
    MultipleNegativeLiteralsUnsafe(Variable),
    /// Aggregate is used in body
    #[error(r#"aggregate used in rule body: `{0}`"#)]
    BodyAggregate(Aggregate),
    /// Unsupported feature: Multiple aggregates in one rule
    #[error(r#"multiple aggregates in one rule is currently unsupported"#)]
    AggregateMultiple,
    /// Unsupported feature: Aggregates combined with existential rules
    #[error(r#"aggregates and existential variables in one rule is currently unsupported"#)]
    AggregatesAndExistentials,
    /// A variable is both universally and existentially quantified
    #[error(r#"variable is both universal and existential: `{0}`"#)]
    VariableMultipleQuantifiers(String),
    /// Fact contains non-ground term
    #[error(r#"non-ground term used in fact: `{0}`"#)]
    FactNonGround(Term),
    /// Atom used without any arguments
    #[error(r#"atoms without arguments are currently unsupported"#)]
    AtomNoArguments,
    /// Non-primitive terms are currently unsupported
    #[error(r#"complex terms are currently unsupported"#)]
    ComplexTerm,
    /// Invalid variable name was used
    #[error(r#"variable name is invalid: `{0}`"#)]
    InvalidVariableName(String),
    /// Invalid tag was used
    #[error(r#"tag is invalid: `{0}`"#)]
    InvalidTermTag(String),
    /// Invalid predicate name was used
    #[error(r#"predicate name is invalid: `{0}"#)]
    InvalidPredicateName(String),
}
