//! This module defines [TranslationError]
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nemo_physical::datavalues::DataValueCreationError;
use thiserror::Error;

use crate::error::rich::RichError;

/// Types of errors that occur
/// while translating the ASP representation of a nemo program
/// into its logical representation.
#[derive(Assoc, Error, Debug, Clone)]
#[func(pub fn note(&self) -> Option<&'static str>)]
#[func(pub fn code(&self) -> usize)]
#[func(pub fn is_warning(&self) -> Option<()>)]
pub enum TranslationError {
    /// A non-atom was used in the head of a rule
    #[error(r#"{kind} used in rule head"#)]
    #[assoc(note = "rule head must only use atoms")]
    #[assoc(code = 101)]
    #[assoc(flag = true)]
    HeadNonAtom { kind: String },
    /// A non-literal was used in the body of a rule
    #[error(r#"{kind} used in rule body"#)]
    #[assoc(note = "rule body must only use literals or boolean operations")]
    #[assoc(code = 102)]
    BodyNonLiteral { kind: String },
    /// An undefined prefix was used
    #[error(r#"unknown prefix: `{prefix}`"#)]
    #[assoc(note = "prefix must be defined using @prefix")]
    #[assoc(code = 103)]
    PrefixUnknown { prefix: String },
    /// Unnamed non-anonymous variable
    #[error(r#"unnamed variable"#)]
    #[assoc(note = "variables starting with ? or ! must have a name")]
    #[assoc(code = 104)]
    UnnamedVariable,
    /// Named non-anonymous variable
    #[error(r#"anonymous variable with name: `{name}`"#)]
    #[assoc(note = "anonymous variables cannot have a name")]
    #[assoc(code = 105)]
    NamedAnonymous { name: String },
    /// Negation of a non-atom
    #[error(r#"found negated {kind}"#)]
    #[assoc(note = "negation can only be applied to atoms")]
    #[assoc(code = 106)]
    NegatedNonAtom { kind: String },
    /// Error in creating an any datavalue term
    #[error(transparent)]
    #[assoc(code = 107)]
    DataValueCreationError(DataValueCreationError),
    /// Unknown aggregation
    #[error(r#"unknown aggregation: `{aggregation}`"#)]
    #[assoc(note = "supported aggregates are sum, count, min, and max")]
    #[assoc(code = 108)]
    AggregationUnknown { aggregation: String },
    /// Distinct non-variable
    #[error(r#"expected variable found {kind}"#)]
    #[assoc(note = "the aggregation term is followed by a list of distinct variables")]
    #[assoc(code = 109)]
    AggregationDistinctNonVariable { kind: String },
    /// Infix expression as inner term
    #[error(r#"comparison not allowed within an atom"#)]
    #[assoc(code = 110)]
    InnerExpressionInfix,
    /// Negation as inner term
    #[error(r#"negation not allowed within an atom"#)]
    #[assoc(code = 111)]
    InnerExpressionNegation,
    /// Negation as inner term
    #[error(r#"unknown directive: `{directive}`"#)]
    #[assoc(code = 112)]
    DirectiveUnknown { directive: String },
    /// Base was redefined
    #[error(r#"base has been redefined"#)]
    #[assoc(note = "program may only contain one @base statement")]
    #[assoc(code = 113)]
    BaseRedefinition,
    /// Prefix was redefined
    #[error(r#"prefix has been redefined"#)]
    #[assoc(code = 114)]
    PrefixRedefinition,
    /// Unkown attribute
    #[error("unknown attribute: `{attribute}`")]
    #[assoc(code = 119)]
    AttributeUnknown { attribute: String },
    /// Unexpected attribute
    #[error("unexpected attribute: `{attribute}`")]
    #[assoc(code = 120)]
    AttributeUnexpected { attribute: String },
    /// Attributed defined multiple times
    #[error("attribute defined multiple times")]
    #[assoc(code = 121)]
    AttributeRedefined,
    /// Attribute contains wrong number of parameters
    #[error("attribute expected `{expected}` parameters, found `{found}`")]
    #[assoc(code = 122)]
    AttributeInvalidParameterCount { expected: usize, found: usize },
    /// Invalid attribute parameter: Wrong type
    #[error("attribute parameter of type `{found}`, expected `{expected}`")]
    #[assoc(code = 123)]
    AttributeParameterWrongType { expected: String, found: String },
    /// Invalid attribute parameter: Wrong component
    #[error("attribute parameter is `{found}`, expected `{expected}`")]
    #[assoc(code = 124)]
    AttributeParameterWrongComponent { expected: String, found: String },
    /// Non-variable-assignment in directive
    #[error("expected a variable assignment, found `{found}`")]
    #[assoc(code = 125)]
    DirectiveNonOperation { found: String },
    /// Arbitrary expression used in place of a fact
    #[error("expected a fact, found `{found}`")]
    #[assoc(code = 127)]
    ExpressionAsFact { found: String },
    /// Keys in a map have the wrong type
    #[error("attribute is of type {found}, but expected {expected}")]
    #[assoc(code = 128)]
    KeyWrongType { found: String, expected: String },
    /// parameter in a map is defined twice
    #[error("parameter {key} is defined multiple times")]
    #[assoc(code = 129)]
    MapParameterRedefined { key: String },
    /// parameter declaration using non-global variable
    #[error("parameter defines non-global variable")]
    #[assoc(note = "parameter names must have the form `$name'")]
    #[assoc(code = 130)]
    ParamDeclarationNotGlobal,

    /// Unsupported: Declare statements
    #[error(r#"declare statements are currently unsupported"#)]
    #[assoc(code = 899)]
    UnsupportedDeclare,
}

impl RichError for TranslationError {
    fn is_warning(&self) -> bool {
        self.is_warning().is_some()
    }

    fn message(&self) -> String {
        self.to_string()
    }

    fn code(&self) -> usize {
        self.code()
    }

    fn note(&self) -> Option<String> {
        self.note().map(str::to_owned)
    }
}
