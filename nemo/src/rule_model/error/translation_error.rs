//! This module defines [TranslationErrorKind]
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nemo_physical::datavalues::DataValueCreationError;
use thiserror::Error;

/// Types of errors that occur
/// while translating the ASP representation of a nemo program
/// into its logical representation.
#[derive(Assoc, Error, Debug, Clone)]
#[func(pub fn note(&self) -> Option<&'static str>)]
#[func(pub fn code(&self) -> usize)]
pub enum TranslationErrorKind {
    /// A non-atom was used in the head of a rule
    #[error(r#"{0} used in rule head"#)]
    #[assoc(note = "rule head must only use atoms")]
    #[assoc(code = 101)]
    HeadNonAtom(String),
    /// A non-literal was used in the body of a rule
    #[error(r#"{0} used in rule body"#)]
    #[assoc(note = "rule body must only use literals or boolean operations")]
    #[assoc(code = 102)]
    BodyNonLiteral(String),
    /// An undefined prefix was used
    #[error(r#"unknown prefix: `{0}`"#)]
    #[assoc(note = "prefix must be defined using @prefix")]
    #[assoc(code = 103)]
    PrefixUnknown(String),
    /// Unnamed non-anonymous variable
    #[error(r#"unnamed variable"#)]
    #[assoc(note = "variables starting with ? or ! must have a name")]
    #[assoc(code = 104)]
    UnnamedVariable,
    /// Named non-anonymous variable
    #[error(r#"anonymous variable with name: ``"#)]
    #[assoc(note = "anonymous variables cannot have a name")]
    #[assoc(code = 105)]
    NamedAnonymous(String),
    /// Negation of a non-atom
    #[error(r#"found negated {0}"#)]
    #[assoc(note = "negation can only be applied to atoms")]
    #[assoc(code = 106)]
    NegatedNonAtom(String),
    /// Error in creating an any datavalue term
    #[error(transparent)]
    #[assoc(code = 107)]
    DataValueCreationError(DataValueCreationError),
    /// Unknown aggregation
    #[error(r#"unknown aggregation: `{0}`"#)]
    #[assoc(note = "supported aggregates are sum, count, min, and max")]
    #[assoc(code = 108)]
    AggregationUnknown(String),
    /// Distinct non-variable
    #[error(r#"expected variable found {0}"#)]
    #[assoc(note = "the aggregation term is followed by a list of distinct variables")]
    #[assoc(code = 109)]
    AggregationDistinctNonVariable(String),
    /// Infix expression as inner term
    #[error(r#"comparison not allowed within an atom"#)]
    #[assoc(code = 110)]
    InnerExpressionInfix,
    /// Negation as inner term
    #[error(r#"negation not allowed within an atom"#)]
    #[assoc(code = 111)]
    InnerExpressionNegation,
    /// Negation as inner term
    #[error(r#"unknown directive: `{0}`"#)]
    #[assoc(code = 112)]
    DirectiveUnknown(String),
    /// Base was redefined
    #[error(r#"base has been redefined"#)]
    #[assoc(note = "program may only contain one @base statement")]
    #[assoc(code = 113)]
    BaseRedefinition,
    /// Prefix was redefined
    #[error(r#"prefix has been redefined"#)]
    #[assoc(code = 114)]
    PrefixRedefinition,
    /// Unknown file format
    #[error(r#"unknown file format: `{0}`"#)]
    #[assoc(code = 115)]
    FileFormatUnknown(String),
    /// Missing file format
    #[error("missing file format")]
    #[assoc(code = 116)]
    FileFormatMissing,
    /// RDF unspecified missing extension
    #[error("no file extension specified")]
    #[assoc(note = "rdf imports/exports must have file extension nt, nq, ttl, trig, or rdf.")]
    #[assoc(code = 117)]
    RdfUnspecifiedMissingExtension,
    /// RDF unspecified missing extension
    #[error("`{0}` is not an rdf format")]
    #[assoc(note = "rdf imports/exports must have file extension nt, nq, ttl, trig, or rdf.")]
    #[assoc(code = 118)]
    RdfUnspecifiedUnknownExtension(String),
    /// Unkown attribute
    #[error("unknown attribute: `{0}`")]
    #[assoc(code = 119)]
    AttributeUnknown(String),
    /// Unexpected attribute
    #[error("unexpected attribute: `{0}`")]
    #[assoc(code = 120)]
    AttributeUnexpected(String),
    /// Attributed defined multiple times
    #[error("")]
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
    NonAssignment { found: String },
    /// Expected a ground term in a fact
    #[error("expected a ground term, found `{found}`")]
    #[assoc(code = 126)]
    NonGroundTerm { found: String },
    /// Arbitrary expression used in place of a fact
    #[error("expected a fact, found `{found}`")]
    #[assoc(code = 126)]
    ExpressionAsFact { found: String },

    /// Unsupported: Declare statements
    #[error(r#"declare statements are currently unsupported"#)]
    #[assoc(code = 899)]
    UnsupportedDeclare,
}
