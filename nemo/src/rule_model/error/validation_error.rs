//! This module defines [ValidationErrorKind].
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nemo_physical::{datavalues::ValueDomain, resource::ResourceValidationErrorKind};
use thiserror::Error;

use crate::rule_model::components::term::primitive::variable::Variable;

/// Types of errors that occur while building the logical rule model
#[derive(Assoc, Error, Clone, Debug)]
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
    /// Aggregate has repeated distinct variables
    #[error(r#"found repeated variable: `{variable}`"#)]
    #[assoc(code = 213)]
    #[assoc(note = "variables marked as distinct must not be repeated")]
    AggregateRepeatedDistinctVariable { variable: String },
    /// Aggregate variable cannot be group-by-variable
    #[error(r#"aggregation over group-by variable: `{variable}`"#)]
    #[assoc(code = 214)]
    #[assoc(note = "cannot aggregate over a variable that is also a group-by variable")]
    AggregateOverGroupByVariable { variable: String },
    /// Distinct variables in aggregate must be named universal variables
    #[error(r#"aggregation marks `{variable_type}` as distinct."#)]
    #[assoc(code = 215)]
    #[assoc(note = "distinct variables must be named universal variables")]
    AggregateDistinctNonNamedUniversal { variable_type: String },
    /// Empty function term
    #[error(r#"function term without arguments"#)]
    #[assoc(code = 216)]
    FunctionTermEmpty,
    /// Wrong number of arguments for function
    #[error(r#"operation used with {used} arguments, expected {expected}"#)]
    #[assoc(code = 217)]
    OperationArgumentNumber { used: usize, expected: String },
    /// Anonymous variable used in operation
    #[error(r#"anonymous variable used in operation"#)]
    #[assoc(code = 218)]
    OperationAnonymous,
    /// Inconsistent arities for predicates
    #[error(r#"predicate `{predicate}` used with arity {arity}."#)]
    #[assoc(code = 219)]
    #[assoc(note = "each predicate is only allowed to have one arity")]
    InconsistentArities { predicate: String, arity: usize },
    /// Import/Export: Missing required attribute
    #[error(r#"missing required parameter `{attribute}` in {direction} statement"#)]
    #[assoc(code = 220)]
    ImportExportMissingRequiredAttribute {
        attribute: String,
        direction: String,
    },
    /// Import/Export: Unrecognized parameter
    #[error(r#"file format {format} does not recognize parameter `{attribute}`"#)]
    #[assoc(code = 221)]
    ImportExportUnrecognizedAttribute { format: String, attribute: String },
    /// Import/Export: wrong input type for resource attribute
    #[error(r#"parameter `{parameter}` was given as a `{given}`, expected `{expected}`"#)]
    #[assoc(code = 222)]
    ImportExportAttributeValueType {
        parameter: String,
        given: String,
        expected: String,
    },
    /// Import/Export: dsv wrong value format
    #[error(r#"unknown {file_format} value format"#)]
    #[assoc(code = 223)]
    ImportExportValueFormat { file_format: String },
    /// Import/Export: negative limit
    #[error(r#"limit was negative"#)]
    #[assoc(code = 224)]
    ImportExportLimitNegative,
    /// Import/Export: delimiter
    #[error(r#"delimiter must be a single character"#)]
    #[assoc(code = 225)]
    ImportExportDelimiter,
    /// Import/Export: unknown compression format
    #[error(r#"unknown compression format `{format}`"#)]
    #[assoc(code = 226)]
    ImportExportUnknownCompression { format: String },
    /// Attribute in rule is unsafe
    #[error(r#"display attribute uses unsafe variable: `{variable}`"#)]
    #[assoc(code = 227)]
    AttributeRuleUnsafe { variable: String },
    /// Aggregation used in fact
    #[error(r#"aggregates may not be used in facts"#)]
    #[assoc(code = 228)]
    FactSubtermAggregate,
    /// RDF unspecified missing extension
    #[error("no file extension specified")]
    #[assoc(note = "rdf imports/exports must have file extension nt, nq, ttl, trig, or rdf.")]
    #[assoc(code = 229)]
    RdfUnspecifiedMissingExtension,
    /// RDF unspecified missing extension
    #[error("`{0}` is not an rdf format")]
    #[assoc(note = "rdf imports/exports must have file extension nt, nq, ttl, trig, or rdf.")]
    #[assoc(code = 230)]
    RdfUnspecifiedUnknownExtension(String),
    /// Unknown file format
    #[error(r#"unknown file format: `{0}`"#)]
    #[assoc(code = 231)]
    ImportExportFileFormatUnknown(String),
    /// Unknown arity
    #[error(r#"arity of predicate {predicate} is unknown"#)]
    #[assoc(note = "arity of predicates in import/export statements must be known in advance.")]
    #[assoc(code = 232)]
    UnknownArity { predicate: String },
    /// Invalid IRI
    #[error(r#"resource or endpoint is not a valid IRI"#)]
    #[assoc(code = 233)]
    InvalidIri,
    /// Invalid SPARQL query
    #[assoc(code = 234)]
    #[error(r#"invalid SPARQL query: {oxi_error}"#)]
    InvalidSparqlQuery { oxi_error: String },
    /// Error during resource validation
    #[assoc(code = 235)]
    #[error(transparent)]
    ResourceValidationError(#[from] ResourceValidationErrorKind),
    /// HTTP parameter is invalid
    #[assoc(code = 236)]
    #[error("HTTP parameter was given as `{given:?}`, expected one of: `{expected:?}`")]
    HttpParameterNotInValueDomain {
        /// Collection of expected [ValueDomain] for HTTP parameter
        expected: Vec<ValueDomain>,
        /// The actual [ValueDomain] of the HTTP parameter
        given: ValueDomain,
    },
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
    /// Unsupported feature: Exporting Json files
    #[error(r#"exporting in json is currently unsupported"#)]
    #[assoc(code = 995)]
    UnsupportedJsonExport,
    /// Unsupported feature: Rules without positive literals
    #[error(r#"rule without positive literals are currently unsupported"#)]
    #[assoc(code = 994)]
    UnsupportedNoPositiveLiterals,
}
