//! This module defines [ValidationError].
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nemo_physical::{datavalues::ValueDomain, resource::ResourceValidationErrorKind};
use thiserror::Error;

use crate::{
    error::rich::RichError,
    rule_model::components::term::{
        primitive::variable::{existential::ExistentialVariable, Variable},
        Term,
    },
};

/// Types of errors that occur while building the logical rule model
#[derive(Assoc, Error, Clone, Debug)]
#[func(pub fn note(&self) -> Option<&'static str>)]
#[func(pub fn code(&self) -> usize)]
#[func(pub fn is_warning(&self) -> Option<()>)]
pub enum ValidationError {
    /// An existentially quantified variable occurs in the body of a rule.
    #[error(r#"existential variable used in rule body: `{variable}`"#)]
    #[assoc(code = 201)]
    BodyExistential { variable: ExistentialVariable },
    /// Unsafe variable used in the head of the rule.
    #[error(r#"unsafe variable used in rule head: `{variable}`"#)]
    #[assoc(
        note = "every universal variable in the head must occur at a safe position in the body"
    )]
    #[assoc(code = 202)]
    HeadUnsafe { variable: Variable },
    /// Anonymous variable used in the head of the rule.
    #[error(r#"anonymous variable used in rule head"#)]
    #[assoc(code = 203)]
    HeadAnonymous,
    /// Operation with unsafe variable
    #[error(r#"unsafe variable used in operation: `{variable}`"#)]
    #[assoc(
        note = "every universal variable used in an operation must occur at a safe position in the body"
    )]
    #[assoc(code = 204)]
    OperationUnsafe { variable: Variable },
    /// Unsafe variable used in multiple negative literals
    #[error(r#"unsafe variable used in multiple negative literals: `{variable}`"#)]
    #[assoc(code = 205)]
    MultipleNegativeLiteralsUnsafe { variable: Variable },
    /// Aggregate is used in body
    #[error(r#"aggregate used in rule body"#)]
    #[assoc(code = 206)]
    BodyAggregate,
    /// A variable is both universally and existentially quantified
    #[error(r#"variable is both universal and existential: `{variable_name}`"#)]
    #[assoc(code = 207)]
    VariableMultipleQuantifiers { variable_name: String },
    /// Fact contains non-ground term
    #[error(r#"non-ground term used in fact"#)]
    #[assoc(code = 208)]
    FactNonGround,
    /// Invalid variable name was used
    #[error(r#"variable name is invalid: `{variable_name}`"#)]
    #[assoc(code = 209)]
    #[assoc(note = "variable names may not start with double underscore")]
    InvalidVariableName { variable_name: String },
    /// Invalid function term name was used
    #[error(r#"function name is invalid: `{function_name}`"#)]
    #[assoc(code = 210)]
    #[assoc(note = "function names may not start with double underscore")]
    InvalidTermTag { function_name: String },
    /// Invalid predicate name was used
    #[error(r#"predicate name is invalid: `{predicate_name}`"#)]
    #[assoc(code = 211)]
    #[assoc(note = "predicate names may not start with double underscore")]
    InvalidPredicateName { predicate_name: String },
    /// Invalid value type for aggregate
    #[error(r#"used aggregate term of type `{found}`, expected `{expected}`"#)]
    #[assoc(code = 212)]
    AggregateInvalidValueType { found: String, expected: String },
    /// Aggregate has repeated distinct variables
    #[error(r#"found redundant distinct variable: `{variable}`"#)]
    #[assoc(code = 213)]
    #[assoc(note = "repeated distinct variables do not affect the result")]
    #[assoc(is_warning = ())]
    AggregateRepeatedDistinctVariable { variable: Variable },
    /// Aggregate variable cannot be group-by-variable
    #[error(r#"aggregation over group-by variable: `{variable}`"#)]
    #[assoc(code = 214)]
    #[assoc(note = "cannot aggregate over a variable that is also a group-by variable")]
    AggregateOverGroupByVariable { variable: Variable },
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
    AttributeRuleUnsafe { variable: Variable },
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
    #[error("`{format}` is not an rdf format")]
    #[assoc(note = "rdf imports/exports must have file extension nt, nq, ttl, trig, or rdf.")]
    #[assoc(code = 230)]
    RdfUnspecifiedUnknownExtension { format: String },
    /// Unknown file format
    #[error(r#"unknown file format: `{format}`"#)]
    #[assoc(code = 231)]
    ImportExportFileFormatUnknown { format: String },
    /// Unknown arity
    #[error(r#"arity of predicate {predicate} is unknown"#)]
    #[assoc(note = "arity of predicates in import/export statements must be known in advance.")]
    #[assoc(code = 232)]
    UnknownArity { predicate: String },
    /// IRI is invalid for HTTP request
    #[error(r#"IRI is invalid for HTTP request"#)]
    #[assoc(code = 233)]
    InvalidHttpIri,
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
    /// Variable assignemnts in directive have incorrect form
    #[error("expected a variable assignment of the form `?variable = term")]
    #[assoc(code = 237)]
    DirectiveNonAssignment,
    /// Variable assignemnts in directive have incorrect form
    #[error("expected a variable assignment to a ground term")]
    #[assoc(note = "ground terms are terms without variables")]
    #[assoc(code = 238)]
    DirectiveAssignmentNotGround,
    /// Directive received conflicting variable assignments
    #[error("variable `{variable}` has been defined multiple times")]
    #[assoc(code = 239)]
    DirectiveConflictingAssignments { variable: Variable },

    /// Stdin is only supported for one import
    #[error("expected at most one `stdin` import, found at least 2 occurrences")]
    #[assoc(code = 237)]
    ReachedStdinImportLimit,
    /// Ground operation contains invalid literals
    #[error("operation does not return a result")]
    #[assoc(code = 238)]
    #[assoc(is_warning = ())]
    InvalidGroundOperation,
    /// Import/Export parameter contains unspecified variables
    #[error("parameter value `{term}` is not a ground term")]
    #[assoc(code = 239)]
    ImportExportParameterNotGround { term: Term },
    /// Parameter declaration references undefined parameter
    #[error("parameter value references an undefined global")]
    #[assoc(code = 240)]
    #[assoc(note = "parameters can only reference parameters defined earlier")]
    ParameterDeclarationReferencesUndefinedGlobal,
    /// Parameter declaration has no definition
    #[error("parameter value needs to defined (via assignment or externally e.g. via --param on the cli)")]
    #[assoc(code = 241)]
    ParameterMissingDefinition,

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

impl RichError for ValidationError {
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
