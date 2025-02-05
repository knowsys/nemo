//! This module defines
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nemo_physical::datavalues::ValueDomain;

/// Potential value types of terms.
/// A coarser, more user-friendly version of [`ValueDomain`]
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn name(&self) -> &'static str)]
pub enum ValueType {
    /// Boolean
    #[assoc(name = "boolean")]
    Boolean,
    /// Number
    #[assoc(name = "number")]
    Number,
    /// String
    #[assoc(name = "string")]
    String,
    /// Language string
    #[assoc(name = "lang-string")]
    LanguageString,
    /// Constant
    #[assoc(name = "constant")]
    Constant,
    /// Null
    #[assoc(name = "null")]
    Null,
    /// Map
    #[assoc(name = "map")]
    Map,
    /// Tuple
    #[assoc(name = "tuple")]
    Tuple,
    /// Function term
    #[assoc(name = "function term")]
    FunctionTerm,
    /// Other
    #[assoc(name = "other")]
    Other,
    /// Any
    #[assoc(name = "any")]
    Any,
}

impl From<ValueDomain> for ValueType {
    fn from(value: ValueDomain) -> Self {
        match value {
            ValueDomain::Float
            | ValueDomain::Double
            | ValueDomain::UnsignedLong
            | ValueDomain::NonNegativeLong
            | ValueDomain::UnsignedInt
            | ValueDomain::NonNegativeInt
            | ValueDomain::Long
            | ValueDomain::Int => ValueType::Number,
            ValueDomain::PlainString => ValueType::String,
            ValueDomain::LanguageTaggedString => ValueType::LanguageString,
            ValueDomain::Iri => ValueType::Constant,
            ValueDomain::Tuple => ValueType::Tuple,
            ValueDomain::Map => ValueType::Map,
            ValueDomain::Boolean => ValueType::Boolean,
            ValueDomain::Null => ValueType::Null,
            ValueDomain::Other => ValueType::Other,
        }
    }
}
