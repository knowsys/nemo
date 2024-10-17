//! This module defines
#![allow(missing_docs)]

use enum_assoc::Assoc;

/// Potential value types of terms
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
