//! This module implements utility functions for parsing program components.

use std::fmt::{Display, Pointer};

use crate::rule_model::error::TranslationError;

#[derive(Debug)]
pub enum ComponentParseError {
    /// Parse Error
    ParseError,
    /// Translation Error
    TranslationError(TranslationError),
}

impl Display for ComponentParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentParseError::ParseError => f.write_str("error while parsing string"),
            ComponentParseError::TranslationError(error) => error.fmt(f),
        }
    }
}
