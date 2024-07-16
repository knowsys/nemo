//! This module defines different kinds of errors that can occur
//! while working with nemo programs.

pub mod translation_error;
pub mod validation_error;

use std::fmt::Display;

use translation_error::TranslationErrorKind;
use validation_error::ValidationErrorKind;

use crate::parser::{ast::ProgramAST, span::CharacterRange};

use super::origin::Origin;

/// Error that occurs during validation of a program.
#[derive(Debug)]
pub struct ValidationError {
    /// The kind of error
    kind: ValidationErrorKind,
    /// Stack of [Origin] from which the original AST node can be derived
    origin_stack: Vec<Origin>,
}

impl Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

/// Builder for [ValidationError]
#[derive(Debug, Default)]
pub struct ValidationErrorBuilder {
    /// Current stack of [Origin]
    origin_stack: Vec<Origin>,
    /// Current stack of [ValidationError]s
    error_stack: Vec<ValidationError>,
}

impl ValidationErrorBuilder {
    /// Push an [Origin] onto the stack.
    pub fn push_origin(&mut self, origin: Origin) {
        self.origin_stack.push(origin);
    }

    /// Pop the origin stack.
    pub fn pop_origin(&mut self) {
        self.origin_stack.pop();
    }

    /// Add a new error.
    pub fn report_error(&mut self, origin: &Origin, error_kind: ValidationErrorKind) {
        let mut origin_stack = self.origin_stack.clone();
        origin_stack.push(origin.clone());

        self.error_stack.push(ValidationError {
            kind: error_kind,
            origin_stack,
        })
    }

    /// Finish building and return a list of [ValidationError]s.
    pub fn finalize(self) -> Vec<ValidationError> {
        println!("Stack: {:?}", self.error_stack);
        self.error_stack
    }
}

/// Error that occurs while translating the ast into the logical representation
#[derive(Debug, Copy, Clone)]
pub struct TranslationError {
    /// The type of error that occurred
    kind: TranslationErrorKind,
    /// Range signifying the program part that should be highlighted
    range: CharacterRange,
}

impl TranslationError {
    /// Create a new [TranslationError] from a given .
    pub fn new<'a, Node: ProgramAST<'a>>(ast: &'a Node, kind: TranslationErrorKind) -> Self {
        Self {
            kind,
            range: ast.span().range(),
        }
    }
}

/// Error that may occur while translating or validating a nemo program
#[derive(Debug)]
pub enum ProgramError {
    /// Error occurred while translating
    /// the AST representation into the logical representation
    TranslationError(TranslationError),
    /// Error occurred while validating a certain program component
    ValidationError(ValidationError),
}

impl ProgramError {
    /// Return the message of the error
    pub fn message(&self) -> String {
        match self {
            ProgramError::TranslationError(error) => error.kind.to_string(),
            ProgramError::ValidationError(error) => error.kind.to_string(),
        }
    }

    /// Return the [CharacterRange] associated with this error
    pub fn range<'a, Node: ProgramAST<'a>>(&self, ast: &'a Node) -> CharacterRange {
        match self {
            ProgramError::TranslationError(error) => error.range,
            ProgramError::ValidationError(error) => ast
                .locate(&error.origin_stack)
                .expect("invalid origin")
                .span()
                .range(),
        }
    }

    /// Return the error code of the message
    pub fn error_code(&self) -> usize {
        123
    }

    /// Return an optional note that may be attached to the error
    pub fn note(&self) -> Option<String> {
        None
    }
}
