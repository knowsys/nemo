//! The statements of the written program a proof tree names, and their source positions.

use serde::{Deserialize, Serialize};

/// A rule of the written program, with its id, text and source position.
///
/// It is a rule statement, or a bound import (an import with a `where` clause),
/// which also derives facts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WrittenRule {
    /// The rule's statement id: its position in the written program.
    pub rule: usize,
    /// The rule's text, as written.
    pub text: String,
    /// The rule's `#[name]`, if it carries one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Where the rule is written (a module file for instantiated
    /// module rules).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceLocation>,
    /// The `@use` directives that instantiated the rule, outermost first,
    /// or none for a rule of the main program.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub instances: Vec<WrittenStatement>,
}

/// A statement of the program as written, by its text and position.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WrittenStatement {
    /// The statement's rendered text.
    pub text: String,
    /// Its source position.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceLocation>,
}

/// A source position: a registered file, a byte range within it,
/// and the line and column the range starts at, as an editor counts them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceLocation {
    /// The file's registered name.
    pub file: String,
    /// Byte offset of the first character.
    pub start: u32,
    /// Byte offset one past the last character.
    pub end: u32,
    /// The 1-based line of the first character.
    pub line: u32,
    /// The 1-based column of the first character, in characters.
    pub column: u32,
}
