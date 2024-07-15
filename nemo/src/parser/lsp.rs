//! This module defines traits and data structures
//! relating to the language server protocol support.
//! TODO: Document this better

use tower_lsp::lsp_types::SymbolKind;

use super::span::CharacterRange;

/// An LSP Identifier
#[derive(Debug)]
pub struct LSPIdentifier {
    identifier: String,
    scope: String,
}

/// Information about the symbol
#[derive(Debug)]
pub struct LSPSymbolInfo {
    name: String,
    kind: SymbolKind,
}

/// Trait implemented by objects that correspond to
/// that correspond to objects identified by the LSP
pub trait LSPComponent {
    /// Return a an [LSPIdentifier].
    ///
    /// The identifier scope will scope this identifier up to any [`AstNode`]
    /// that has an identifier that has this node's identifier scope as a prefix.
    ///
    /// This can be used to restict rename operations to be local, e.g. for variable idenfiers inside of rules.
    fn identifier(&self) -> Option<LSPIdentifier>;
    /// Return information about this symbol, e.g. for syntax highlighting
    fn symbol_info(&self) -> Option<LSPSymbolInfo>;
    /// Range of the part of the node that should be renamed or [None] if the node can not be renamed
    fn range_renaming(&self) -> Option<CharacterRange>;
}
