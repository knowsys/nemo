//! This module defines traits and data structures
//! relating to the language server protocol support.

use nemo::parser::{ast::ProgramAST, context::ParserContext, span::CharacterRange};
use tower_lsp::lsp_types::SymbolKind;

/// An LSP Identifier
#[derive(Debug)]
pub(super) struct LSPIdentifier {
    identifier: (ParserContext, String),
    scope: ParserContext,
}

impl LSPIdentifier {
    /// Get Indentifier String of [`LSPIdentifier`]
    pub(super) fn identifier(&self) -> &(ParserContext, String) {
        &self.identifier
    }

    /// Get Scope String of [`LSPIdentifier`]
    pub(super) fn scope(&self) -> &ParserContext {
        &self.scope
    }
}

/// Information about the symbol
#[derive(Debug)]
pub(super) struct LSPSymbolInfo {
    name: String,
    kind: SymbolKind,
}

impl LSPSymbolInfo {
    /// Get Name of [`LSPSymbolInfo`]
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    /// Get [`SymbolKind`] of [`LSPSymbolInfo`]
    pub(super) fn kind(&self) -> &SymbolKind {
        &self.kind
    }
}

/// Trait implemented by objects that correspond to
/// that correspond to objects identified by the LSP
pub(super) trait LSPComponent {
    /// Return a an [`LSPIdentifier`].
    ///
    /// The identifier scope will scope this identifier up to any [`LSPComponent`]
    /// that has the identifier scope as its type (aka. context).
    ///
    /// This can be used to restict rename operations to be local, e.g. for variable idenfiers inside of rules.
    fn identifier(&self) -> Option<LSPIdentifier>;

    /// Return information about this symbol, e.g. for syntax highlighting
    fn symbol_info(&self) -> Option<LSPSymbolInfo>;

    /// Range of the part of the node that should be renamed or [None] if the node can not be renamed
    fn range_renaming(&self) -> Option<CharacterRange>;
}

impl<'a, T: ?Sized> LSPComponent for T
where
    T: ProgramAST<'a>,
{
    fn identifier(&self) -> Option<LSPIdentifier> {
        let scope = match self.context() {
            ParserContext::Number | ParserContext::Variable | ParserContext::RdfLiteral => {
                Some(ParserContext::Rule)
            }
            ParserContext::Iri
            | ParserContext::Constant
            | ParserContext::String
            | ParserContext::StructureTag
            | ParserContext::Rule
            | ParserContext::Prefix => Some(ParserContext::Program),
            _ => None,
        };

        scope.map(|scope| LSPIdentifier {
            scope,
            identifier: (self.context(), self.span().fragment().to_string()),
        })
    }

    fn symbol_info(&self) -> Option<LSPSymbolInfo> {
        let kind = match self.context() {
            ParserContext::Base => {
                return Some(LSPSymbolInfo {
                    kind: SymbolKind::PROPERTY,
                    name: "Base".to_string(),
                })
            }
            ParserContext::Declare => {
                return Some(LSPSymbolInfo {
                    kind: SymbolKind::PROPERTY,
                    name: "Declare".to_string(),
                })
            }
            ParserContext::Import => {
                return Some(LSPSymbolInfo {
                    kind: SymbolKind::PROPERTY,
                    name: "Import".to_string(),
                })
            }
            ParserContext::Export => {
                return Some(LSPSymbolInfo {
                    kind: SymbolKind::PROPERTY,
                    name: "Export".to_string(),
                })
            }
            ParserContext::Prefix => {
                return Some(LSPSymbolInfo {
                    kind: SymbolKind::PROPERTY,
                    name: "Prefix".to_string(),
                })
            }
            ParserContext::Output => {
                return Some(LSPSymbolInfo {
                    kind: SymbolKind::PROPERTY,
                    name: "Output".to_string(),
                })
            }

            ParserContext::Rule => Some(SymbolKind::CLASS),
            ParserContext::Atom => Some(SymbolKind::FIELD),
            ParserContext::DataType => Some(SymbolKind::TYPE_PARAMETER),
            ParserContext::Variable => Some(SymbolKind::VARIABLE),
            ParserContext::Iri => Some(SymbolKind::STRING),
            ParserContext::Constant | ParserContext::RdfLiteral | ParserContext::Blank => {
                Some(SymbolKind::CONSTANT)
            }
            ParserContext::StructureTag => Some(SymbolKind::FUNCTION),
            ParserContext::Number => Some(SymbolKind::NUMBER),
            ParserContext::String => Some(SymbolKind::STRING),
            ParserContext::Boolean => Some(SymbolKind::BOOLEAN),
            ParserContext::Arithmetic
            | ParserContext::Operation
            | ParserContext::Aggregation
            | ParserContext::Negation => Some(SymbolKind::OPERATOR),
            _ => None,
        };

        kind.map(|kind| LSPSymbolInfo {
            kind,
            name: format!(
                "{}",
                self.span()
                    .fragment()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
        })
    }

    fn range_renaming(&self) -> Option<CharacterRange> {
        let allows_renaming = matches!(
            self.context(),
            ParserContext::Variable
                | ParserContext::Iri
                | ParserContext::Constant
                | ParserContext::Number
                | ParserContext::String
                | ParserContext::RdfLiteral
                | ParserContext::StructureTag
                | ParserContext::Prefix
        );

        allows_renaming.then_some(self.span().range())
    }
}
