use std::collections::HashMap;
use std::vec;

use futures::lock::Mutex;
use line_index::{LineCol, LineIndex, WideEncoding};
use nemo::io::parser::ast::program::Program;
use nemo::io::parser::ast::{AstNode, Position};
use nemo::io::parser::new::parse_program_str;
use nemo_position::{
    lsp_position_to_nemo_position, nemo_position_to_lsp_position, PositionConversionError,
};
use tower_lsp::lsp_types::{
    CompletionOptions, Diagnostic, DidChangeTextDocumentParams, DidOpenTextDocumentParams,
    DocumentChangeOperation, DocumentChanges, DocumentSymbol, DocumentSymbolOptions,
    DocumentSymbolParams, DocumentSymbolResponse, InitializeParams, InitializeResult,
    InitializedParams, Location, MessageType, OneOf, OptionalVersionedTextDocumentIdentifier,
    PrepareRenameResponse, Range, ReferenceParams, RenameOptions, RenameParams, ServerCapabilities,
    TextDocumentEdit, TextDocumentPositionParams, TextDocumentSyncCapability, TextDocumentSyncKind,
    TextEdit, Url, VersionedTextDocumentIdentifier, WorkDoneProgressOptions, WorkspaceEdit,
};
use tower_lsp::{Client, LanguageServer};

mod nemo_position;

#[derive(Debug)]
pub struct Backend {
    client: Client,
    state: Mutex<BackendState>, // TODO: Replace with RwLock, see https://github.com/rust-lang/futures-rs/pull/2082
}

#[derive(Debug)]
pub(crate) struct BackendState {
    text_document_store: HashMap<Url, TextDocumentInfo>,
}

#[derive(Debug, Clone)]
struct TextDocumentInfo {
    /// Content of the text document
    text: String,
    // Version information so that the language client can check if the server operated on the up to date version
    version: i32,
}

/// Converts a source position to a LSP position
pub(crate) fn line_col_to_position(
    line_index: &LineIndex,
    line_col: LineCol,
) -> Result<tower_lsp::lsp_types::Position, ()> {
    let wide_line_col = line_index
        .to_wide(WideEncoding::Utf16, line_col)
        .ok_or(())?;

    Ok(tower_lsp::lsp_types::Position {
        line: wide_line_col.line,
        character: wide_line_col.col,
    })
}

impl Backend {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            state: Mutex::new(BackendState {
                text_document_store: HashMap::new(),
            }),
        }
    }

    async fn handle_change(&self, text_document: VersionedTextDocumentIdentifier, text: &str) {
        self.state.lock().await.text_document_store.insert(
            text_document.uri.clone(),
            TextDocumentInfo {
                text: text.to_string(),
                version: text_document.version,
            },
        );

        let line_index = LineIndex::new(text);

        let (_program, errors) = parse_program_str(text);

        use std::collections::{BTreeMap, HashSet};
        let mut error_map: BTreeMap<Position, HashSet<String>> = BTreeMap::new();
        for error in &errors {
            if let Some(set) = error_map.get_mut(&error.pos) {
                set.insert(error.msg.clone());
            } else {
                let mut set = HashSet::new();
                set.insert(error.msg.clone());
                error_map.insert(error.pos, set);
            };
        }

        let diagnostics = error_map
            .into_iter()
            .map(|(pos, error_set)| Diagnostic {
                message: /*error.msg*/ {
                    format!("expected{}", {
                        let mut string = String::new();
                        for s in error_set {
                            string.push_str(" '");
                            string.push_str(s.as_str());
                            string.push_str("',");
                        }
                        string
                    })
                },
                range: Range::new(
                    line_col_to_position(
                        &line_index,
                        LineCol {
                            line: pos.line - 1,
                            col: pos.column - 1,
                        },
                    )
                    .unwrap(),
                    line_col_to_position(
                        &line_index,
                        LineCol {
                            line: pos.line - 1,
                            col: pos.column - 1 + 1,
                        },
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .collect();

        self.client
            .publish_diagnostics(
                text_document.uri.clone(),
                diagnostics,
                Some(text_document.version),
            )
            .await;
    }

    async fn read_text_document_info(&self, uri: &Url) -> Option<TextDocumentInfo> {
        if let Some(info) = self.state.lock().await.text_document_store.get(uri) {
            let a = info.clone();
            Some(a)
        } else {
            self.client
                .log_message(
                    MessageType::ERROR,
                    "could not find text document with URI {uri}",
                )
                .await;
            None
        }
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(
        &self,
        _: InitializeParams,
    ) -> tower_lsp::jsonrpc::Result<InitializeResult> {
        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                references_provider: Some(OneOf::Left(true)),
                document_symbol_provider: Some(OneOf::Right(DocumentSymbolOptions {
                    label: Some("Nemo".to_string()),
                    work_done_progress_options: WorkDoneProgressOptions {
                        ..Default::default()
                    },
                })),
                rename_provider: Some(OneOf::Right(RenameOptions {
                    prepare_provider: Some(true),
                    work_done_progress_options: WorkDoneProgressOptions {
                        ..Default::default()
                    },
                })),
                completion_provider: Some(CompletionOptions {
                    work_done_progress_options: WorkDoneProgressOptions {
                        ..Default::default()
                    },
                    ..Default::default()
                }),

                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "server initialized")
            .await;
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        self.handle_change(
            VersionedTextDocumentIdentifier {
                uri: params.text_document.uri,
                version: params.text_document.version,
            },
            &params.text_document.text,
        )
        .await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        self.handle_change(params.text_document, &params.content_changes[0].text)
            .await;
    }

    async fn references(
        &self,
        params: ReferenceParams,
    ) -> tower_lsp::jsonrpc::Result<Option<Vec<Location>>> {
        let info = self
            .read_text_document_info(&params.text_document_position.text_document.uri)
            .await;

        match info {
            Some(info) => {
                let text = info.text;
                let line_index = LineIndex::new(&text);
                let position = lsp_position_to_nemo_position(
                    &line_index,
                    params.text_document_position.position,
                )
                .unwrap(); // TODO handle unwrap

                let program = parse_program_str(&text);
                let program = program.0;

                let node_path = find_in_ast(&program, position);

                // Get most identifier most specific to the position
                let indentified_node = node_path_deepest_identifier(&node_path);
                let indentified_node = match indentified_node {
                    Some(indentified_node) => indentified_node,
                    None => return Ok(None),
                };

                // Find other AST nodes with the same global identifier
                let referenced_nodes =
                    find_by_identifier(indentified_node.scoping_node, &indentified_node.identifier);

                let locations = referenced_nodes
                    .iter()
                    .map(|node| Location {
                        uri: params.text_document_position.text_document.uri.clone(),
                        range: node_to_range_lsp(&line_index, *node),
                    })
                    .collect();

                Ok(Some(locations))
            }
            None => Ok(None), // TODO: Handle error
        }
    }

    async fn document_symbol(
        &self,
        params: DocumentSymbolParams,
    ) -> tower_lsp::jsonrpc::Result<Option<DocumentSymbolResponse>> {
        let info = self
            .read_text_document_info(&params.text_document.uri)
            .await;

        match info {
            Some(info) => {
                let text = info.text;
                let line_index = LineIndex::new(&text);

                let program = parse_program_str(&text);
                let program = program.0;

                let document_symbol = ast_node_to_document_symbol(&line_index, &program);

                if let Ok(document_symbol) = document_symbol {
                    return Ok(document_symbol.map(|document_symbol| {
                        DocumentSymbolResponse::Nested(document_symbol.children.unwrap())
                    }));
                }

                Ok(None)
            }
            None => Ok(None), // TODO: Handle error
        }
    }

    /// Finds references to symbol that was renamed and sends edit operations to language client
    async fn rename(
        &self,
        params: RenameParams,
    ) -> tower_lsp::jsonrpc::Result<Option<WorkspaceEdit>> {
        let info = self
            .read_text_document_info(&params.text_document_position.text_document.uri)
            .await;

        let info = match info {
            Some(info) => info,
            None => return Ok(None),
        };

        let text = info.text;
        let line_index = LineIndex::new(&text);
        let position =
            lsp_position_to_nemo_position(&line_index, params.text_document_position.position)
                .unwrap();

        let program = parse_program_str(&text);
        let program = program.0;

        let node_path = find_in_ast(&program, position);

        // Get most identifier most specific to the position
        let indentified_node = node_path_deepest_identifier(&node_path);
        let indentified_node = match indentified_node {
            Some(indentified_node) => indentified_node,
            None => return Ok(None),
        };

        // Find other AST nodes with the same global identifier
        let referenced_nodes =
            find_by_identifier(indentified_node.scoping_node, &indentified_node.identifier);

        let edit = TextDocumentEdit {
            text_document: OptionalVersionedTextDocumentIdentifier {
                uri: params.text_document_position.text_document.uri,
                version: Some(info.version),
            },
            edits: referenced_nodes
                .into_iter()
                .filter_map(|node| {
                    node.lsp_sub_node_to_rename().map(|renamed_node| {
                        OneOf::Left(TextEdit {
                            range: node_to_range_lsp(&line_index, renamed_node),
                            new_text: params.new_name.clone(),
                        })
                    })
                })
                .collect(),
        };

        Ok(Some(WorkspaceEdit {
            document_changes: Some(DocumentChanges::Operations(vec![
                DocumentChangeOperation::Edit(edit),
            ])),
            ..Default::default()
        }))
    }

    /// Tells the language client the range of the token that will be renamed
    async fn prepare_rename(
        &self,
        params: TextDocumentPositionParams,
    ) -> tower_lsp::jsonrpc::Result<Option<PrepareRenameResponse>> {
        let info = self
            .read_text_document_info(&params.text_document.uri)
            .await;

        let info = match info {
            Some(info) => info,
            None => return Ok(None),
        };

        let text = info.text;
        let line_index = LineIndex::new(&text);
        let position = lsp_position_to_nemo_position(&line_index, params.position).unwrap();

        let program = parse_program_str(&text);
        let program = program.0;

        let node_path = find_in_ast(&program, position);

        // Get identifier most specific to the position
        let indentified_node = node_path_deepest_identifier(&node_path);

        match indentified_node {
            Some(indentified_node) => {
                Ok(indentified_node
                    .node
                    .lsp_sub_node_to_rename()
                    .map(|renamed_node| {
                        PrepareRenameResponse::Range(node_to_range_lsp(&line_index, renamed_node))
                    }))
            }
            None => Ok(None),
        }
    }

    async fn shutdown(&self) -> tower_lsp::jsonrpc::Result<()> {
        Ok(())
    }
}

struct IdentifiedNode<'a> {
    node: &'a dyn AstNode,
    identifier: String,
    scoping_node: &'a dyn AstNode,
}

struct PariallyIdentifiedNode<'a> {
    node: &'a dyn AstNode,
    identifier: String,
    identifier_scope: String,
}

/// Get identifier most specific to the position of the node path
fn node_path_deepest_identifier<'a>(node_path: &[&'a dyn AstNode]) -> Option<IdentifiedNode<'a>> {
    let mut info = None;

    for node in node_path.iter().rev() {
        match info {
            None => {
                if let Some((identifier, identifier_scope)) = node.lsp_identifier() {
                    info = Some(PariallyIdentifiedNode {
                        node: *node,
                        identifier,
                        identifier_scope,
                    });
                }
            }
            Some(ref info) => {
                if let Some(parent_identifier) = node.lsp_identifier()
                    && parent_identifier.0.starts_with(&info.identifier_scope)
                {
                    return Some(IdentifiedNode {
                        node: info.node,
                        identifier: info.identifier.clone(),
                        scoping_node: *node,
                    });
                }
            }
        }
    }

    return info.map(|info| IdentifiedNode {
        node: info.node,
        identifier: info.identifier,
        scoping_node: *node_path.first().unwrap(),
    });
}

fn find_by_identifier<'a>(node: &'a dyn AstNode, identifier: &str) -> Vec<&'a dyn AstNode> {
    let mut references = Vec::new();

    find_by_identifier_recurse(node, identifier, &mut references);

    references
}

fn find_by_identifier_recurse<'a>(
    node: &'a dyn AstNode,
    identifier: &str,
    references: &mut Vec<&'a dyn AstNode>,
) {
    if node
        .lsp_identifier()
        .map(|(i, _)| i == identifier)
        .unwrap_or(false)
    {
        references.push(node);
    }

    if let Some(children) = node.children() {
        for child in children {
            find_by_identifier_recurse(child, identifier, references);
        }
    };
}

fn find_in_ast<'a>(node: &'a Program<'a>, position: Position) -> Vec<&'a dyn AstNode> {
    let mut path = Vec::new();

    find_in_ast_recurse(node, position, &mut path);

    path
}

fn find_in_ast_recurse<'a>(
    node: &'a dyn AstNode,
    position: Position,
    path: &mut Vec<&'a dyn AstNode>,
) {
    path.push(node);

    if let Some(children) = node.children() {
        for (child, next_child) in children.iter().zip(children.iter().skip(1)) {
            if next_child.position() > position {
                find_in_ast_recurse(*child, position, path);
                return;
            }
        }
        if let Some(child) = children.last() {
            find_in_ast_recurse(*child, position, path);
        }
    };
}

fn node_to_range_lsp(line_index: &LineIndex, node: &dyn AstNode) -> Range {
    Range {
        start: nemo_position_to_lsp_position(line_index, node.position()).unwrap(), // TODO: Improve error handling
        end: nemo_position_to_lsp_position(
            line_index,
            Position {
                offset: node.position().offset + node.span().len(),
                line: node.position().line + node.span().fragment().lines().count() as u32 - 1,
                column: if node.span().fragment().lines().count() > 1 {
                    1 + node.span().fragment().lines().last().unwrap().len() // TODO: Check if length is in correct encoding
                        as u32
                } else {
                    node.position().column + node.span().fragment().len() as u32
                    // TODO: Check if length is in correct encoding
                },
            },
        )
        .unwrap(),
    }
}

fn ast_node_to_document_symbol(
    line_index: &LineIndex,
    node: &dyn AstNode,
) -> Result<Option<DocumentSymbol>, PositionConversionError> {
    let range = node_to_range_lsp(line_index, node);

    let selection_range = range;

    if let Some((name, kind)) = node.lsp_symbol_info() {
        let children_results: Vec<_> = node
            .children()
            .into_iter()
            .flatten()
            .map(|child| ast_node_to_document_symbol(line_index, child))
            .collect();
        let mut children = Vec::with_capacity(children_results.len());
        for child_result in children_results {
            child_result?
                .into_iter()
                .for_each(|symbol| children.push(symbol))
        }
        let children = if children.is_empty() {
            None
        } else {
            Some(children)
        };

        Ok(Some(
            #[allow(deprecated)]
            DocumentSymbol {
                children,
                detail: None,
                kind,
                name,
                range,
                selection_range,
                tags: None,
                deprecated: None,
            },
        ))
    } else {
        Ok(None)
    }
}
