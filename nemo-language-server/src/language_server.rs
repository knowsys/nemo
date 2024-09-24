mod lsp_component;
mod nemo_position;
mod token_type;

use nemo::rule_model::translation::ProgramErrorReport;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::vec;
use strum::IntoEnumIterator;

use anyhow::anyhow;
use futures::lock::Mutex;
use line_index::{LineCol, LineIndex, WideEncoding};
use lsp_component::LSPComponent;
use nemo::parser::ast::program::Program;
use nemo::parser::ast::ProgramAST;
use nemo::parser::context::ParserContext;
use nemo::parser::span::{CharacterPosition, CharacterRange};
use nemo::parser::{Parser, ParserErrorReport};
use nemo_position::{
    lsp_position_to_nemo_position, nemo_range_to_lsp_range, PositionConversionError,
};
use token_type::TokenType;
use tower_lsp::lsp_types::{
    Diagnostic, DidChangeTextDocumentParams, DidOpenTextDocumentParams, DocumentChangeOperation,
    DocumentChanges, DocumentSymbol, DocumentSymbolOptions, DocumentSymbolParams,
    DocumentSymbolResponse, InitializeParams, InitializeResult, InitializedParams, Location,
    MessageType, OneOf, OptionalVersionedTextDocumentIdentifier, Position, PrepareRenameResponse,
    Range, ReferenceParams, RenameOptions, RenameParams, SemanticToken, SemanticTokens,
    SemanticTokensFullOptions, SemanticTokensLegend, SemanticTokensOptions, SemanticTokensParams,
    SemanticTokensResult, SemanticTokensServerCapabilities, ServerCapabilities, TextDocumentEdit,
    TextDocumentPositionParams, TextDocumentSyncCapability, TextDocumentSyncKind, TextEdit, Url,
    VersionedTextDocumentIdentifier, WorkDoneProgressOptions, WorkspaceEdit,
};
use tower_lsp::{Client, LanguageServer};

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
pub(crate) fn line_col_to_lsp_position(
    line_index: &LineIndex,
    line_col: LineCol,
) -> Result<tower_lsp::lsp_types::Position, PositionConversionError> {
    let wide_line_col = line_index
        .to_wide(WideEncoding::Utf16, line_col)
        .ok_or(PositionConversionError::LspLineCol(line_col))?;

    Ok(tower_lsp::lsp_types::Position {
        line: wide_line_col.line,
        character: wide_line_col.col,
    })
}

fn jsonrpc_error(error: anyhow::Error) -> tower_lsp::jsonrpc::Error {
    tower_lsp::jsonrpc::Error {
        code: tower_lsp::jsonrpc::ErrorCode::ServerError(1),
        message: error.to_string().into(),
        data: None,
    }
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

    /// Parses the Nemo Program and returns errors with their respective positions
    async fn handle_change(
        &self,
        text_document: VersionedTextDocumentIdentifier,
        text: &str,
    ) -> anyhow::Result<()> {
        self.state.lock().await.text_document_store.insert(
            text_document.uri.clone(),
            TextDocumentInfo {
                text: text.to_string(),
                version: text_document.version,
            },
        );

        let line_index = LineIndex::new(text);

        let (program, parse_errors): (Program, Option<ParserErrorReport>) =
            Parser::initialize(text, text_document.uri.to_string())
                .parse()
                .map(|prg| (prg, None))
                .unwrap_or_else(|(prg, err)| (*prg, Some(err)));

        let translation_result: Option<
            Result<nemo::rule_model::program::Program, ProgramErrorReport>,
        > = parse_errors.is_none().then(|| {
            nemo::rule_model::translation::ASTProgramTranslation::initialize(
                text,
                text_document.uri.to_string(),
            )
            .translate(&program)
        });

        // Group errors by position and deduplicate error
        let mut errors_by_posision: BTreeMap<CharacterRange, BTreeSet<String>> = BTreeMap::new();
        for error in parse_errors.iter().flat_map(|report| report.errors()) {
            if let Some(set) = errors_by_posision.get_mut(&CharacterRange::from(error.position)) {
                set.insert(format!("expected `{}`", error.context[0].name()));
            } else {
                errors_by_posision.insert(
                    CharacterRange::from(error.position),
                    std::iter::once(format!("expected `{}`", error.context[0].name())).collect(),
                );
            };
        }

        if let Some(Err(program_error_report)) = translation_result {
            for error in program_error_report.errors() {
                let range_opt = error.character_range(|origin| {
                    program_error_report
                        .origin_map()
                        .get(origin)
                        .map(|node| node.span().range())
                });
                let Some(range) = range_opt else {
                    continue;
                };

                let message = format!(
                    "{}{}{}",
                    error.message(),
                    error
                        .note()
                        .map(|n| format!("\nNote: {n}"))
                        .unwrap_or("".to_string()),
                    error
                        .hints()
                        .iter()
                        .map(|h| format!("\nHint: {h}"))
                        .collect::<String>(),
                );

                if let Some(set) = errors_by_posision.get_mut(&range) {
                    set.insert(message);
                } else {
                    errors_by_posision.insert(range, std::iter::once(message).collect());
                };
            }
        }

        let diagnostics = errors_by_posision
            .into_iter()
            .map(|(range, error_set)| {
                Ok(Diagnostic {
                    message: error_set.into_iter().collect::<Vec<_>>().join("\n\n"),
                    range: Range::new(
                        line_col_to_lsp_position(
                            &line_index,
                            LineCol {
                                line: range.start.line - 1,
                                col: range.start.column - 1,
                            },
                        )
                        .unwrap(),
                        line_col_to_lsp_position(
                            &line_index,
                            LineCol {
                                line: range.end.line - 1,
                                col: range.end.column - 1,
                            },
                        )
                        .unwrap(),
                    ),
                    ..Default::default()
                })
            })
            .filter_map(|result: Result<_, PositionConversionError>| result.ok())
            .collect();

        self.client
            .publish_diagnostics(
                text_document.uri.clone(),
                diagnostics,
                Some(text_document.version),
            )
            .await;

        Ok(())
    }

    async fn read_text_document_info(&self, uri: &Url) -> anyhow::Result<TextDocumentInfo> {
        if let Some(info) = self.state.lock().await.text_document_store.get(uri) {
            Ok(info.clone())
        } else {
            Err(anyhow!("could not find text document with URI {uri}"))
        }
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    /// Called when the language server is started from the client
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
                semantic_tokens_provider: Some(
                    SemanticTokensServerCapabilities::SemanticTokensOptions(
                        SemanticTokensOptions {
                            legend: SemanticTokensLegend {
                                token_types: TokenType::iter()
                                    .map(TokenType::to_semantic_token_type)
                                    .collect(),
                                token_modifiers: vec![],
                            },
                            full: Some(SemanticTokensFullOptions::Bool(true)),
                            ..Default::default()
                        },
                    ),
                ),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    /// Called when initialization finished on the client side
    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "server initialized")
            .await;
    }

    /// Called when a document is opened on the client side
    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        if let Err(error) = self
            .handle_change(
                VersionedTextDocumentIdentifier {
                    uri: params.text_document.uri,
                    version: params.text_document.version,
                },
                &params.text_document.text,
            )
            .await
        {
            self.client
                .log_message(
                    MessageType::ERROR,
                    format!("error while handling textDocument/didOpen request: {error}"),
                )
                .await;
        }
    }

    /// Called when the opened document changes
    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        if let Err(error) = self
            .handle_change(
                VersionedTextDocumentIdentifier {
                    uri: params.text_document.uri,
                    version: params.text_document.version,
                },
                &params.content_changes[0].text,
            )
            .await
        {
            self.client
                .log_message(
                    MessageType::ERROR,
                    format!("error while handling textDocument/didChange request: {error}"),
                )
                .await;
        }
    }

    /// Called when the client requests references of the symbol under the cursor
    async fn references(
        &self,
        params: ReferenceParams,
    ) -> tower_lsp::jsonrpc::Result<Option<Vec<Location>>> {
        let info = self
            .read_text_document_info(&params.text_document_position.text_document.uri)
            .await
            .map_err(jsonrpc_error)?;

        let text = info.text;
        let line_index = LineIndex::new(&text);
        let position =
            lsp_position_to_nemo_position(&line_index, params.text_document_position.position)
                .map_err(Into::into)
                .map_err(jsonrpc_error)?;

        let (program, _): (Program, Option<ParserErrorReport>) = Parser::initialize(
            &text,
            params.text_document_position.text_document.uri.to_string(),
        )
        .parse()
        .map(|prg| (prg, None))
        .unwrap_or_else(|(prg, err)| (*prg, Some(err)));

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
            .filter_map(|node| node_with_range(&line_index, *node))
            .map(|(_node, range)| {
                Ok(Location {
                    uri: params.text_document_position.text_document.uri.clone(),
                    range,
                })
            })
            .filter_map(|result: Result<_, ()>| result.ok())
            .collect();

        Ok(Some(locations))
    }

    /// Returns all DocumentSymbols in the Nemo Program, which are used to produce an outline
    async fn document_symbol(
        &self,
        params: DocumentSymbolParams,
    ) -> tower_lsp::jsonrpc::Result<Option<DocumentSymbolResponse>> {
        let info = self
            .read_text_document_info(&params.text_document.uri)
            .await
            .map_err(jsonrpc_error)?;

        let text = info.text;
        let line_index = LineIndex::new(&text);

        let (program, _): (Program, Option<ParserErrorReport>) =
            Parser::initialize(&text, params.text_document.uri.to_string())
                .parse()
                .map(|prg| (prg, None))
                .unwrap_or_else(|(prg, err)| (*prg, Some(err)));

        let document_symbols = ast_node_to_document_symbol(&line_index, &program)
            .map_err(Into::into)
            .map_err(jsonrpc_error)?
            .ok_or(anyhow!("program has no document symbol"))
            .map_err(jsonrpc_error)?;

        Ok(Some(DocumentSymbolResponse::Nested(document_symbols)))
    }

    /// Called to receive syntax highlighting information
    async fn semantic_tokens_full(
        &self,
        params: SemanticTokensParams,
    ) -> tower_lsp::jsonrpc::Result<Option<SemanticTokensResult>> {
        let info = self
            .read_text_document_info(&params.text_document.uri)
            .await
            .map_err(jsonrpc_error)?;

        let text = info.text;
        let line_index = LineIndex::new(&text);

        let (program, _): (Program, Option<ParserErrorReport>) =
            Parser::initialize(&text, params.text_document.uri.to_string())
                .parse()
                .map(|prg| (prg, None))
                .unwrap_or_else(|(prg, err)| (*prg, Some(err)));

        let token_types_with_ranges = ast_node_to_semantic_tokens(&line_index, &program);

        Ok(Some(SemanticTokensResult::Tokens(SemanticTokens {
            result_id: None,
            data: token_types_with_ranges
                .into_iter()
                .scan(Position::new(0, 0), |last_pos, (token_type, range)| {
                    let delta_line = range.start.line - last_pos.line;
                    let result: SemanticToken = SemanticToken {
                        delta_line,
                        delta_start: if delta_line == 0 {
                            range.start.character - last_pos.character
                        } else {
                            range.start.character
                        },
                        length: if range.start.line == range.end.line {
                            range.end.character - range.start.character
                        } else {
                            range.end.character
                        },
                        token_type: token_type as u32,
                        token_modifiers_bitset: 0,
                    };
                    *last_pos = range.start;
                    Some(result)
                })
                .collect(),
        })))
    }

    /// Finds references to symbol that was renamed and sends edit operations to language client
    async fn rename(
        &self,
        params: RenameParams,
    ) -> tower_lsp::jsonrpc::Result<Option<WorkspaceEdit>> {
        let info = self
            .read_text_document_info(&params.text_document_position.text_document.uri)
            .await
            .map_err(jsonrpc_error)?;

        let text = info.text;
        let line_index = LineIndex::new(&text);
        let position =
            lsp_position_to_nemo_position(&line_index, params.text_document_position.position)
                .map_err(Into::into)
                .map_err(jsonrpc_error)?;

        let (program, _): (Program, Option<ParserErrorReport>) = Parser::initialize(
            &text,
            params.text_document_position.text_document.uri.to_string(),
        )
        .parse()
        .map(|prg| (prg, None))
        .unwrap_or_else(|(prg, err)| (*prg, Some(err)));

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
                    node.range_renaming().map(|renamed_node_range| {
                        Ok({
                            OneOf::Left(TextEdit {
                                range: nemo_range_to_lsp_range(&line_index, renamed_node_range)
                                    .map_err(|_error| ())?, // TODO: Print error,
                                new_text: params.new_name.clone(),
                            })
                        })
                    })
                })
                .filter_map(|result: Result<_, ()>| result.ok())
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
            .await
            .map_err(jsonrpc_error)?;

        let text = info.text;
        let line_index = LineIndex::new(&text);
        let position = lsp_position_to_nemo_position(&line_index, params.position)
            .map_err(Into::into)
            .map_err(jsonrpc_error)?;

        let (program, _): (Program, Option<ParserErrorReport>) =
            Parser::initialize(&text, params.text_document.uri.to_string())
                .parse()
                .map(|prg| (prg, None))
                .unwrap_or_else(|(prg, err)| (*prg, Some(err)));

        let node_path = find_in_ast(&program, position);

        // Get identifier most specific to the position
        let indentified_node = node_path_deepest_identifier(&node_path);

        match indentified_node {
            Some(indentified_node) => Ok(Some(PrepareRenameResponse::Range(
                nemo_range_to_lsp_range(
                    &line_index,
                    indentified_node
                        .node
                        .range_renaming()
                        .ok_or_else(|| anyhow!("identified node can not be renamed"))
                        .map_err(jsonrpc_error)?,
                )
                .map_err(Into::into)
                .map_err(jsonrpc_error)?,
            ))),
            None => Ok(None),
        }
    }

    async fn shutdown(&self) -> tower_lsp::jsonrpc::Result<()> {
        Ok(())
    }
}

/// Associates a ProgramAST node with its corresponding range in the LSP world
fn node_with_range<'a>(
    line_index: &LineIndex,
    node: &'a dyn ProgramAST<'a>,
) -> Option<(&'a dyn ProgramAST<'a>, Range)> {
    nemo_range_to_lsp_range(line_index, node.span().range())
        .map(|range| (node, range)) // TODO: Handle error
        .ok()
}

struct IdentifiedNode<'a> {
    node: &'a dyn ProgramAST<'a>,
    identifier: (ParserContext, String),
    scoping_node: &'a dyn ProgramAST<'a>,
}

struct PartiallyIdentifiedNode<'a> {
    node: &'a dyn ProgramAST<'a>,
    identifier: (ParserContext, String),
    identifier_scope: ParserContext,
}

/// Get identifier most specific to the position of the node path
fn node_path_deepest_identifier<'a>(
    node_path: &[&'a dyn ProgramAST<'a>],
) -> Option<IdentifiedNode<'a>> {
    let mut info = None;

    for node in node_path.iter().rev() {
        match info {
            None => {
                if let Some(lsp_ident) = node.identifier() {
                    info = Some(PartiallyIdentifiedNode {
                        node: *node,
                        identifier: lsp_ident.identifier().clone(),
                        identifier_scope: *lsp_ident.scope(),
                    });
                }
            }
            Some(ref info) => {
                if let Some(parent_identifier) = node.identifier()
                    && parent_identifier.identifier().0 == info.identifier_scope
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

/// Finds all children of the given node (potentially the node itself) that match the identifier
fn find_by_identifier<'a>(
    node: &'a dyn ProgramAST<'a>,
    identifier: &(ParserContext, String),
) -> Vec<&'a dyn ProgramAST<'a>> {
    let mut references = Vec::new();

    find_by_identifier_recurse(node, identifier, &mut references);

    references
}

/// Actual implementation of [`find_by_identifier`]
fn find_by_identifier_recurse<'a>(
    node: &'a dyn ProgramAST<'a>,
    identifier: &(ParserContext, String),
    references: &mut Vec<&'a dyn ProgramAST<'a>>,
) {
    if node
        .identifier()
        .map(|ident| ident.identifier() == identifier)
        .unwrap_or(false)
    {
        references.push(node);
    }

    for child in node.children() {
        find_by_identifier_recurse(child, identifier, references);
    }
}

/// Returns the path of AST nodes that lead to a given position from a given node
fn find_in_ast<'a>(
    node: &'a Program<'a>,
    position: CharacterPosition,
) -> Vec<&'a dyn ProgramAST<'a>> {
    let mut path = Vec::new();

    find_in_ast_recurse(node, position, &mut path);

    path
}

/// Actual implementation of [`find_in_ast`]
fn find_in_ast_recurse<'a>(
    node: &'a dyn ProgramAST<'a>,
    position: CharacterPosition,
    path: &mut Vec<&'a dyn ProgramAST<'a>>,
) {
    path.push(node);

    for child in node.children() {
        let range = child.span().range();
        if range.start <= position && position < range.end {
            find_in_ast_recurse(child, position, path);
            break; // Assume no nodes overlap
        }
    }
}

/// Turns a given AST node into a DocumentSymbol to show in the outline; the DocumentSymbol has a
/// tree structure in itself so this function calls itself recursively.
fn ast_node_to_document_symbol<'a>(
    line_index: &LineIndex,
    node: &'a dyn ProgramAST<'a>,
) -> Result<Option<Vec<DocumentSymbol>>, PositionConversionError> {
    let range = nemo_range_to_lsp_range(line_index, node.span().range())?;

    let children_results: Vec<_> = node
        .children()
        .into_iter()
        .map(|child| ast_node_to_document_symbol(line_index, child))
        .collect();
    let mut children = Vec::with_capacity(children_results.len());
    for child_result in children_results {
        child_result?
            .into_iter()
            .flatten()
            .for_each(|symbol| children.push(symbol))
    }
    let children = if children.is_empty() {
        None
    } else {
        Some(children)
    };

    if let Some(symb_info) = node.symbol_info() {
        Ok(Some(vec![
            #[allow(deprecated)]
            DocumentSymbol {
                children,
                detail: None,
                kind: *symb_info.kind(),
                name: symb_info.name().to_string(),
                range,
                selection_range: range,
                tags: None,
                deprecated: None,
            },
        ]))
    } else {
        Ok(children)
    }
}

/// Returns syntax highlighting information for all children of the given node including the node
/// itself. Once a child has syntax highlighting information associated with it, the recursion does
/// not go any deeper.
fn ast_node_to_semantic_tokens<'a>(
    line_index: &LineIndex,
    node: &'a dyn ProgramAST<'a>,
) -> Vec<(TokenType, Range)> {
    if let Some(token_type) = TokenType::from_parser_context(node.context()) {
        let range_res = nemo_range_to_lsp_range(line_index, node.span().range());
        if let Ok(range) = range_res {
            vec![(token_type, range)]
        } else {
            vec![]
        }
    } else {
        node.children()
            .into_iter()
            .flat_map(|child| ast_node_to_semantic_tokens(line_index, child))
            .collect()
    }
}
