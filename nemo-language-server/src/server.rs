//! The protocol shell around the language core.
//!
//! Everything here is translation: protocol types in, core types out,
//! and back. The knowledge of what a Nemo program means lives in
//! [Workspace] and the index it keeps, so
//! this file can be replaced with a different framework without taking
//! any of that with it.

use std::{collections::HashMap, sync::Arc};

use nemo::model::{AggregateKind, Operator};
use nemo_physical::diagnostics::{Diagnostic as Report, Severity};
use tokio::sync::RwLock;
use tower_lsp_server::{
    Client, LanguageServer,
    jsonrpc::{self, Result},
    ls_types::{
        CompletionItem, CompletionItemKind, CompletionOptions, CompletionParams,
        CompletionResponse, Diagnostic, DiagnosticRelatedInformation, DiagnosticSeverity,
        DidChangeTextDocumentParams, DidCloseTextDocumentParams, DidOpenTextDocumentParams,
        DocumentFormattingParams, DocumentHighlight, DocumentHighlightKind,
        DocumentHighlightParams, DocumentSymbol, DocumentSymbolParams, DocumentSymbolResponse,
        GotoDefinitionParams, GotoDefinitionResponse, Hover, HoverContents, HoverParams,
        HoverProviderCapability, InitializeParams, InitializeResult, InlayHint, InlayHintKind,
        InlayHintLabel, InlayHintParams, Location, MarkupContent, MarkupKind, MessageType,
        NumberOrString, OneOf, Position, PrepareRenameResponse, Range, ReferenceParams,
        RenameOptions, RenameParams, ServerCapabilities, ServerInfo, SymbolKind,
        TextDocumentPositionParams, TextDocumentSyncCapability, TextDocumentSyncKind, TextEdit,
        Uri, WorkspaceEdit,
    },
};

use crate::{
    document::Document,
    hover,
    symbols::{Kind, Use},
    workspace::Workspace,
};

/// The language server: the open documents, and the client to report to.
#[derive(Debug)]
pub struct Server {
    /// The channel diagnostics are published on.
    client: Client,
    /// The open documents.
    workspace: Arc<RwLock<Workspace>>,
}

impl Server {
    /// Creates a server reporting to the given client.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            workspace: Arc::new(RwLock::new(Workspace::new())),
        }
    }

    /// Parses a document and publishes what the parser said about it.
    ///
    /// The version travels with the diagnostics: it is how a client
    /// knows to discard a report that predates what it now holds.
    async fn refresh(&self, uri: Uri, text: String, version: i32) {
        let diagnostics = {
            let mut workspace = self.workspace.write().await;
            let document = workspace.set(uri.as_str(), text);
            document
                .diagnostics()
                .iter()
                .map(|report| translate(document, &uri, report))
                .collect()
        };
        self.client
            .publish_diagnostics(uri, diagnostics, Some(version))
            .await;
    }
}

impl LanguageServer for Server {
    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                // Full sync: the parser is fast enough that an edit costs
                // one parse, and incremental sync would buy nothing until
                // the parser itself is incremental.
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                document_symbol_provider: Some(OneOf::Left(true)),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                definition_provider: Some(OneOf::Left(true)),
                references_provider: Some(OneOf::Left(true)),
                document_highlight_provider: Some(OneOf::Left(true)),
                document_formatting_provider: Some(OneOf::Left(true)),
                rename_provider: Some(OneOf::Right(RenameOptions {
                    // The editor asks what may be renamed before it asks
                    // for a new name, so a cursor on nothing renameable
                    // says so at once.
                    prepare_provider: Some(true),
                    work_done_progress_options: Default::default(),
                })),
                completion_provider: Some(CompletionOptions {
                    // A marker decides what a word could be, so each one
                    // opens the list.
                    trigger_characters: Some(["@", "?", "!", "$", "#"].map(String::from).to_vec()),
                    ..CompletionOptions::default()
                }),
                inlay_hint_provider: Some(OneOf::Left(true)),
                ..ServerCapabilities::default()
            },
            server_info: Some(ServerInfo {
                name: String::from("nemo-language-server"),
                version: Some(String::from(env!("CARGO_PKG_VERSION"))),
            }),
            // Left unset, which is the protocol's UTF-16, what
            // [Lines](crate::position::Lines) counts.
            offset_encoding: None,
        })
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let document = params.text_document;
        self.refresh(document.uri, document.text, document.version)
            .await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        // Full sync sends the whole text as one range-less change. A
        // client that sends ranges anyway is not one this server
        // advertised itself to, and taking a range's text for the whole
        // document would replace the file with a fragment, so nothing
        // is taken unless it is the whole document.
        let whole = params
            .content_changes
            .into_iter()
            .rfind(|change| change.range.is_none());
        let Some(change) = whole else {
            self.client
                .log_message(
                    MessageType::WARNING,
                    "an edit arrived as a range, but this server syncs whole documents",
                )
                .await;
            return;
        };
        self.refresh(
            params.text_document.uri,
            change.text,
            params.text_document.version,
        )
        .await;
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        let uri = params.text_document.uri;
        self.workspace.write().await.remove(uri.as_str());
        // A closed document's diagnostics are the editor's to forget.
        self.client.publish_diagnostics(uri, Vec::new(), None).await;
    }

    async fn document_symbol(
        &self,
        params: DocumentSymbolParams,
    ) -> Result<Option<DocumentSymbolResponse>> {
        let workspace = self.workspace.read().await;
        let Some(document) = workspace.get(params.text_document.uri.as_str()) else {
            return Ok(None);
        };
        let symbols: Vec<DocumentSymbol> = document
            .symbols()
            .outline()
            .iter()
            .map(|entry| {
                let range = range(document, entry.span);
                #[allow(deprecated)]
                DocumentSymbol {
                    name: entry.name.clone(),
                    detail: entry.detail.clone(),
                    kind: match entry.kind {
                        Kind::Fact => SymbolKind::CONSTANT,
                        Kind::Rule => SymbolKind::FUNCTION,
                        Kind::Directive => SymbolKind::NAMESPACE,
                    },
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range: range,
                    children: None,
                }
            })
            .collect();
        Ok(Some(DocumentSymbolResponse::Nested(symbols)))
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let position = params.text_document_position_params;
        let workspace = self.workspace.read().await;
        let Some(document) = workspace.get(position.text_document.uri.as_str()) else {
            return Ok(None);
        };
        let offset = document.offset(position.position.line, position.position.character);
        let Some((text, span)) = hover::describe(document, offset) else {
            return Ok(None);
        };
        Ok(Some(Hover {
            contents: HoverContents::Markup(MarkupContent {
                kind: MarkupKind::Markdown,
                value: text,
            }),
            range: Some(range(document, span)),
        }))
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let position = params.text_document_position_params;
        let uri = position.text_document.uri;
        let workspace = self.workspace.read().await;
        let Some(document) = workspace.get(uri.as_str()) else {
            return Ok(None);
        };
        let Some(occurrence) = at(document, position.position) else {
            return Ok(None);
        };
        // Where a predicate is derived is what "definition" means for a
        // rule language: the facts and rule heads that give it rows.
        let locations: Vec<Location> = document
            .symbols()
            .definitions(&occurrence.name)
            .map(|definition| Location {
                uri: uri.clone(),
                range: range(document, definition.span),
            })
            .collect();
        if locations.is_empty() {
            return Ok(None);
        }
        Ok(Some(GotoDefinitionResponse::Array(locations)))
    }

    async fn references(&self, params: ReferenceParams) -> Result<Option<Vec<Location>>> {
        let position = params.text_document_position;
        let uri = position.text_document.uri;
        let workspace = self.workspace.read().await;
        let Some(document) = workspace.get(uri.as_str()) else {
            return Ok(None);
        };
        let Some(occurrence) = at(document, position.position) else {
            return Ok(None);
        };
        let include_declaration = params.context.include_declaration;
        let locations: Vec<Location> = document
            .symbols()
            .occurrences(&occurrence.name)
            .filter(|other| include_declaration || other.used != Use::Derives)
            .map(|other| Location {
                uri: uri.clone(),
                range: range(document, other.span),
            })
            .collect();
        Ok(Some(locations))
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let position = params.text_document_position;
        let workspace = self.workspace.read().await;
        let Some(document) = workspace.get(position.text_document.uri.as_str()) else {
            return Ok(None);
        };
        let offset = document.offset(position.position.line, position.position.character);
        let prefix = document.prefix(offset);

        // What a word could be is decided by the marker it opens with:
        // there is nothing to offer after a `$` but a global.
        let items: Vec<CompletionItem> = match prefix.chars().next() {
            Some('@') => DIRECTIVES
                .iter()
                .map(|name| item(format!("@{name}"), CompletionItemKind::KEYWORD, None))
                .collect(),
            Some(marker @ ('?' | '!' | '$')) => document
                .symbols()
                .scope(offset)
                .map(|scope| scope.variables.clone())
                .unwrap_or_default()
                .into_iter()
                .filter(|variable| variable.starts_with(marker))
                .map(|variable| item(variable, CompletionItemKind::VARIABLE, None))
                .collect(),
            Some('#') => AggregateKind::ALL
                .iter()
                .map(|kind| {
                    item(
                        format!("#{}", kind.name()),
                        CompletionItemKind::FUNCTION,
                        Some(String::from("aggregate")),
                    )
                })
                .collect(),
            _ => {
                let mut items: Vec<CompletionItem> = document
                    .symbols()
                    .predicates()
                    .into_iter()
                    .map(|(name, arity)| {
                        item(
                            name.to_owned(),
                            CompletionItemKind::FUNCTION,
                            arity.map(|arity| format!("{arity} column{}", plural(arity))),
                        )
                    })
                    .collect();
                items.extend(Operator::NAMES.iter().map(|name| {
                    item(
                        (*name).to_owned(),
                        CompletionItemKind::FUNCTION,
                        Some(String::from("builtin")),
                    )
                }));
                items
            }
        };
        Ok(Some(CompletionResponse::Array(items)))
    }

    async fn document_highlight(
        &self,
        params: DocumentHighlightParams,
    ) -> Result<Option<Vec<DocumentHighlight>>> {
        let position = params.text_document_position_params;
        let workspace = self.workspace.read().await;
        let Some(document) = workspace.get(position.text_document.uri.as_str()) else {
            return Ok(None);
        };
        let Some(occurrence) = at(document, position.position) else {
            return Ok(None);
        };
        let highlights: Vec<DocumentHighlight> = document
            .symbols()
            .occurrences(&occurrence.name)
            .filter_map(|other| {
                let span = document.name_span(&other.name, other.span)?;
                Some(DocumentHighlight {
                    range: range(document, span),
                    kind: Some(match other.used {
                        Use::Derives => DocumentHighlightKind::WRITE,
                        Use::Reads => DocumentHighlightKind::READ,
                    }),
                })
            })
            .collect();
        Ok(Some(highlights))
    }

    async fn inlay_hint(&self, params: InlayHintParams) -> Result<Option<Vec<InlayHint>>> {
        let workspace = self.workspace.read().await;
        let Some(document) = workspace.get(params.text_document.uri.as_str()) else {
            return Ok(None);
        };
        let start = document.offset(params.range.start.line, params.range.start.character);
        let end = document.offset(params.range.end.line, params.range.end.character);
        let hints = document
            .hints()
            .iter()
            .filter(|hint| start <= hint.offset && hint.offset < end)
            .map(|hint| {
                let (line, character) = document.position(hint.offset);
                InlayHint {
                    position: Position::new(line, character),
                    label: InlayHintLabel::String(hint.label.clone()),
                    kind: Some(InlayHintKind::PARAMETER),
                    text_edits: None,
                    tooltip: None,
                    padding_left: None,
                    padding_right: Some(true),
                    data: None,
                }
            })
            .collect();
        Ok(Some(hints))
    }

    async fn prepare_rename(
        &self,
        params: TextDocumentPositionParams,
    ) -> Result<Option<PrepareRenameResponse>> {
        let workspace = self.workspace.read().await;
        let Some(document) = workspace.get(params.text_document.uri.as_str()) else {
            return Ok(None);
        };
        let Some(occurrence) = at(document, params.position) else {
            return Ok(None);
        };
        let Some(span) = document.name_span(&occurrence.name, occurrence.span) else {
            return Ok(None);
        };
        Ok(Some(PrepareRenameResponse::RangeWithPlaceholder {
            range: range(document, span),
            placeholder: occurrence.name.clone(),
        }))
    }

    async fn rename(&self, params: RenameParams) -> Result<Option<WorkspaceEdit>> {
        let position = params.text_document_position;
        let uri = position.text_document.uri;
        let workspace = self.workspace.read().await;
        let Some(document) = workspace.get(uri.as_str()) else {
            return Ok(None);
        };
        let Some(occurrence) = at(document, position.position) else {
            return Ok(None);
        };

        // Every occurrence of the name is renamed, or none is: renaming
        // some of them would change what the program says.
        let mut edits = Vec::new();
        for other in document.symbols().occurrences(&occurrence.name) {
            let Some(span) = document.name_span(&other.name, other.span) else {
                return Err(jsonrpc::Error::invalid_params(format!(
                    "`{}` cannot be renamed: one of its occurrences could not be \
                     found inside the statement that holds it",
                    occurrence.name
                )));
            };
            edits.push(TextEdit {
                range: range(document, span),
                new_text: params.new_name.clone(),
            });
        }
        Ok(Some(WorkspaceEdit {
            changes: Some(HashMap::from([(uri, edits)])),
            ..WorkspaceEdit::default()
        }))
    }

    async fn formatting(&self, params: DocumentFormattingParams) -> Result<Option<Vec<TextEdit>>> {
        let workspace = self.workspace.read().await;
        let Some(document) = workspace.get(params.text_document.uri.as_str()) else {
            return Ok(None);
        };
        // A document that does not parse has no statements to print, and
        // printing what did parse would delete the rest of it.
        if document
            .diagnostics()
            .iter()
            .any(|report| report.severity() == Severity::Error)
        {
            return Ok(None);
        }
        let edits = crate::format::edits(document.statements(), document.text())
            .into_iter()
            .map(|edit| TextEdit {
                range: range(document, edit.span),
                new_text: edit.text,
            })
            .collect();
        Ok(Some(edits))
    }
}

/// The directives a statement can open with.
const DIRECTIVES: &[&str] = &[
    "base",
    "declare",
    "define",
    "export",
    "extern",
    "import",
    "input",
    "output",
    "parameter",
    "prefix",
    "type",
    "use",
];

/// Returns one completion, named and classified.
fn item(label: String, kind: CompletionItemKind, detail: Option<String>) -> CompletionItem {
    CompletionItem {
        label,
        kind: Some(kind),
        detail,
        ..CompletionItem::default()
    }
}

/// Returns the occurrence a protocol position is in.
fn at(document: &Document, position: Position) -> Option<&crate::symbols::Occurrence> {
    let offset = document.offset(position.line, position.character);
    document.symbols().at(offset)
}

/// Returns a span as the protocol's range.
fn range(document: &Document, span: nemo::model::Span) -> Range {
    let ((start_line, start_character), (end_line, end_character)) = document.range(span);
    Range {
        start: Position::new(start_line, start_character),
        end: Position::new(end_line, end_character),
    }
}

/// Returns a report as the protocol's diagnostic:
/// the first label is its location, and the rest come along as related information.
fn translate(document: &Document, uri: &Uri, report: &Report) -> Diagnostic {
    // A label pointing into another file has no range in this one, and
    // giving it one anyway would put the report somewhere it does not
    // belong. Its message survives, in the text.
    let mut labels = report
        .labels()
        .iter()
        .filter(|label| document.holds(label.span));
    let anchor = labels.next().map(|label| label.span);
    let related: Vec<DiagnosticRelatedInformation> = labels
        .map(|label| DiagnosticRelatedInformation {
            location: Location {
                uri: uri.clone(),
                range: range(document, label.span),
            },
            message: label.message.clone(),
        })
        .collect();

    Diagnostic {
        // A report without a label is about the file, not a place in it,
        // and the protocol has nowhere to put that but the start.
        range: anchor.map_or_else(
            || Range::new(Position::new(0, 0), Position::new(0, 0)),
            |span| range(document, span),
        ),
        severity: Some(match report.severity() {
            Severity::Error => DiagnosticSeverity::ERROR,
            Severity::Warning => DiagnosticSeverity::WARNING,
        }),
        code: Some(NumberOrString::String(report.code().to_string())),
        code_description: None,
        source: Some(String::from("nemo")),
        message: message(report),
        related_information: (!related.is_empty()).then_some(related),
        tags: None,
        data: None,
    }
}

/// Returns a report's message, with what its label says about the place it
/// points at, and the context that follows.
fn message(report: &Report) -> String {
    let mut message = report.message().to_owned();
    if let Some(label) = report.labels().first()
        && !label.message.is_empty()
        && label.message != "here"
    {
        message.push_str(&format!(": {}", label.message));
    }
    for note in report.notes() {
        message.push_str(&format!("\nnote: {note}"));
    }
    for hint in report.hints() {
        message.push_str(&format!("\nhelp: {hint}"));
    }
    message
}

/// Returns the plural `s` of a count, or nothing.
fn plural(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}
