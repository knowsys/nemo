//! Diagnostics as plain JavaScript data, and thrown errors.

use serde::Serialize;
use wasm_bindgen::prelude::*;

use nemo::diagnostics::render::{ResolvedDiagnostic, SourceTexts};

/// One resolved label of a diagnostic (see [Diagnostic]).
#[derive(Debug, Serialize)]
pub(crate) struct Label {
    /// The label's message.
    pub message: String,
    /// The name of the source the span points into, if known.
    pub source: Option<String>,
    /// The 1-based line, if the span could be located.
    pub line: Option<u32>,
    /// The 1-based column (in characters), if the span could be located.
    pub column: Option<u32>,
}

/// One diagnostic as plain data,
/// which `check` returns and thrown errors carry on their `diagnostics` property.
#[derive(Debug, Serialize)]
pub(crate) struct Diagnostic {
    /// The stable code, like `"N0204"`.
    pub code: String,
    /// `"error"` or `"warning"`.
    pub severity: String,
    /// The headline message.
    pub message: String,
    /// The labels, resolved against the run's sources.
    pub labels: Vec<Label>,
    /// Additional notes.
    pub notes: Vec<String>,
    /// Hints toward a fix.
    pub hints: Vec<String>,
    /// The full plain rendering, snippets included.
    pub rendered: String,
}

/// Resolves a diagnostic against the sources of its run.
pub(crate) fn resolve(
    diagnostic: &nemo::diagnostics::Diagnostic,
    sources: &SourceTexts,
) -> Diagnostic {
    let resolved = ResolvedDiagnostic::new(diagnostic, sources);
    Diagnostic {
        code: resolved.code,
        severity: resolved.severity.to_string(),
        message: resolved.message,
        labels: resolved
            .labels
            .into_iter()
            .map(|label| Label {
                message: label.message,
                source: label.source,
                line: label.line,
                column: label.column,
            })
            .collect(),
        notes: resolved.notes,
        hints: resolved.hints,
        rendered: resolved.rendered,
    }
}

/// Returns the JavaScript form of a list of diagnostics.
pub(crate) fn to_js(diagnostics: &[Diagnostic]) -> JsValue {
    serde_wasm_bindgen::to_value(diagnostics).expect("plain data serializes")
}

/// Returns a thrown `Error` whose message is the rendering and whose
/// `diagnostics` property carries the structured entries.
pub(crate) fn throw(
    diagnostics: Vec<nemo::diagnostics::Diagnostic>,
    sources: &SourceTexts,
) -> JsValue {
    let resolved: Vec<Diagnostic> = diagnostics
        .iter()
        .map(|diagnostic| resolve(diagnostic, sources))
        .collect();
    let message = resolved
        .iter()
        .map(|diagnostic| diagnostic.rendered.trim_end())
        .collect::<Vec<_>>()
        .join("\n");
    let error = js_sys::Error::new(&message);
    let _ = js_sys::Reflect::set(&error, &JsValue::from_str("diagnostics"), &to_js(&resolved));
    error.into()
}
