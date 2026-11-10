//! Diagnostics as plain JavaScript data, and the errors that the bindings throw.

use serde::Serialize;
use wasm_bindgen::prelude::*;

use nemo::diagnostics::render::{Renderer, SourceTexts, notes};

/// A label of a [Diagnostic]: its message, and the place it points at if that is known.
#[derive(Debug, Serialize)]
pub(crate) struct Label {
    /// The label's message.
    pub message: String,
    /// The name of the source that the label points into, if it is known.
    pub source: Option<String>,
    /// The line, counted from 1, if the place is known.
    pub line: Option<u32>,
    /// The column, in characters and counted from 1, if the place is known.
    pub column: Option<u32>,
}

/// A diagnostic as plain data, the form in which the bindings give diagnostics to
/// JavaScript.
#[derive(Debug, Serialize)]
pub(crate) struct Diagnostic {
    /// The stable code, like `"N0204"`.
    pub code: String,
    /// The severity, `"error"` or `"warning"`.
    pub severity: String,
    /// The main message.
    pub message: String,
    /// The labels, with the places they point at.
    pub labels: Vec<Label>,
    /// Additional notes.
    pub notes: Vec<String>,
    /// Hints on how to fix the problem.
    pub hints: Vec<String>,
    /// The full rendering, with source snippets and without colors.
    pub rendered: String,
}

/// Returns a diagnostic as plain data, with its rendering against `sources`.
///
/// The place of a label is the location that the label has.
pub(crate) fn resolve(
    diagnostic: &nemo::diagnostics::Diagnostic,
    sources: &SourceTexts,
) -> Diagnostic {
    Diagnostic {
        code: diagnostic.code().to_string(),
        severity: diagnostic.severity().to_string(),
        message: diagnostic.message().to_owned(),
        labels: diagnostic
            .labels()
            .iter()
            .map(|label| {
                let location = label.location.as_ref();
                Label {
                    message: label.message.clone(),
                    source: location
                        .and_then(|location| location.source.as_deref())
                        .map(str::to_owned),
                    line: location.map(|location| location.line),
                    column: location.map(|location| location.column),
                }
            })
            .collect(),
        notes: notes(diagnostic),
        hints: diagnostic.hints().to_vec(),
        rendered: Renderer::plain(sources).show(diagnostic),
    }
}

/// Returns a diagnostic whose hints name the methods of the JavaScript API.
///
/// The hints of [NO_CLIENT](nemo::codes::NO_CLIENT),
/// [METHOD_SUSPENDED](nemo::codes::METHOD_SUSPENDED) and
/// [PARAMETER_WITHOUT_VALUE](nemo::codes::PARAMETER_WITHOUT_VALUE) name engine methods that
/// the bindings lack or name differently, so they are replaced.
fn with_browser_hints(diagnostic: nemo::diagnostics::Diagnostic) -> nemo::diagnostics::Diagnostic {
    let code = diagnostic.code();
    if code == nemo::codes::NO_CLIENT {
        diagnostic.without_hints().with_hint(
            "call the method's async form, such as `loadAsync`, which fetches the data, \
             or its `start` form, such as `startLoad`, and pass the responses to `resume`",
        )
    } else if code == nemo::codes::METHOD_SUSPENDED {
        diagnostic
            .without_hints()
            .with_hint("pass the responses to `resume`, or call `discardPending`")
    } else if code == nemo::codes::PARAMETER_WITHOUT_VALUE {
        diagnostic
            .without_hints()
            .with_hint("give it a value with `setParameter`")
    } else {
        diagnostic
    }
}

/// Returns the JavaScript form of a list of diagnostics.
pub(crate) fn to_js(diagnostics: &[Diagnostic]) -> JsValue {
    serde_wasm_bindgen::to_value(diagnostics).expect("plain data serializes")
}

/// Returns the `Error` to throw for the diagnostics of a failed call.
///
/// The error's message joins the renderings of the diagnostics,
/// and its `diagnostics` property lists them as plain data.
/// Hints that name engine methods the bindings lack or name differently are replaced,
/// as [with_browser_hints] does.
pub(crate) fn throw(
    diagnostics: Vec<nemo::diagnostics::Diagnostic>,
    sources: &SourceTexts,
) -> JsValue {
    let resolved: Vec<Diagnostic> = diagnostics
        .into_iter()
        .map(|diagnostic| resolve(&with_browser_hints(diagnostic), sources))
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
