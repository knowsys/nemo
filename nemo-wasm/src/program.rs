//! Programs: parsed text, inspectable without an engine.

use js_sys::Array;
use wasm_bindgen::prelude::*;

use nemo::{diagnostics::render::SourceTexts, model::FileId};

use crate::diagnostics::{self, Diagnostic};

/// A parsed program: the text it came from, and what parsing had to say.
///
/// Parsing is total and reads no data, so inspection needs no engine and fetches nothing.
#[wasm_bindgen]
#[derive(Debug)]
pub struct NemoProgram {
    /// The source text.
    pub(crate) text: String,
    /// The display name diagnostics show for it.
    pub(crate) name: String,
    /// The parsed model (spans numbered from file 0).
    pub(crate) model: nemo::model::Program,
    /// The parse diagnostics, resolved against the text.
    diagnostics: Vec<Diagnostic>,
}

#[wasm_bindgen]
impl NemoProgram {
    /// Parses program text.
    ///
    /// It never throws, and `diagnostics` reports what did not parse.
    #[wasm_bindgen(js_name = "parse")]
    pub fn parse(text: String, name: Option<String>) -> NemoProgram {
        let name = name.unwrap_or_else(|| String::from("<program>"));
        let (model, diagnostics) = nemo::model::Program::parse(&text, FileId::new(0));
        let sources = SourceTexts::detached(name.clone(), text.clone());
        let diagnostics = diagnostics
            .iter()
            .map(|diagnostic| diagnostics::resolve(diagnostic, &sources))
            .collect();
        NemoProgram {
            text,
            name,
            model,
            diagnostics,
        }
    }

    /// Returns the parse diagnostics, as plain objects.
    #[wasm_bindgen(getter)]
    pub fn diagnostics(&self) -> JsValue {
        diagnostics::to_js(&self.diagnostics)
    }

    /// Returns whether the whole text parsed without errors.
    #[wasm_bindgen(getter)]
    pub fn ok(&self) -> bool {
        self.diagnostics
            .iter()
            .all(|diagnostic| diagnostic.severity != "error")
    }

    /// Returns the predicates the program's atoms name, as `[name, columns]`
    /// pairs, sorted.
    #[wasm_bindgen(getter)]
    pub fn predicates(&self) -> Array {
        self.model
            .predicates()
            .into_iter()
            .map(|(name, arity)| {
                JsValue::from(Array::of2(
                    &JsValue::from_str(&name),
                    &JsValue::from_f64(arity as f64),
                ))
            })
            .collect()
    }

    /// Returns the number of statements that parsed.
    #[wasm_bindgen(getter, js_name = "statementCount")]
    pub fn statement_count(&self) -> usize {
        self.model.statements.len()
    }

    /// Returns the program in Nemo syntax (as parsed, not the original text).
    #[wasm_bindgen(js_name = "toString")]
    pub fn to_string_js(&self) -> String {
        self.model.to_string()
    }
}
