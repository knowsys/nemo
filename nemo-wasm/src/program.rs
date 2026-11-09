//! Parsed programs, which can be inspected without an engine.

use js_sys::Array;
use wasm_bindgen::prelude::*;

use nemo::{diagnostics::render::SourceTexts, model::FileId};

use crate::diagnostics::{self, Diagnostic};

/// A parsed program, with its text and the diagnostics of parsing.
///
/// Parsing never fails: a statement that does not parse becomes a diagnostic,
/// and the other statements form the program.
/// Inspecting a program needs no engine and fetches nothing.
#[wasm_bindgen]
#[derive(Debug)]
pub struct NemoProgram {
    /// The source text.
    pub(crate) text: String,
    /// The name that diagnostics show for the program.
    pub(crate) name: String,
    /// The parsed program, whose spans are in file 0.
    pub(crate) model: nemo::model::Program,
    /// The diagnostics of parsing.
    diagnostics: Vec<Diagnostic>,
}

#[wasm_bindgen]
impl NemoProgram {
    /// Parses program text, which diagnostics refer to as `name`, or as `<program>` without
    /// a name.
    ///
    /// It never throws: `diagnostics` lists what did not parse,
    /// and `ok` says whether parsing found no errors.
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

    /// Returns the diagnostics of parsing, as plain objects.
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

    /// Returns the predicates that the program's atoms name, as `[name, columns]` pairs,
    /// sorted.
    ///
    /// A predicate written with two numbers of columns is listed twice.
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

    /// Returns the program in Nemo syntax, printed from the parsed statements rather than
    /// copied from the original text.
    #[wasm_bindgen(js_name = "toString")]
    pub fn to_string_js(&self) -> String {
        self.model.to_string()
    }
}
