//! WebAssembly bindings for the Nemo rule engine.
//!
//! The bindings are thin over the `nemo` crate's embedding API.
//! The engine is synchronous, so run it in a web worker, which keeps the main thread from
//! blocking.
//! A method that needs data from the network suspends,
//! and its async form fetches the data with the global `fetch` and resumes it (see `fetch`).
//!
//! [NemoProgram] parses and inspects without reading any data.
//! [NemoEngine] loads, reasons, and reads results back through one defined conversion table
//! (see `values`).
//! Data enters through registered in-memory resources, imported rows and remote imports,
//! since a browser has no filesystem for `@import` to read,
//! and leaves the same way:
//! `exportTables()` serializes the program's declared exports into bytes.

mod diagnostics;
mod engine;
mod fetch;
#[cfg(target_arch = "wasm32")]
mod language_server;
mod program;
mod values;

pub use engine::{NemoCursor, NemoEngine};
#[cfg(target_arch = "wasm32")]
pub use language_server::{
    LspClientChannel, LspServerRequests, LspServerResponses, create_language_server,
    language_server_handles,
};
pub use program::NemoProgram;

use wasm_bindgen::prelude::*;

/// Returns the crate version.
#[wasm_bindgen(js_name = "nemoVersion")]
pub fn nemo_version() -> String {
    String::from(env!("CARGO_PKG_VERSION"))
}

/// Routes panics to the browser console with their message and
/// location, instead of an opaque `unreachable` trap.
#[wasm_bindgen(js_name = "setPanicHook")]
pub fn set_panic_hook() {
    console_error_panic_hook::set_once();
}
