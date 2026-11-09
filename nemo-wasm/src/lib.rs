//! WebAssembly bindings for the Nemo rule engine.
//!
//! The bindings are a thin layer over the API of the `nemo` crate.
//! The engine is synchronous, so it should run in a web worker, where it does not block the
//! main thread.
//! A method that needs data from the network is suspended for it,
//! and the method's async form fetches the data with the global `fetch` and resumes it,
//! as the `fetch` module describes.
//!
//! [NemoProgram] parses and inspects a program without reading any data.
//! [NemoEngine] loads programs, reasons, and reads the results,
//! converting values as the `values` module describes.
//! A browser has no file system for `@import` to read,
//! so data comes in through resources registered in memory, imported rows and remote imports.
//! Results go out as values, or as bytes: `exportTables()` serializes the program's exports.

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

/// Returns the version of the Nemo bindings.
#[wasm_bindgen(js_name = "nemoVersion")]
pub fn nemo_version() -> String {
    String::from(env!("CARGO_PKG_VERSION"))
}

/// Makes every later panic log its message and location to the browser console.
///
/// Without it, a panic shows only as an `unreachable` trap.
#[wasm_bindgen(js_name = "setPanicHook")]
pub fn set_panic_hook() {
    console_error_panic_hook::set_once();
}
