//! The Nemo language server.
//!
//! It implements the Language Server Protocol:
//! parse and check diagnostics with their codes, an outline of a file's statements,
//! navigation over predicates (what derives one, what reads it, and what it looks like),
//! completion, hover, rename, and formatting.
//!
//! The protocol shell is [Server].
//! Everything it responds with comes from the `workspace`, `document`, `symbols` and `hover`
//! modules,
//! which know nothing about the protocol and can be driven from a test.
//! The binary serves one session over standard input and output.
//! In a browser, where there is no process, the [service] is driven
//! through the page's own message loop.

mod document;
mod format;
mod hover;
mod position;
mod server;
mod symbols;
mod workspace;

pub use tower_lsp_server::{
    ClientSocket, ExitedError, LspService,
    jsonrpc::{Request, Response},
};

pub use crate::server::Server;

/// Returns the language service and the socket carrying what the server sends on its own
/// (diagnostics, logs), ready for any transport:
/// the binary frames it over standard streams, a browser over messages.
pub fn service() -> (LspService<Server>, ClientSocket) {
    LspService::new(Server::new)
}
