//! The Nemo language server.
//!
//! It implements these features of the Language Server Protocol:
//! - the diagnostics of parsing and checking, with their codes;
//! - an outline of a file's statements;
//! - navigation over predicates, to the statements that derive one and those that read it;
//! - hover, highlights, completion, inlay hints, rename and formatting.
//!
//! [Server] implements the protocol.
//! Its responses come from the `workspace`, `document`, `symbols`, `hover` and `format`
//! modules, which know nothing about the protocol and can be tested without it.
//! The binary serves one session over standard input and output.
//! In a browser, which has no process with standard streams, the [service] is driven
//! through the page's messages.

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

/// Returns the language service, and the socket for what the server sends on its own
/// initiative, such as diagnostics and log messages.
///
/// Both can be connected to any transport, such as standard input and output,
/// or a browser's messages.
pub fn service() -> (LspService<Server>, ClientSocket) {
    LspService::new(Server::new)
}
