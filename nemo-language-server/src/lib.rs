pub use language_server::Backend;

pub use tower_lsp::{
    ClientSocket, ExitedError, LspService,
    jsonrpc::{Request, Response},
};
pub use tower_service::Service;

mod language_server;

pub fn create_language_server() -> (LspService<Backend>, ClientSocket) {
    LspService::new(Backend::new)
}

// See https://doc.rust-lang.org/cargo/reference/features.html#mutually-exclusive-features
// #[cfg(all(feature = "js", feature = "tokio"))]
// compile_error!("feature \"js\" and feature \"tokio\" cannot be enabled at the same time");
