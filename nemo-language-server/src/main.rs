//! The Nemo language server over standard input and output.

use tower_lsp_server::Server as Connection;

/// Serves one session over standard input and output.
#[tokio::main]
async fn main() {
    let (service, socket) = nemo_language_server::service();
    Connection::new(tokio::io::stdin(), tokio::io::stdout(), socket)
        .serve(service)
        .await;
}
