# nemo-language-server

The Nemo language server. It speaks the Language Server Protocol and provides diagnostics with
their codes, an outline of a file's statements, navigation over predicates, completion, hover,
rename, and formatting.

The `nemo-language-server` binary serves one session over standard input and output (the default
`stdio` feature). In a browser, the `wasm` feature provides the service without a runtime, driven
through the page's own message loop; [nemo-wasm](../nemo-wasm/README.md) builds it in.
The editor extension in [nemo-vscode](../nemo-vscode/README.md) uses either.
