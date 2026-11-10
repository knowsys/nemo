//! Program transformations: the pipeline stages that a loaded program passes through before
//! compilation.
//!
//! The only stage is the SPARQL rewrite (`sparql_rewrite`).

pub(super) mod sparql_rewrite;
