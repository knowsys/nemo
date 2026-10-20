//! Program transformations: the pipeline stages a loaded program passes before compilation.
//!
//! The stage here is the SPARQL rewrite of rules over `sparql` imports without a `where` clause.

#[cfg(feature = "sparql")]
pub(super) mod sparql_rewrite;
