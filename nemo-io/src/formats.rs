//! The format adapters: one module per serialization.
//!
//! Each module holds a reader decoding resources into [loads](crate::load::Loaded),
//! and a writer serializing rows, over the vocabulary of the crate root.
//! There are modules for delimited text, RDF, JSON, Parquet, and the solutions of SPARQL
//! endpoints.
//!
//! Every reader hands each well-formed row to the same routine of the load (`Loaded::take`),
//! so the declared types, the condition, and the counts of dropped rows mean the same in every
//! format.

pub mod dsv;
pub mod json;
#[cfg(feature = "parquet")]
pub mod parquet;
pub mod rdf;
#[cfg(feature = "sparql")]
pub mod sparql;
