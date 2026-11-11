//! The format adapters, one module per kind of serialization: delimited text, RDF, JSON,
//! Parquet, and the solutions of SPARQL endpoints.
//!
//! Each module has a reader, which decodes resources into [loads](crate::load::Loaded).
//! Every module except the SPARQL one also has a writer, which serializes rows.
//!
//! Every reader passes its well-formed rows to the same intake of a load,
//! so the selection's condition and the counts of dropped rows mean the same in every format.

pub mod dsv;
pub mod json;
#[cfg(feature = "parquet")]
pub mod parquet;
pub mod rdf;
pub mod sparql;
