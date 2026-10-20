//! Format and network adapters for the Nemo rule engine.
//!
//! This crate imports and exports tabular and RDF data:
//! delimiter-separated values, RDF serializations, JSON, Parquet,
//! and the solutions of SPARQL endpoints, compressed or not, local or remote.
//! It is a client of the [`nemo_physical`] load API and holds no rule-language or engine logic.
//! The configurations it reads and writes with are validated before they reach it.
//!
//! The crate has three layers.
//! At the top is the vocabulary every format shares:
//! - Cell [formats](mod@format) parse fields into values.
//! - A [file format](file_format::FileFormat) carries the metadata of a serialization.
//! - A [selection](selection::RowSelection) says which columns and rows a reader delivers,
//!   and its [condition](row_filter) which rows it admits.
//! - Every reader reports the same [load](load::Loaded),
//!   and every writer the same [outcome](written::Written).
//! - A [batch](load::ResourceBatch) parses its resources in parallel,
//!   while one thread interns what they deliver, in order, as it arrives.
//!
//! In the middle are the format adapters, one reader and one writer per serialization:
//! [dsv], [rdf], [json], and `parquet` and `sparql` behind their features.
//! At the bottom are the streams they read and write:
//! [compression] around a byte stream, and [remote] resources fetched into one.

mod error;
mod term;

pub use error::AboutResource;

#[cfg(feature = "arrow")]
pub mod arrow;
pub mod codes;
pub mod file_format;
pub mod format;
mod formats;
pub mod load;
pub mod row_filter;
pub mod selection;
mod stream;
#[cfg(test)]
mod testing;
pub mod written;

#[cfg(feature = "parquet")]
pub use formats::parquet;
#[cfg(feature = "sparql")]
pub use formats::sparql;
pub use formats::{dsv, json, rdf};
pub use stream::{compression, remote};
