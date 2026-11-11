//! Format and network adapters for the Nemo rule engine.
//!
//! This crate imports and exports tabular and RDF data:
//! delimiter-separated values, RDF serializations, JSON, Parquet,
//! and the solutions of SPARQL endpoints, compressed or not, local or remote.
//! It reads into the row buffers and the dictionary of [`nemo_physical`],
//! and has no rule-language or engine logic.
//! It expects every configuration it is given, such as a format's options, to be valid already.
//!
//! The crate has three layers.
//! At the top is the vocabulary that every format shares:
//! - Cell [formats](mod@value_format) read the text of cells as values, and write values as cells.
//!   A [column format](value_format::ColumnFormat) also says whether its column admits a
//!   missing value.
//! - A [file format](file_format::FileFormat) names a serialization and its metadata.
//! - A [selection](selection::RowSelection) says which columns a reader delivers,
//!   and its [condition](selection::RowCondition) says which rows it admits.
//! - Every reader produces the same kind of [load](load::Loaded),
//!   and every writer reports the same kind of [outcome](write::Written).
//!
//! In the middle are the format adapters, each with a reader and, except for SPARQL, a writer:
//! [dsv], [rdf], [json], [sparql], and `parquet` behind its feature.
//! A [batch](load::ResourceBatch) of delimiter-separated resources is parsed in parallel,
//! while one thread interns the rows in order, as they arrive.
//! At the bottom are the byte streams they read and write:
//! [compression] wraps a byte stream, and [remote] has the requests for remote resources and
//! the clients that send them.

mod error;

pub use error::AboutResource;

#[cfg(feature = "arrow")]
pub mod arrow;
pub mod codes;
pub mod file_format;
mod formats;
pub mod load;
pub mod selection;
mod stream;
#[cfg(test)]
mod testing;
pub mod value_format;
pub mod write;

#[cfg(feature = "parquet")]
pub use formats::parquet;
pub use formats::sparql;
pub use formats::{dsv, json, rdf};
pub use stream::{compression, remote};
