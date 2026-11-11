//! Parquet, the standard columnar file format, read and written through Arrow.
//!
//! Parquet has its own compression, so no stream compression applies.

mod reader;
mod writer;

pub use reader::ParquetReader;
pub use writer::ParquetWriter;

#[cfg(test)]
mod tests;
