//! RDF serializations of triples and quads, read and written through `oxrdfio`.
//!
//! A *statement* is one triple, or one quad in the variants with graphs.

mod batch;
mod reader;
mod writer;

pub use batch::{RdfBatch, RdfResource};
pub use reader::{RdfReader, RdfReaderOptions};
pub(crate) use writer::object_term;
pub use writer::{RdfSink, RdfWriter, RdfWriterOptions};

use oxrdfio::RdfFormat;

use crate::file_format::RdfVariant;

/// The IRI that names the default graph in the graph column of quads.
///
/// RDF and SPARQL give the default graph no name,
/// so this IRI stands for it in the rows that are read and written.
pub(crate) const DEFAULT_GRAPH: &str = "tag:nemo:defaultgraph";

impl RdfVariant {
    /// Returns the `oxrdfio` format of the variant.
    fn format(self) -> RdfFormat {
        match self {
            RdfVariant::NTriples => RdfFormat::NTriples,
            RdfVariant::NQuads => RdfFormat::NQuads,
            RdfVariant::Turtle => RdfFormat::Turtle,
            RdfVariant::TriG => RdfFormat::TriG,
            RdfVariant::RdfXml => RdfFormat::RdfXml,
        }
    }
}

#[cfg(test)]
mod tests;
