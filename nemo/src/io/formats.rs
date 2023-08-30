//! The input and output formats supported by Nemo.

pub mod dsv;
pub mod rdf_triples;
pub mod types;

pub use dsv::DSVReader;
pub use rdf_triples::RDFTriplesReader;

const PROGRESS_NOTIFY_INCREMENT: u64 = 1_000_000;
