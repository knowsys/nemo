//! The input and output formats supported by Nemo.

pub mod dsv;
pub mod rdf;
pub mod types;

pub use dsv::DSVFormat;
pub use rdf::RDFFormat;

const PROGRESS_NOTIFY_INCREMENT: u64 = 1_000_000;
