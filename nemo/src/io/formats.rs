//! The input and output formats supported by Nemo.

pub(crate) mod import_export;
pub(crate) mod types;

pub mod dsv;
pub mod dsv_reader;
pub mod dsv_value_format;
pub mod dsv_writer;

pub mod rdf;
pub mod rdf_reader;
pub mod rdf_writer;

pub mod json;
pub mod json_reader;

pub(crate) use dsv::DsvHandler;
pub(crate) use rdf::RdfHandler;

const PROGRESS_NOTIFY_INCREMENT: u64 = 10_000_000;
