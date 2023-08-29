//! The input and output formats supported by Nemo.

pub mod dsv;
pub mod rdf_triples;

use std::collections::HashSet;

pub use dsv::DSVReader;
use nemo_physical::table_reader::{Resource, TableReader};
pub use rdf_triples::RDFTriplesReader;

use crate::{error::Error, model::PrimitiveType};

use super::resource_providers::ResourceProviders;

const PROGRESS_NOTIFY_INCREMENT: u64 = 1_000_000;

/// A writer for tables that takes iterators over [`PrimitiveType`][PrimitiveType] for each row.
pub trait TableWriter {
    /// Write a record.
    fn write_record(&mut self, record: dyn Iterator<Item = PrimitiveType>) -> Result<(), Error>;
}

/// A supported file format for I/O.
pub trait FileFormat: std::fmt::Debug {
    /// Obtain a [`TableReader`][TableReader] for this format, if supported.
    fn reader(
        &self,
        resource_providers: ResourceProviders,
        resource: Resource,
        logical_types: Vec<PrimitiveType>,
    ) -> Result<Box<dyn TableReader>, Error>;

    /// Obtain a [`TableWriter`] for this format, if supported.
    fn writer(&self) -> Result<Box<dyn TableWriter>, Error>;

    /// Attributes that are valid for this format, but not required.
    fn optional_attributes() -> HashSet<String>;

    /// Attributes that are required for this format.
    fn required_attributes() -> HashSet<String>;

    /// Check whether the given attributes are a valid combination for this format.
    fn attributes_are_valid(attributes: impl IntoIterator<Item = String>) -> bool {
        let given = attributes.into_iter().collect();
        let valid = Self::required_attributes()
            .union(&Self::optional_attributes())
            .cloned()
            .collect();

        Self::required_attributes().is_subset(&given) && given.is_subset(&valid)
    }
}

/// Actions that can be applied to files.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FileAction {
    /// Read a file.
    Read,
    /// Write a file.
    Write,
}

impl std::fmt::Display for FileAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read => write!(f, "reading"),
            Self::Write => write!(f, "writing"),
        }
    }
}

/// Supported file formats.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FileFormats {
    /// Comma-separated values
    CSV,
    /// Delimiter-separated values
    DSV,
    /// Tab-separated values
    TSV,
    /// RDF Triples
    RDF,
    /// RDF N-Triples
    NTriples,
    /// RDF N-Quads
    NQuads,
    /// RDF Turtle
    Turtle,
    /// RDF/XML
    RDFXML,
}

impl FileFormats {
    // fn format(self) -> Box<dyn FileFormat> {
    //     match self {
    //         Self::DSV | Self::CSV | Self::TSV => Box::new(DSVFormat {}),
    //     }
    // }
}

impl std::fmt::Display for FileFormats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormats::DSV => write!(f, "DSV"),
            FileFormats::CSV => write!(f, "CSV"),
            FileFormats::TSV => write!(f, "TSV"),
            FileFormats::RDF => write!(f, "RDF"),
            FileFormats::NTriples => write!(f, "RDF N-Triples"),
            FileFormats::NQuads => write!(f, "RDF N-Quads"),
            FileFormats::Turtle => write!(f, "RDF Turtle"),
            FileFormats::RDFXML => write!(f, "RDF/XML"),
        }
    }
}

#[derive(Debug)]
struct DSVFormat {}

impl FileFormat for DSVFormat {
    fn reader(
        &self,
        resource_providers: ResourceProviders,
        resource: Resource,
        logical_types: Vec<PrimitiveType>,
    ) -> Result<Box<dyn TableReader>, Error> {
        Ok(Box::new(DSVReader::dsv(
            resource_providers,
            todo!(),
            logical_types,
        )))
    }

    fn writer(&self) -> Result<Box<dyn TableWriter>, Error> {
        Err(Error::UnsupportedFileFormat {
            action: FileAction::Write,
            format: FileFormats::DSV,
        })
    }

    fn optional_attributes() -> HashSet<String> {
        [].into()
    }

    fn required_attributes() -> HashSet<String> {
        ["delimiter".into()].into()
    }
}
