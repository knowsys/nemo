//! This module defines [FileFormat]s that are supported.
#![allow(missing_docs)]

use std::{collections::HashMap, fmt::Display};

use enum_assoc::Assoc;

use crate::rule_model::{
    components::import_export::attributes::ImportExportAttribute,
    syntax::import_export::file_formats,
};

/// Marks whether a an attribute is required or optional
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum AttributeRequirement {
    /// Attribute is required
    Required,
    /// Attribute is optional and results in the provided default
    Optional,
}

/// Supported file formats
#[derive(Assoc, Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[func(pub fn name(&self) -> &'static str)]
#[func(pub fn extension(&self) -> &'static str)]
#[func(pub fn attributes(&self) -> HashMap<ImportExportAttribute, AttributeRequirement>)]
pub enum FileFormat {
    /// Comma-separated values
    #[assoc(name = file_formats::FILE_FORMAT_CSV)]
    #[assoc(extension = file_formats::EXTENSION_CSV)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Required)
    ]))]
    CSV,
    /// Delimiter-separated values
    #[assoc(name = file_formats::FILE_FORMAT_DSV)]
    #[assoc(extension = file_formats::EXTENSION_DSV)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Required)
    ]))]
    DSV,
    /// Tab-separated values
    #[assoc(name = file_formats::FILE_FORMAT_TSV)]
    #[assoc(extension = file_formats::EXTENSION_TSV)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Required)
    ]))]
    TSV,
    /// JSON objects
    #[assoc(name = file_formats::FILE_FORMAT_JSON)]
    #[assoc(extension = file_formats::EXTENSION_JSON)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Required)
    ]))]
    JSON,
    /// RDF 1.1 N-Triples
    #[assoc(name = file_formats::FILE_FORMAT_RDF_NTRIPLES)]
    #[assoc(extension = file_formats::EXTENSION_RDF_NTRIPLES)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Required)
    ]))]
    NTriples,
    /// RDF 1.1 N-Quads
    #[assoc(name = file_formats::FILE_FORMAT_RDF_NQUADS)]
    #[assoc(extension = file_formats::EXTENSION_RDF_NQUADS)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Required)
    ]))]
    NQuads,
    /// RDF 1.1 Turtle
    #[assoc(name = file_formats::FILE_FORMAT_RDF_TURTLE)]
    #[assoc(extension = file_formats::EXTENSION_RDF_TURTLE)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Required)
    ]))]
    Turtle,
    /// RDF 1.1 RDF/XML
    #[assoc(name = file_formats::FILE_FORMAT_RDF_XML)]
    #[assoc(extension = file_formats::EXTENSION_RDF_XML)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Required)
    ]))]
    RDFXML,
    /// RDF 1.1 TriG
    #[assoc(name = file_formats::FILE_FORMAT_RDF_TRIG)]
    #[assoc(extension = file_formats::EXTENSION_RDF_TRIG)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Required)
    ]))]
    TriG,
}

impl Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
