//! This module defines [FileFormat]s that are supported.
#![allow(missing_docs)]

use std::{collections::HashMap, fmt::Display};

use enum_assoc::Assoc;
use strum_macros::EnumIter;

use crate::{
    rule_model::components::import_export::attributes::ImportExportAttribute,
    syntax::import_export::file_format,
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
#[derive(Assoc, EnumIter, Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[func(pub fn name(&self) -> &'static str)]
#[func(pub fn from_name(name: &str) -> Option<Self>)]
#[func(pub fn extension(&self) -> &'static str)]
#[func(pub fn media_type(&self) -> &'static str)]
#[func(pub fn attributes(&self) -> HashMap<ImportExportAttribute, AttributeRequirement>)]
#[func(pub fn arity(&self) -> Option<usize>)]
pub enum FileFormat {
    /// Comma-separated values
    #[assoc(name = file_format::CSV)]
    #[assoc(from_name = file_format::CSV)]
    #[assoc(extension = file_format::EXTENSION_CSV)]
    #[assoc(media_type = file_format::MEDIA_TYPE_CSV)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Optional),
        (ImportExportAttribute::Format, AttributeRequirement::Optional),
        (ImportExportAttribute::Limit, AttributeRequirement::Optional),
        (ImportExportAttribute::Compression, AttributeRequirement::Optional),
        (ImportExportAttribute::IgnoreHeaders, AttributeRequirement::Optional),
    ]))]
    CSV,
    /// Delimiter-separated values
    #[assoc(name = file_format::DSV)]
    #[assoc(from_name = file_format::DSV)]
    #[assoc(extension = file_format::EXTENSION_DSV)]
    #[assoc(media_type = file_format::MEDIA_TYPE_DSV)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Optional),
        (ImportExportAttribute::Delimiter, AttributeRequirement::Required),
        (ImportExportAttribute::Format, AttributeRequirement::Optional),
        (ImportExportAttribute::Limit, AttributeRequirement::Optional),
        (ImportExportAttribute::Compression, AttributeRequirement::Optional),
        (ImportExportAttribute::IgnoreHeaders, AttributeRequirement::Optional),
    ]))]
    DSV,
    /// Tab-separated values
    #[assoc(name = file_format::TSV)]
    #[assoc(from_name = file_format::TSV)]
    #[assoc(extension = file_format::EXTENSION_TSV)]
    #[assoc(media_type = file_format::MEDIA_TYPE_TSV)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Optional),
        (ImportExportAttribute::Format, AttributeRequirement::Optional),
        (ImportExportAttribute::Limit, AttributeRequirement::Optional),
        (ImportExportAttribute::Compression, AttributeRequirement::Optional),
        (ImportExportAttribute::IgnoreHeaders, AttributeRequirement::Optional),
    ]))]
    TSV,
    /// JSON objects
    #[assoc(name = file_format::JSON)]
    #[assoc(from_name = file_format::JSON)]
    #[assoc(extension = file_format::EXTENSION_JSON)]
    #[assoc(media_type = file_format::MEDIA_TYPE_JSON)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Optional)
    ]))]
    #[assoc(arity = 3)] // TODO: In the future we probably want arbitrary arity here
    JSON,
    /// RDF 1.1 N-Triples
    #[assoc(name = file_format::RDF_NTRIPLES)]
    #[assoc(from_name = file_format::RDF_NTRIPLES)]
    #[assoc(extension = file_format::EXTENSION_RDF_NTRIPLES)]
    #[assoc(media_type = file_format::MEDIA_TYPE_RDF_NTRIPLES)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Optional),
        (ImportExportAttribute::Base, AttributeRequirement::Optional),
        (ImportExportAttribute::Format, AttributeRequirement::Optional),
        (ImportExportAttribute::Limit, AttributeRequirement::Optional),
        (ImportExportAttribute::Compression, AttributeRequirement::Optional),
    ]))]
    #[assoc(arity = 3)]
    NTriples,
    /// RDF 1.1 N-Quads
    #[assoc(name = file_format::RDF_NQUADS)]
    #[assoc(from_name = file_format::RDF_NQUADS)]
    #[assoc(extension = file_format::EXTENSION_RDF_NQUADS)]
    #[assoc(media_type = file_format::MEDIA_TYPE_RDF_NQUADS)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Optional),
        (ImportExportAttribute::Base, AttributeRequirement::Optional),
        (ImportExportAttribute::Format, AttributeRequirement::Optional),
        (ImportExportAttribute::Limit, AttributeRequirement::Optional),
        (ImportExportAttribute::Compression, AttributeRequirement::Optional),
    ]))]
    #[assoc(arity = 4)]
    NQuads,
    /// RDF 1.1 Turtle
    #[assoc(name = file_format::RDF_TURTLE)]
    #[assoc(from_name = file_format::RDF_TURTLE)]
    #[assoc(extension = file_format::EXTENSION_RDF_TURTLE)]
    #[assoc(media_type = file_format::MEDIA_TYPE_RDF_TURTLE)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Optional),
        (ImportExportAttribute::Base, AttributeRequirement::Optional),
        (ImportExportAttribute::Format, AttributeRequirement::Optional),
        (ImportExportAttribute::Limit, AttributeRequirement::Optional),
        (ImportExportAttribute::Compression, AttributeRequirement::Optional),
    ]))]
    #[assoc(arity = 3)]
    Turtle,
    /// RDF 1.1 RDF/XML
    #[assoc(name = file_format::RDF_XML)]
    #[assoc(from_name = file_format::RDF_XML)]
    #[assoc(extension = file_format::EXTENSION_RDF_XML)]
    #[assoc(media_type = file_format::MEDIA_TYPE_RDF_XML)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Optional),
        (ImportExportAttribute::Base, AttributeRequirement::Optional),
        (ImportExportAttribute::Format, AttributeRequirement::Optional),
        (ImportExportAttribute::Limit, AttributeRequirement::Optional),
        (ImportExportAttribute::Compression, AttributeRequirement::Optional),
    ]))]
    #[assoc(arity = 3)]
    RDFXML,
    /// RDF 1.1 TriG
    #[assoc(name = file_format::RDF_TRIG)]
    #[assoc(from_name = file_format::RDF_TRIG)]
    #[assoc(extension = file_format::EXTENSION_RDF_TRIG)]
    #[assoc(media_type = file_format::MEDIA_TYPE_RDF_TRIG)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Resource, AttributeRequirement::Optional),
        (ImportExportAttribute::Base, AttributeRequirement::Optional),
        (ImportExportAttribute::Format, AttributeRequirement::Optional),
        (ImportExportAttribute::Limit, AttributeRequirement::Optional),
        (ImportExportAttribute::Compression, AttributeRequirement::Optional),
    ]))]
    #[assoc(arity = 4)]
    TriG,
    /// SPARQL 1.1 
    #[assoc(name = file_format::SPARQL)]
    #[assoc(from_name = file_format::SPARQL)]
    #[assoc(extension = file_format::EXTENSION_TSV)]
    #[assoc(media_type = file_format::MEDIA_TYPE_SPARQL)]
    #[assoc(attributes = HashMap::from([
        (ImportExportAttribute::Endpoint, AttributeRequirement::Required),
        (ImportExportAttribute::Base, AttributeRequirement::Optional),
        (ImportExportAttribute::Format, AttributeRequirement::Optional),
        (ImportExportAttribute::Limit, AttributeRequirement::Optional),
        (ImportExportAttribute::Query, AttributeRequirement::Required),
    ]))]
    Sparql,
}

/// List of RDF [FileFormat]s
pub const FILE_FORMATS_RDF: &[FileFormat] = &[
    FileFormat::NQuads,
    FileFormat::NTriples,
    FileFormat::RDFXML,
    FileFormat::TriG,
    FileFormat::Turtle,
];

impl Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
