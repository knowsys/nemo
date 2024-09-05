//! This module contains functions for building [ChaseImport]s.

use std::collections::HashMap;

use oxiri::Iri;

use crate::{
    chase_model::components::{import::ChaseImport, ChaseComponent},
    io::formats::{
        dsv::{value_format::DsvValueFormats, DsvHandler},
        json::JsonHandler,
        rdf::{value_format::RdfValueFormats, RdfHandler, RdfVariant},
        Direction, ImportExportHandler, ImportExportResource,
    },
    rule_model::components::{
        import_export::{
            attributes::ImportExportAttribute, compression::CompressionFormat,
            file_formats::FileFormat, ImportExportDirective,
        },
        tag::Tag,
        term::Term,
        ProgramComponent,
    },
};

use super::ProgramChaseTranslation;

impl ProgramChaseTranslation {
    /// Build a [ChaseImport] from a given
    /// [ImportDirective][crate::rule_model::components::import_export::ImportDirective].
    pub(crate) fn build_import(
        predicate_arity: &HashMap<Tag, usize>,
        import: &crate::rule_model::components::import_export::ImportDirective,
    ) -> ChaseImport {
        let origin = import.origin().clone();
        let predicate = import.predicate().clone();
        let arity = predicate_arity.get(&predicate).cloned();
        let attributes = import.attributes();

        let handler = match import.file_format() {
            FileFormat::CSV => {
                Self::build_dsv_handler(Direction::Import, Some(b','), arity, &attributes)
            }
            FileFormat::DSV => Self::build_dsv_handler(Direction::Import, None, arity, &attributes),
            FileFormat::TSV => {
                Self::build_dsv_handler(Direction::Import, Some(b'\t'), arity, &attributes)
            }
            FileFormat::JSON => todo!(),
            FileFormat::NTriples => todo!(),
            FileFormat::NQuads => todo!(),
            FileFormat::Turtle => todo!(),
            FileFormat::RDFXML => todo!(),
            FileFormat::TriG => todo!(),
        };

        ChaseImport::new(predicate, handler).set_origin(origin)
    }

    /// Read resource attribute and check compression.
    fn read_resource(
        attributes: &HashMap<ImportExportAttribute, &Term>,
    ) -> (CompressionFormat, ImportExportResource) {
        attributes
            .get(&ImportExportAttribute::Resource)
            .and_then(|term| ImportExportDirective::string_value(term))
            .map(|resource| CompressionFormat::from_resource(&resource))
            .map(|(format, resource)| (format, ImportExportResource::from_string(resource)))
            .expect("invalid program: missing resource in import/export")
    }

    /// Read the [DsvValueFormats] from the attributes.
    fn read_dsv_value_formats(
        attributes: &HashMap<ImportExportAttribute, &Term>,
    ) -> Option<DsvValueFormats> {
        let term = attributes.get(&ImportExportAttribute::Format)?;

        if let Term::Tuple(tuple) = term {
            Some(
                DsvValueFormats::from_tuple(tuple)
                    .expect("invalid program: format attributed malformed in dsv import/export"),
            )
        } else {
            unreachable!("invalid program: format attributed malformed in dsv import/export")
        }
    }

    /// Read the [RdfValueFormats] from the attributes.
    fn read_rdf_value_formats(
        attributes: &HashMap<ImportExportAttribute, &Term>,
    ) -> Option<RdfValueFormats> {
        let term = attributes.get(&ImportExportAttribute::Format)?;

        if let Term::Tuple(tuple) = term {
            Some(
                RdfValueFormats::from_tuple(tuple)
                    .expect("invalid program: format attributed malformed in rdf import/export"),
            )
        } else {
            unreachable!("invalid program: format attributed malformed in rdf import/export")
        }
    }

    /// Read the limit from the attributes.
    fn read_limit(attributes: &HashMap<ImportExportAttribute, &Term>) -> Option<u64> {
        attributes
            .get(&ImportExportAttribute::Limit)
            .and_then(|term| ImportExportDirective::integer_value(term))
            .map(|limit| u64::try_from(limit).unwrap_or_default())
    }

    /// Read the compression format from the attributes.
    fn read_compression(
        attributes: &HashMap<ImportExportAttribute, &Term>,
    ) -> Option<CompressionFormat> {
        if let Some(term) = attributes.get(&ImportExportAttribute::Compression) {
            return Some(
                CompressionFormat::from_name(
                    &ImportExportDirective::string_value(term).expect(
                        "invalid program: compression given in wrong type in import/export",
                    ),
                )
                .expect("invalid program: unknown compression format in import/export"),
            );
        }

        None
    }

    /// Read the iri base path from the attributes.
    fn read_base(attributes: &HashMap<ImportExportAttribute, &Term>) -> Option<Iri<String>> {
        let term = attributes.get(&ImportExportAttribute::Base)?;
        Some(Iri::parse_unchecked(
            ImportExportDirective::plain_value(term)
                .expect("invalid program: base given in the wrong type"),
        ))
    }

    /// Build a [DsvHandler].
    fn build_dsv_handler(
        direction: Direction,
        delimiter: Option<u8>,
        arity: Option<usize>,
        attributes: &HashMap<ImportExportAttribute, &Term>,
    ) -> Box<dyn ImportExportHandler> {
        let (mut compression_format, resource) = Self::read_resource(attributes);

        let value_formats = Self::read_dsv_value_formats(attributes)
            .unwrap_or(DsvValueFormats::default(arity.unwrap_or_default()));

        let limit = Self::read_limit(attributes);

        let delimiter = if let Some(delimiter) = delimiter {
            delimiter
        } else {
            let term = attributes
                .get(&ImportExportAttribute::Delimiter)
                .expect("invalid program: unknown delimiter in dsv import/export");
            let string = ImportExportDirective::string_value(term)
                .expect("invalid program: delimiter given in wrong type in dsv import/export");
            string.as_bytes()[0]
        };

        if let Some(format) = Self::read_compression(attributes) {
            compression_format = format;
        }

        Box::new(DsvHandler::new(
            delimiter,
            resource,
            value_formats,
            limit,
            compression_format,
            direction,
        ))
    }

    /// Build an [RdfHandler].
    fn build_rdf_handler(
        direction: Direction,
        variant: RdfVariant,
        arity: usize,
        attributes: &HashMap<ImportExportAttribute, &Term>,
    ) -> Box<dyn ImportExportHandler> {
        let (mut compression_format, resource) = Self::read_resource(attributes);

        if let Some(format) = Self::read_compression(attributes) {
            compression_format = format;
        }

        let value_formats =
            Self::read_rdf_value_formats(attributes).unwrap_or(RdfValueFormats::default(arity));

        let limit = Self::read_limit(attributes);

        let base = Self::read_base(attributes);

        Box::new(RdfHandler::new(
            resource,
            base,
            variant,
            value_formats,
            limit,
            compression_format,
            direction,
        ))
    }

    /// Build a [JsonHandler].
    fn build_json_handler(
        attributes: &HashMap<ImportExportAttribute, &Term>,
    ) -> Box<dyn ImportExportHandler> {
        let (_, resource) = Self::read_resource(attributes);

        Box::new(JsonHandler::new(resource))
    }
}
