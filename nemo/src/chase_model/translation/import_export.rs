//! This module contains functions for building [ChaseImport]s.

use std::collections::HashMap;

use oxiri::Iri;

use crate::{
    chase_model::components::{export::ChaseExport, import::ChaseImport, ChaseComponent},
    io::formats::{
        dsv::{value_format::DsvValueFormats, DsvHandler},
        sparql::SparqlHandler,
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
        &self,
        import: &crate::rule_model::components::import_export::ImportDirective,
    ) -> ChaseImport {
        let origin = *import.origin();
        let predicate = import.predicate().clone();
        let attributes = import.attributes();
        let file_format = import.file_format();

        let handler = self.import_export_handler(
            Direction::Import,
            predicate.clone(),
            attributes,
            file_format,
        );

        ChaseImport::new(predicate, handler).set_origin(origin)
    }

    /// Build a [ChaseExport] from a given
    /// [ExportDirective][crate::rule_model::components::import_export::ExportDirective].
    pub(crate) fn build_export(
        &self,
        export: &crate::rule_model::components::import_export::ExportDirective,
    ) -> ChaseExport {
        let origin = *export.origin();
        let predicate = export.predicate().clone();
        let attributes = export.attributes();
        let file_format = export.file_format();

        let handler = self.import_export_handler(
            Direction::Import,
            predicate.clone(),
            attributes,
            file_format,
        );

        ChaseExport::new(predicate, handler).set_origin(origin)
    }

    /// Create a [ImportExportHandler].
    /// [ImportDirective][crate::rule_model::components::import_export::ImportDirective].
    fn import_export_handler(
        &self,
        direction: Direction,
        predicate: Tag,
        mut attributes: HashMap<ImportExportAttribute, Term>,
        file_format: FileFormat,
    ) -> Box<dyn ImportExportHandler> {
        let arity = self.predicate_arity.get(&predicate).cloned();

        attributes
            .entry(ImportExportAttribute::Resource)
            .or_insert_with(|| {
                let default_file_name = format!("{}.{}", predicate, file_format.extension());
                Term::from(default_file_name)
            });

        match file_format {
            FileFormat::CSV => {
                Self::build_dsv_handler(Direction::Import, Some(b','), arity, &attributes)
            }
            FileFormat::DSV => Self::build_dsv_handler(Direction::Import, None, arity, &attributes),
            FileFormat::TSV => {
                Self::build_dsv_handler(Direction::Import, Some(b'\t'), arity, &attributes)
            }
            FileFormat::JSON => Self::build_json_handler(&attributes),
            FileFormat::Sparql => Self::build_sparql_handler(Direction::Import, arity, &attributes),
            FileFormat::NTriples => {
                Self::build_rdf_handler(direction, RdfVariant::NTriples, arity, &attributes)
            }
            FileFormat::NQuads => {
                Self::build_rdf_handler(direction, RdfVariant::NQuads, arity, &attributes)
            }
            FileFormat::Turtle => {
                Self::build_rdf_handler(direction, RdfVariant::Turtle, arity, &attributes)
            }
            FileFormat::RDFXML => {
                Self::build_rdf_handler(direction, RdfVariant::RDFXML, arity, &attributes)
            }
            FileFormat::TriG => {
                Self::build_rdf_handler(direction, RdfVariant::TriG, arity, &attributes)
            }

        }
    }

    /// Read resource attribute and check compression.
    fn read_resource(
        attributes: &HashMap<ImportExportAttribute, Term>,
    ) -> (CompressionFormat, ImportExportResource) {
        attributes
            .get(&ImportExportAttribute::Resource)
            .and_then(ImportExportDirective::string_value)
            .map(|path| (CompressionFormat::from_resource(&path).0, path))
            .map(|(format, path)| (format, ImportExportResource::from_string(path)))
            .expect("invalid program: missing resource in import/export")
    }

    /// Read SPARQL endpoint
    fn read_endpoint(
        attributes: &HashMap<ImportExportAttribute, Term>,
    ) -> Iri<String> {
        let term = attributes
            .get(&ImportExportAttribute::Endpoint)
            .expect("Missing endpoint for sparql query");
        Iri::parse_unchecked(
            ImportExportDirective::plain_value(term)
                .expect("invalid program: base given in the wrong type")
            )
    }

    /// Read SPARQL query
    fn read_query(
        attributes: &HashMap<ImportExportAttribute, Term>,
    ) -> String {
        attributes
            .get(&ImportExportAttribute::Query)
            .and_then(&ImportExportDirective::string_value)
            .expect("No query provided for sparql keyword")
    }

    /// Read the [DsvValueFormats] from the attributes.
    fn read_dsv_value_formats(
        attributes: &HashMap<ImportExportAttribute, Term>,
    ) -> Option<DsvValueFormats> {
        let term = attributes.get(&ImportExportAttribute::Format)?;

        match term {
            Term::Tuple(tuple) => Some(
                DsvValueFormats::from_tuple(tuple)
                    .expect("invalid program: format attributed malformed in dsv import/export"),
            ),
            _ => panic!("invalid program: format attributed malformed in dsv import/export"),
        }
    }

    /// Read the [RdfValueFormats] from the attributes.
    fn read_rdf_value_formats(
        attributes: &HashMap<ImportExportAttribute, Term>,
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
    fn read_limit(attributes: &HashMap<ImportExportAttribute, Term>) -> Option<u64> {
        attributes
            .get(&ImportExportAttribute::Limit)
            .and_then(ImportExportDirective::integer_value)
            .map(|limit| u64::try_from(limit).unwrap_or_default())
    }

    /// Read the compression format from the attributes.
    fn read_compression(
        attributes: &HashMap<ImportExportAttribute, Term>,
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
    fn read_base(attributes: &HashMap<ImportExportAttribute, Term>) -> Option<Iri<String>> {
        let term = attributes.get(&ImportExportAttribute::Base)?;
        Some(Iri::parse_unchecked(
            ImportExportDirective::plain_value(term)
                .expect("invalid program: base given in the wrong type"),
        ))
    }

    /// Read the ignore_headers option from the attributes.
    fn read_ignore_headers(attributes: &HashMap<ImportExportAttribute, Term>) -> Option<bool> {
        attributes
            .get(&ImportExportAttribute::IgnoreHeaders)
            .and_then(ImportExportDirective::boolean_value)
    }
    

    /// Build a [DsvHandler].
    fn build_dsv_handler(
        direction: Direction,
        delimiter: Option<u8>,
        arity: Option<usize>,
        attributes: &HashMap<ImportExportAttribute, Term>,
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

        let ignore_headers = Self::read_ignore_headers(attributes).unwrap_or_default();

        Box::new(DsvHandler::new(
            delimiter,
            resource,
            value_formats,
            limit,
            compression_format,
            ignore_headers,
            direction,
        ))
    }

    /// Build an [RdfHandler].
    fn build_rdf_handler(
        direction: Direction,
        variant: RdfVariant,
        arity: Option<usize>,
        attributes: &HashMap<ImportExportAttribute, Term>,
    ) -> Box<dyn ImportExportHandler> {
        let arity = arity.expect("rdf types have known arity");
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
        attributes: &HashMap<ImportExportAttribute, Term>,
    ) -> Box<dyn ImportExportHandler> {
        let (_, resource) = Self::read_resource(attributes);

        Box::new(JsonHandler::new(resource))
    }

     /// Build a [SparqlHandler].
     fn build_sparql_handler(
        direction: Direction,
        arity: Option<usize>,
        attributes: &HashMap<ImportExportAttribute, Term>,
    ) -> Box<dyn ImportExportHandler> {
        let endpoint = Self::read_endpoint(attributes);
        let query = Self::read_query(attributes);

        let limit = Self::read_limit(attributes);
        // TODO: do we expect/ allow the format parameter & treat it like dsv?
        let value_formats = Self::read_dsv_value_formats(attributes)
            .unwrap_or(DsvValueFormats::default(arity.unwrap_or_default()));

        Box::new(SparqlHandler::new(
            ImportExportResource::from_query(endpoint, query),
            limit,
            value_formats,
            direction,
        ))
    }
}
