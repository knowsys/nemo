//! Handler for resources of type JSON (java script object notation).

pub(crate) mod reader;

use std::{io::Read, sync::Arc};

use nemo_physical::datasources::table_providers::TableProvider;
use reader::JsonReader;

use crate::{
    chase_model::components::rule::ChaseRule,
    io::format_builder::{
        AnyImportExportBuilder, FormatBuilder, Parameters, StandardParameter, SupportedFormatTag,
        format_tag,
    },
    rule_model::{
        components::import_export::Direction,
        error::validation_error::ValidationError,
    },
    syntax::import_export::file_format,
};

use super::{FileFormatMeta, ImportHandler};

#[derive(Debug, Clone)]
pub(crate) struct JsonHandler;

impl JsonHandler {
    /// Return the [SupportedFormatTag] for this handler.
    pub fn format_tag(&self) -> SupportedFormatTag {
        SupportedFormatTag::Json(JsonTag::Json)
    }
}

impl FileFormatMeta for JsonHandler {
    fn default_extension(&self) -> String {
        file_format::EXTENSION_JSON.to_string()
    }

    fn media_type(&self) -> String {
        file_format::MEDIA_TYPE_JSON.to_string()
    }
}

impl ImportHandler for JsonHandler {
    fn reader(&self, read: Box<dyn Read>) -> Result<Box<dyn TableProvider>, crate::error::Error> {
        Ok(Box::new(JsonReader(read)))
    }
}

format_tag! {
    pub enum JsonTag(SupportedFormatTag::Json) {
        Json => file_format::JSON,
    }
}

impl From<JsonHandler> for AnyImportExportBuilder {
    fn from(value: JsonHandler) -> Self {
        AnyImportExportBuilder::Json(value)
    }
}

impl FormatBuilder for JsonHandler {
    type Tag = JsonTag;
    type Parameter = StandardParameter;

    fn new(
        _tag: Self::Tag,
        _parameters: &Parameters<Self>,
        direction: Direction,
    ) -> Result<Self, ValidationError> {
        if matches!(direction, Direction::Export) {
            return Err(ValidationError::UnsupportedJsonExport);
        }

        Ok(Self)
    }

    fn expected_arity(&self) -> Option<usize> {
        Some(3)
    }

    fn build_import(
        &self,
        _arity: usize,
        filter_rules: Vec<ChaseRule>,
    ) -> Arc<dyn ImportHandler + Send + Sync + 'static> {
        Arc::new(Self)
    }

    fn build_export(
        &self,
        _arity: usize,
        _filter_rules: Vec<ChaseRule>,
    ) -> Arc<dyn super::ExportHandler + Send + Sync + 'static> {
        unimplemented!("json export is currently not supported")
    }
}
