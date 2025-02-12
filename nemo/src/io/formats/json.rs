//! Handler for resources of type JSON (java script object notation).

pub(crate) mod reader;

use std::{io::Read, sync::Arc};

use nemo_physical::datasources::table_providers::TableProvider;
use reader::JsonReader;

use crate::{
    io::format_builder::{
        format_tag, AnyImportExportBuilder, FormatBuilder, Parameters, StandardParameter,
    },
    rule_model::{
        components::import_export::Direction, error::validation_error::ValidationErrorKind,
    },
    syntax::import_export::file_format,
};

use super::{FileFormatMeta, ImportHandler, ResourceSpec};

#[derive(Debug, Clone)]
pub(crate) struct JsonHandler;

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
    pub(crate) enum JsonTag(SupportedFormatTag::Json) {
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
    ) -> Result<(Self, Option<ResourceSpec>), ValidationErrorKind> {
        if matches!(direction, Direction::Export) {
            return Err(ValidationErrorKind::UnsupportedJsonExport);
        }

        Ok((Self, None))
    }

    fn expected_arity(&self) -> Option<usize> {
        Some(3)
    }

    fn build_import(&self, _arity: usize) -> Arc<dyn ImportHandler + Send + Sync + 'static> {
        Arc::new(Self)
    }

    fn build_export(&self, _arity: usize) -> Arc<dyn super::ExportHandler + Send + Sync + 'static> {
        unimplemented!("json export is currently not supported")
    }
}
