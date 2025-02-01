//! Handler for resources of type JSON (java script object notation).

pub(crate) mod reader;

use std::io::Read;

use nemo_physical::datasources::table_providers::TableProvider;
use reader::JsonReader;

use crate::syntax::import_export::file_format;

use super::{FileFormatMeta, ImportHandler, ResourceSpec};

#[derive(Debug, Clone)]
pub(crate) struct JsonHandler {
    resource: ResourceSpec,
}

impl JsonHandler {
    pub fn new(resource: ResourceSpec) -> Self {
        Self { resource }
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
