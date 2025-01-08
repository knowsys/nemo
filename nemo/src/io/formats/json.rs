//! Handler for resources of type JSON (java script object notation).

pub(crate) mod reader;

use std::io::BufRead;

use nemo_physical::datasources::table_providers::TableProvider;
use reader::JsonReader;

use crate::rule_model::components::import_export::{
    compression::CompressionFormat, file_formats::FileFormat,
};

use super::{ImportExportHandler, ImportExportResource, TableWriter};

#[derive(Debug, Clone)]
pub(crate) struct JsonHandler {
    resource: ImportExportResource,
}

impl JsonHandler {
    pub fn new(resource: ImportExportResource) -> Self {
        Self { resource }
    }
}

impl ImportExportHandler for JsonHandler {
    fn file_format(&self) -> FileFormat {
        FileFormat::JSON
    }

    fn reader(
        &self,
        read: Box<dyn BufRead>,
    ) -> Result<Box<dyn TableProvider>, crate::error::Error> {
        Ok(Box::new(JsonReader::new(read)))
    }

    fn writer(
        &self,
        _writer: Box<dyn std::io::prelude::Write>,
    ) -> Result<Box<dyn TableWriter>, crate::error::Error> {
        unimplemented!("writing json is currently not supported")
    }

    fn predicate_arity(&self) -> usize {
        3
    }

    fn file_extension(&self) -> String {
        self.file_format().extension().to_string()
    }

    fn compression_format(&self) -> CompressionFormat {
        CompressionFormat::None
    }

    fn import_export_resource(&self) -> &ImportExportResource {
        &self.resource
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn format_metadata() {
        let handler = JsonHandler::new(ImportExportResource::from_string("dummy.json".to_string()));

        assert_eq!(FileFormat::JSON.extension(), handler.file_extension());
        assert_eq!(
            FileFormat::JSON.media_type(),
            handler.file_format().media_type()
        );
        assert_eq!(FileFormat::JSON.arity(), Some(handler.predicate_arity()));
    }
}
