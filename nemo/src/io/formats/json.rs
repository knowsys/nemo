use std::io::BufRead;

use nemo_physical::{
    datasources::table_providers::TableProvider,
    datavalues::{AnyDataValue, DataValue, MapDataValue},
    resource::Resource,
};

use super::{
    import_export::{
        ImportExportError, ImportExportHandler, ImportExportHandlers, ImportExportResource,
    },
    json_reader::JsonReader,
    types::Direction,
};

#[derive(Debug, Clone)]
pub(crate) struct JsonHandler {
    resource: ImportExportResource,
}

impl JsonHandler {
    pub(crate) fn try_new_import(
        attributes: &MapDataValue,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        // todo: check attributes
        let resource = ImportExportHandlers::extract_resource(attributes, Direction::Import)?;

        Ok(Box::new(JsonHandler { resource }))
    }
}

impl ImportExportHandler for JsonHandler {
    fn file_format(&self) -> crate::model::FileFormat {
        crate::model::FileFormat::JSON
    }

    fn reader(
        &self,
        read: Box<dyn BufRead>,
        arity: usize,
    ) -> Result<Box<dyn TableProvider>, crate::error::Error> {
        if arity != 3 {
            return Err(ImportExportError::InvalidArity { arity, expected: 3 }.into());
        }

        Ok(Box::new(JsonReader::new(read)))
    }

    fn writer(
        &self,
        writer: Box<dyn std::io::prelude::Write>,
        arity: usize,
    ) -> Result<Box<dyn super::types::TableWriter>, crate::error::Error> {
        unimplemented!()
    }

    fn predicate_arity(&self) -> Option<usize> {
        Some(3)
    }

    fn file_extension(&self) -> Option<String> {
        Some("json".into())
    }

    fn compression_format(&self) -> Option<crate::io::compression_format::CompressionFormat> {
        None
    }

    fn import_export_resource(&self) -> &ImportExportResource {
        &self.resource
    }
}
