//! Handler for resources of type Sparql (java script object notation).

//pub(crate) mod reader;
pub mod value_format;

use std::io::BufRead;

use nemo_physical::datasources::table_providers::TableProvider;
//use reader::SparqlReader;

use crate::io::formats::dsv::{value_format::DsvValueFormats, DsvHandler};
//use value_format::SparqlValueFormats;

use crate::rule_model::components::import_export::{
    compression::CompressionFormat, file_formats::FileFormat,
};

use super::{Direction, ImportExportHandler, ImportExportResource, TableWriter};

#[derive(Debug, Clone)]
pub(crate) struct SparqlHandler {
    /// The 
    //resource: ImportExportResource,
    //limit: Option<u64>,
    //value_formats: DsvValueFormats,
    //_direction: Direction,
    dsv_handler: DsvHandler,
}

impl SparqlHandler {
    pub fn new(
        resource: ImportExportResource,
        limit: Option<u64>,
        value_formats: DsvValueFormats,
        _direction: Direction,
    ) -> Self {
        Self { 
            //resource,
            //limit,
            //value_formats,
            //_direction,
            dsv_handler: DsvHandler::new(
                b'\t',
                resource,
                value_formats,
                limit,
                CompressionFormat::None,
                true,
                _direction,)
        }
    }
}

impl ImportExportHandler for SparqlHandler {
    fn file_format(&self) -> FileFormat {
        FileFormat::Sparql
    }

    fn reader(
        &self,
        read: Box<dyn BufRead>,
    ) -> Result<Box<dyn TableProvider>, crate::error::Error> {
            self.dsv_handler.reader(read)
    }

    fn writer(
        &self,
        _writer: Box<dyn std::io::prelude::Write>,
    ) -> Result<Box<dyn TableWriter>, crate::error::Error> {
        unimplemented!("writing sparql is currently not supported")
    }

    fn predicate_arity(&self) -> usize {
        self.dsv_handler.predicate_arity()
    }

    fn file_extension(&self) -> String {
        // TODO: should this return something else?
        String::from("")
    }

    fn compression_format(&self) -> CompressionFormat {
        CompressionFormat::None
    }

    fn import_export_resource(&self) -> &ImportExportResource {
        self.dsv_handler.import_export_resource()
    }
}

#[cfg(test)]
mod test {
    // TODO: Implement meaningful test
    use super::*;
}