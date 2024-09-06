//! This module contains the [ExportManager], which provides the main API to handle
//! [ExportDirective]s and to write tables to files.

use std::{
    io::{ErrorKind, Write},
    path::PathBuf,
};

use crate::{error::Error, rule_model::components::import_export::compression::CompressionFormat};

use nemo_physical::datavalues::AnyDataValue;
use sanitise_file_name::{sanitise_with_options, Options};

use super::formats::ImportExportHandler;

/// Main object for exporting data to files and for accessing aspects
/// of [ExportDirective]s that might be of public interest.
#[derive(Debug, Default)]
pub struct ExportManager {
    /// The base path for writing files.
    base_path: PathBuf,
    /// Overwrite files, note that the target folder will be emptied if `overwrite` is set to [true].
    overwrite: bool,
    /// If `true`, then writing operations will not be performed. The object can still be used for validation etc.
    disable_write: bool,
    /// Compression format to be used.
    compression_format: CompressionFormat,
}

impl ExportManager {
    /// Use the given path as a base path.
    pub fn set_base_path(mut self, path: PathBuf) -> Self {
        self.base_path = path;
        self
    }

    /// Define if existing files should be overwritten
    /// or not.
    pub fn overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Define if gzip compression should be the default
    /// choice.
    pub fn compress(mut self, compress: bool) -> Self {
        self.compression_format = if compress {
            CompressionFormat::GZip
        } else {
            CompressionFormat::None
        };
        self
    }

    /// Disable writing. Writing operations will not be performed, but
    /// data will still be validated.
    pub fn disable_write(mut self) -> Self {
        self.disable_write = true;
        self
    }

    /// Returns true if writing has been disabled.
    pub fn write_disabled(&self) -> bool {
        self.disable_write
    }

    /// Validates the given [ExportDirective].
    /// This also checks whether the specified file could (likely) be written.
    pub fn validate(&self, handler: &dyn ImportExportHandler) -> Result<(), Error> {
        // let handler = ImportExportHandlers::export_handler(export_directive)?;

        if handler.resource_is_stdout() {
            return Ok(());
        }

        let path = self.output_file_path(handler);

        let meta_info = path.metadata();
        if let Err(err) = meta_info {
            if err.kind() == ErrorKind::NotFound {
                Ok(())
            } else {
                Err(Error::IO(err))
            }
        } else if self.overwrite || self.disable_write {
            Ok(())
        } else {
            Err(Error::IOExists {
                error: ErrorKind::AlreadyExists.into(),
                path,
            })
        }
    }

    /// Get the output file name for the given [ExportDirective].
    ///
    /// This is a complete path (based on our base path),
    /// which includes all extensions.
    fn output_file_path(&self, export_handler: &dyn ImportExportHandler) -> PathBuf {
        let mut pred_path = self.base_path.to_path_buf();

        let sanitize_options = Options::<Option<char>> {
            url_safe: true,
            ..Default::default()
        };

        let file_name_unsafe = export_handler
            .resource()
            .unwrap_or_else(|| export_handler.file_extension());
        let file_name = sanitise_with_options(&file_name_unsafe, &sanitize_options);
        pred_path.push(file_name);

        pred_path = export_handler
            .compression_format()
            .path_with_extension(pred_path);
        pred_path
    }

    /// Export a (possibly empty) table according to the given [ImportExportHandler],
    /// and direct output into the given writer.
    ///
    /// Nothing is written if writing is disabled.
    ///
    /// If this operation succeeds, then it returns `Ok(true)` if the resource is stdout
    /// and `Ok(false)` otherwise.
    pub(crate) fn export_table_with_handler_writer<'a>(
        &self,
        export_handler: &dyn ImportExportHandler,
        writer: Box<dyn Write>,
        table: Option<impl Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<bool, Error> {
        if self.disable_write {
            return Ok(false);
        }

        if let Some(table) = table {
            let table_writer = export_handler.writer(writer)?;
            table_writer.export_table_data(Box::new(table))?;
        }

        Ok(export_handler.resource_is_stdout())
    }
}
