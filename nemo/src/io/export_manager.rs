//! This module contains the [ExportManager].
use std::{
    fs::{create_dir_all, OpenOptions},
    io::{ErrorKind, Write},
    path::PathBuf,
};

use crate::{
    error::Error,
    rule_model::components::{import_export::compression::CompressionFormat, tag::Tag},
};

use nemo_physical::{datavalues::AnyDataValue, resource::Resource};
use sanitise_file_name::{sanitise_with_options, Options};

use super::formats::ImportExportHandler;

/// Main object for exporting data to files.
#[derive(Debug, Default)]
pub struct ExportManager {
    /// The base path for writing files.
    base_path: PathBuf,
    /// Overwrite files, note that the target folder will be emptied if `overwrite` is set to [true].
    overwrite: bool,
    /// If `true`, then writing operations will not be performed. The object can still be used for validation etc.
    disable_write: bool,
    /// Compression format to be used.
    default_compression_format: CompressionFormat,
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
        self.default_compression_format = if compress {
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

    /// Validates the given [ImportExportHandler].
    /// This also checks whether the specified file could (likely) be written.
    pub fn validate(
        &self,
        predicate: &Tag,
        handler: &dyn ImportExportHandler,
    ) -> Result<(), Error> {
        if handler.resource_is_stdout() {
            return Ok(());
        }

        let path = self.output_file_path(predicate, handler);

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

    /// Returns the compression format for an export handler,
    /// or the default compression format if the handler does not specify compression.
    fn compression_format(&self, export_handler: &dyn ImportExportHandler) -> CompressionFormat {
        let their_compression_format = export_handler.compression_format();

        if matches!(their_compression_format, CompressionFormat::None) {
            self.default_compression_format
        } else {
            their_compression_format
        }
    }

    /// Create a writer based on an export handler. The predicate is used to
    /// obtain a default file name if needed.
    ///
    /// This function may already create directories, and should not be used if
    /// [ExportManager::disable_write] is `true`.
    fn writer(
        &self,
        export_handler: &dyn ImportExportHandler,
        predicate: &Tag,
    ) -> Result<Box<dyn Write>, Error> {
        if export_handler.resource_is_stdout() {
            Ok(Box::new(std::io::stdout().lock()))
        } else {
            let output_path = self.output_file_path(predicate, export_handler);

            log::info!("Exporting predicate \"{}\" to {output_path:?}", predicate);

            if let Some(parent) = output_path.parent() {
                create_dir_all(parent)?;
            }

            self.compression_format(export_handler)
                .file_writer(output_path, Self::open_options(self.overwrite))
        }
    }

    /// Get the output file name for the given [ImportExportHandler].
    ///
    /// This is a complete path (based on our base path),
    /// which includes all extensions.
    fn output_file_path(
        &self,
        predicate: &Tag,
        export_handler: &dyn ImportExportHandler,
    ) -> PathBuf {
        let mut pred_path = self.base_path.to_path_buf();

        let sanitize_options = Options::<Option<char>> {
            url_safe: true,
            ..Default::default()
        };

        let file_name_unsafe = export_handler
            .resource()
            .map(|resource| match resource {
                Resource::Path(path) => path,
                Resource::Iri {iri,..} => iri.to_string()
            })
            .unwrap_or(format!(
            "{}.{}",
            predicate,
            export_handler.file_extension()
        ));
        let file_name = sanitise_with_options(&file_name_unsafe, &sanitize_options);
        pred_path.push(file_name);

        pred_path = self
            .compression_format(export_handler)
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
    pub fn export_table<'a>(
        &self,
        predicate: &Tag,
        export_handler: &dyn ImportExportHandler,
        table: Option<impl Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<bool, Error> {
        if self.disable_write {
            return Ok(false);
        }

        let writer = self.writer(export_handler, predicate)?;

        if let Some(table) = table {
            let table_writer = export_handler.writer(writer)?;
            table_writer.export_table_data(Box::new(table))?;
        }

        Ok(export_handler.resource_is_stdout())
    }

    /// Export a (possibly empty) table according to the given [ImportExportHandler],
    /// and directly output into the given writer.
    ///
    /// This function ignores [ExportManager::disable_write].
    pub fn export_table_with_writer<'a>(
        &self,
        writer: Box<dyn Write>,
        export_handler: &dyn ImportExportHandler,
        table: Option<impl Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error> {
        if let Some(table) = table {
            let table_writer = export_handler.writer(writer)?;
            table_writer.export_table_data(Box::new(table))?;
        }

        Ok(())
    }

    /// Provide suitable options writing to files under the given settings.
    fn open_options(overwrite: bool) -> OpenOptions {
        let mut options = OpenOptions::new();
        options.write(true);

        if overwrite {
            options.create(true).truncate(true);
        } else {
            options.create_new(true);
        };

        options
    }
}
