//! This module contains the [ExportManager].
use std::{
    fs::{OpenOptions, create_dir_all},
    io::{ErrorKind, Write},
    path::PathBuf,
};

use crate::{error::Error, rule_model::components::tag::Tag};

use nemo_physical::{datavalues::AnyDataValue, resource::Resource};
use sanitise_file_name::{Options, sanitise_with_options};

use super::{
    compression_format::CompressionFormat,
    formats::{Export, ExportHandler},
};

/// Main object for exporting data to files.
#[derive(Debug, Default)]
pub struct ExportManager {
    /// The base path for writing files.
    base_path: PathBuf,
    /// Overwrite files, note that the target folder will be emptied if `overwrite` is set to [true].
    overwrite: bool,
    /// If `true`, then writing operations will not be performed. The object can still be used for validation etc.
    disable_write: bool,
    /// Compression format to be used by default.
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

    /// Validates the given [ExportHandler].
    /// This also checks whether the specified file could (likely) be written.
    pub fn validate(&self, _predicate: &Tag, handler: &Export) -> Result<(), Error> {
        let resource = handler.resource();

        if resource.is_pipe() {
            return Ok(());
        };

        let path = self.sanitized_path(resource, !handler.is_compressed());

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

    /// Get the output file path for the given (unsanitized) file name.
    ///
    /// This is a complete path (based on our base path),
    /// which includes all extensions.
    fn sanitized_path(&self, resource: &Resource, add_compression: bool) -> PathBuf {
        let mut pred_path = self.base_path.to_path_buf();

        let sanitize_options = Options::<Option<char>> {
            url_safe: true,
            ..Default::default()
        };

        let file_name_unsanitized = resource.to_string();

        let file_name = sanitise_with_options(&file_name_unsanitized, &sanitize_options);

        pred_path.push(file_name);

        if add_compression && let Some(ext) = self.default_compression_format.extension() {
            pred_path.add_extension(ext);
        }

        pred_path
    }

    /// Create a writer based on an export handler. The predicate is used to
    /// obtain a default file name if needed.
    ///
    /// This function may already create directories, and should not be used if
    /// [ExportManager::disable_write] is `true`.
    fn writer(&self, export_handler: &Export, predicate: &Tag) -> Result<Box<dyn Write>, Error> {
        let resource = export_handler.resource();
        let writer: Box<dyn Write> = if resource.is_pipe() {
            Box::new(std::io::stdout().lock())
        } else {
            let output_path = self.sanitized_path(resource, !export_handler.is_compressed());

            log::info!("Exporting predicate \"{predicate}\" to {output_path:?}");

            if let Some(parent) = output_path.parent() {
                create_dir_all(parent)?;
            }

            Box::new(Self::open_options(self.overwrite).open(output_path)?)
        };

        if resource.supports_compression() && !export_handler.is_compressed() {
            Ok(self
                .default_compression_format
                .implementation()
                .compress(writer))
        } else {
            Ok(writer)
        }
    }

    /// Export a (possibly empty) table according to the given [ExportHandler],
    /// and direct output into the given writer.
    ///
    /// Nothing is written if writing is disabled.
    ///
    /// If this operation succeeds, then it returns `Ok(true)` if the resource is stdout
    /// and `Ok(false)` otherwise.
    pub fn export_table<'a>(
        &self,
        predicate: &Tag,
        export_handler: &Export,
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

        Ok(export_handler.resource().is_pipe())
    }

    /// Export a (possibly empty) table according to the given [ExportHandler],
    /// and directly output into the given writer.
    ///
    /// This function ignores `ExportManager::disable_write` and `ExportManager::default_compression_format`.
    pub fn export_table_with_writer<'a>(
        &self,
        writer: Box<dyn Write>,
        export_handler: &impl ExportHandler,
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
