//! This module contains the [ExportManager], which provides the main API to handle
//! [ExportDirective]s and to write tables to files.

use std::{
    fs::{create_dir_all, OpenOptions},
    io::{ErrorKind, Write},
    path::PathBuf,
};

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    error::Error,
    io::formats::import_export::ImportExportHandlers,
    model::{ExportDirective, Identifier},
};

use sanitise_file_name::{sanitise_with_options, Options};

use super::{
    compression_format::CompressionFormat,
    formats::import_export::{ImportExportError, ImportExportHandler},
};

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
    /// Default compression format to be used.
    pub(crate) compression_format: CompressionFormat,
}

impl ExportManager {
    /// Constructor.
    pub fn new() -> Self {
        Default::default()
    }

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
            CompressionFormat::Gzip
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
    pub fn validate(&self, export_directive: &ExportDirective) -> Result<(), Error> {
        let handler = ImportExportHandlers::export_handler(export_directive)?;
        let path = self.output_file_path(&handler, &export_directive.predicate());

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

    /// Export a (possibly empty) table according to the given [ExportDirective].
    /// If the table is empty (i.e., [Option<_>::None]), an empty output file will be created.
    ///
    /// The `predicate_arity` is the arity of the predicate that is to be exported. This information
    /// is used for validation and as a hint to exporters that were not initialized with details
    /// about the arity.
    pub fn export_table<'a>(
        &self,
        export_directive: &ExportDirective,
        table: Option<impl Iterator<Item = Vec<AnyDataValue>> + 'a>,
        predicate_arity: usize,
    ) -> Result<(), Error> {
        if self.disable_write {
            return Ok(());
        }

        let handler = ImportExportHandlers::export_handler(export_directive)?;

        let writer = self.writer(&handler, &export_directive.predicate())?;

        self.export_table_with_handler_writer(&handler, writer, table, predicate_arity)
    }

    /// Export a (possibly empty) table according to the given [ExportDirective],
    /// but direct output into the given writer instead of using whatever
    /// resource the directive specifies.
    ///
    /// The `predicate_arity` is the arity of the predicate that is to be exported. This information
    /// is used for validation and as a hint to exporters that were not initialized with details
    /// about the arity.
    ///
    /// This function ignores [ExportManager::disable_write].
    pub fn export_table_with_writer<'a>(
        &self,
        export_directive: &ExportDirective,
        writer: Box<dyn Write>,
        table: Option<impl Iterator<Item = Vec<AnyDataValue>> + 'a>,
        predicate_arity: usize,
    ) -> Result<(), Error> {
        let handler = ImportExportHandlers::export_handler(export_directive)?;
        self.export_table_with_handler_writer(&handler, writer, table, predicate_arity)
    }

    /// Export a (possibly empty) table according to the given [ImportExportHandler],
    /// and direct output into the given writer instead of using whatever
    /// resource the handler specifies.
    ///
    /// The `predicate_arity` is the arity of the predicate that is to be exported. This information
    /// is used for validation and as a hint to exporters that were not initialized with details
    /// about the arity.
    ///
    /// This function ignores [ExportManager::disable_write].
    pub(crate) fn export_table_with_handler_writer<'a>(
        &self,
        export_handler: &Box<dyn ImportExportHandler>,
        writer: Box<dyn Write>,
        table: Option<impl Iterator<Item = Vec<AnyDataValue>> + 'a>,
        predicate_arity: usize,
    ) -> Result<(), Error> {
        if let Some(export_arity) = export_handler.predicate_arity() {
            if export_arity != predicate_arity {
                return Err(ImportExportError::InvalidArity {
                    arity: export_arity,
                    expected: predicate_arity,
                }
                .into());
            }
        }
        if let Some(table) = table {
            let table_writer = export_handler.writer(writer, predicate_arity)?;
            table_writer.export_table_data(Box::new(table))?;
        }
        Ok(())
    }

    /// Create a writer based on an export handler. The predicate is used to
    /// obtain a default file name if needed.
    ///
    /// This function may already create directories, and should not be used if
    /// [ExportManager::disable_write] is `true`.
    fn writer(
        &self,
        export_handler: &Box<dyn ImportExportHandler>,
        predicate: &Identifier,
    ) -> Result<Box<dyn Write>, Error> {
        let output_path = self.output_file_path(export_handler, predicate);

        log::info!(
            "Exporting predicate \"{}\" to {output_path:?}",
            predicate.name()
        );

        if let Some(parent) = output_path.parent() {
            create_dir_all(parent)?;
        }

        export_handler
            .compression_format()
            .unwrap_or(self.compression_format)
            .writer(output_path, Self::open_options(self.overwrite))
    }

    /// Get the output file name for the given [ExportDirective]. This is a complete path (based on our base path),
    /// which includes all extensions.
    fn output_file_path(
        &self,
        export_handler: &Box<dyn ImportExportHandler>,
        predicate: &Identifier,
    ) -> PathBuf {
        let mut pred_path = self.base_path.to_path_buf();

        let sanitise_options = Options::<Option<char>> {
            url_safe: true,
            ..Default::default()
        };

        let file_name_unsafe = export_handler.resource().unwrap_or_else(|| {
            if let Some(ext) = export_handler.file_extension() {
                predicate.name() + "." + ext.as_str()
            } else {
                predicate.name()
            }
        });
        let file_name = sanitise_with_options(&file_name_unsafe, &sanitise_options);
        pred_path.push(file_name);

        pred_path = export_handler
            .compression_format()
            .unwrap_or(self.compression_format)
            .path_with_extension(pred_path);
        pred_path
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
