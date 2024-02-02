//! This module contains the [OutputManager], which writes tables to files.

use std::{
    fs::{create_dir_all, OpenOptions},
    io::{ErrorKind, Write},
    path::PathBuf,
};

use flate2::{write::GzEncoder, Compression};
use nemo_physical::datavalues::AnyDataValue;

use crate::{
    error::Error,
    io::formats::import_export::ImportExportHandlers,
    model::{ExportDirective, Identifier},
};

use sanitise_file_name::{sanitise_with_options, Options};

use super::formats::import_export::ImportExportHandler;

/// Compression level for gzip output, cf. gzip(1):
///
/// > Regulate the speed of compression using the specified digit #,
/// > where -1 or --fast indicates the fastest compression method (less
/// > compression) and -9 or --best indicates the slowest compression
/// > method (best compression).  The default compression level is -6
/// > (that is, biased towards high compression at expense of speed).
const GZIP_COMPRESSION_LEVEL: Compression = Compression::new(6);

/// Represent the compression of a file
#[derive(Debug, Copy, Clone, Default)]
pub enum CompressionFormat {
    /// No file compression
    #[default]
    None,
    /// Compress with Gzip
    Gzip,
}

impl CompressionFormat {
    /// Create a writer, that compresses the output stream with the set compression
    pub fn writer(&self, path: PathBuf, options: OpenOptions) -> Result<Box<dyn Write>, Error> {
        match self {
            CompressionFormat::None => {
                let writer = options.open(path)?;
                Ok(Box::new(writer))
            }
            CompressionFormat::Gzip => {
                let writer = GzEncoder::new(options.open(path)?, GZIP_COMPRESSION_LEVEL);
                Ok(Box::new(writer))
            }
        }
    }

    /// Returns the file extension to be used in files of this compression format.
    pub fn extension(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Gzip => Some("gz"),
        }
    }
}

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
    pub fn export_table<'a>(
        &self,
        export_directive: &ExportDirective,
        table: Option<impl Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error> {
        if self.disable_write {
            return Ok(());
        }

        let handler = ImportExportHandlers::export_handler(export_directive)?;

        let writer = self.writer(&handler, &export_directive.predicate())?;

        self.export_table_with_handler_writer(&handler, writer, table)
    }

    /// Export a (possibly empty) table according to the given [ExportDirective],
    /// but direct output into the given writer instead of using whatever
    /// resource the directive specifies.
    ///
    /// This function ignores [ExportManager::disable_write].
    pub fn export_table_with_writer<'a>(
        &self,
        export_directive: &ExportDirective,
        writer: Box<dyn Write>,
        table: Option<impl Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error> {
        let handler = ImportExportHandlers::export_handler(export_directive)?;
        self.export_table_with_handler_writer(&handler, writer, table)
    }

    /// Export a (possibly empty) table according to the given [ImportExportHandler],
    /// and direct output into the given writer instead of using whatever
    /// resource the handler specifies.
    pub(crate) fn export_table_with_handler_writer<'a>(
        &self,
        export_handler: &Box<dyn ImportExportHandler>,
        writer: Box<dyn Write>,
        table: Option<impl Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error> {
        if let Some(table) = table {
            let mut table_writer = export_handler.writer(writer)?;
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

        self.compression_format
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

        pred_path = Self::path_with_extension(pred_path, self.compression_format.extension());
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

    /// Augment the given [path][PathBuf] with an optional file extension.
    /// Existing extentions that are different from the new extension are kept
    /// as part of the file name.
    fn path_with_extension(path: PathBuf, extension: Option<&str>) -> PathBuf {
        match extension {
            Some(new_extension) => path.with_extension(match path.extension() {
                Some(existing_extension) => {
                    let existing_extension = existing_extension.to_str().expect("valid UTF-8");

                    if existing_extension == new_extension {
                        existing_extension.to_string()
                    } else {
                        format!("{existing_extension}.{new_extension}")
                    }
                }
                None => new_extension.to_string(),
            }),
            None => path,
        }
    }
}
