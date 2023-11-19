//! This module contains the [OutputManager], which writes tables to files.

use std::{
    fs::{create_dir_all, OpenOptions},
    io::{ErrorKind, Write},
    path::PathBuf,
};

use flate2::{write::GzEncoder, Compression};

use crate::{
    error::Error,
    io::formats::types::{ExportSpec, PathWithFormatSpecificExtension},
    model::{Identifier, TupleConstraint},
};

use super::formats::DSVFormat;

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
}

impl PathWithFormatSpecificExtension for CompressionFormat {
    fn extension(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Gzip => Some("gz"),
        }
    }
}

/// Contains all the needed information to create output file writers.
#[derive(Debug, Default)]
pub struct OutputManager {
    /// The path to where the results shall be written to.
    path: PathBuf,
    /// Overwrite files, note that the target folder will be emptied if `overwrite` is set to [true].
    overwrite: bool,
    /// Compression and file format.
    pub(crate) compression_format: CompressionFormat,
}

impl OutputManager {
    /// Create a [builder][OutputManagerBuilder] for an [output
    /// file manager][OutputManager] with the given [path][PathBuf].
    pub fn builder(path: PathBuf) -> Result<OutputManagerBuilder, Error> {
        create_dir_all(&path)?;
        Ok(OutputManagerBuilder::new(path))
    }

    /// Get the output file name for the given predicate, including all extensions
    pub fn output_file_name(&self, export_spec: &ExportSpec) -> PathBuf {
        let mut pred_path = export_spec
            .predicate()
            .sanitised_file_name(self.path.to_path_buf());
        pred_path = export_spec.file_format().path_with_extension(pred_path);
        pred_path = self.compression_format.path_with_extension(pred_path);
        pred_path
    }

    fn open_options(&self) -> OpenOptions {
        let mut options = OpenOptions::new();
        options.write(true);

        if self.overwrite {
            options.create(true).truncate(true);
        } else {
            options.create_new(true);
        };

        options
    }

    /// Export
    pub fn export_table(
        &self,
        export_spec: &ExportSpec,
        table: Option<impl Iterator<Item = Vec<String>>>,
    ) -> Result<(), Error> {
        let output_path = self.output_file_name(export_spec);
        log::info!(
            "Creating {} as {output_path:?}",
            export_spec.predicate().name()
        );

        let mut writer = self
            .compression_format
            .writer(output_path, self.open_options())?;

        if let Some(table) = table {
            export_spec.write_table(table, &mut writer)?;
        }

        Ok(())
    }

    /// Checks if results shall be saved without allowing to overwrite
    /// Returns an Error if files are existing without being allowed to overwrite them
    pub fn prevent_accidental_overwrite(
        &self,
        output_predicates: impl Iterator<Item = ExportSpec>,
    ) -> Result<(), Error> {
        if self.overwrite {
            return Ok(());
        }

        for pred in output_predicates {
            let path = self.output_file_name(&pred);
            let meta_info = path.metadata();
            if let Err(err) = meta_info {
                if err.kind() != ErrorKind::NotFound {
                    return Err(Error::IO(err));
                }
            } else {
                return Err(Error::IOExists {
                    error: ErrorKind::AlreadyExists.into(),
                    path,
                });
            }
        }

        Ok(())
    }

    /// Obtain a default export spec (CSV) for the given [predicate][Identifier]
    /// and the [declared types][TupleConstraint].
    pub fn default_export_spec(
        predicate: Identifier,
        declared_types: TupleConstraint,
    ) -> Result<ExportSpec, Error> {
        Ok(DSVFormat::csv().try_into_export(predicate.name(), predicate, declared_types)?)
    }
}

/// A builder for an [OutputManager].
#[derive(Debug)]
pub struct OutputManagerBuilder {
    manager: OutputManager,
}

impl OutputManagerBuilder {
    /// Create a new builder with the given [path][PathBuf].
    pub fn new(path: PathBuf) -> Self {
        let manager = OutputManager {
            path,
            ..Default::default()
        };

        Self { manager }
    }

    /// Build the [output manager][OutputManager].
    pub fn build(self) -> OutputManager {
        self.manager
    }

    /// Enable overwriting.
    pub fn overwrite(mut self) -> Self {
        self.manager.overwrite = true;
        self
    }

    /// Disable overwriting.
    pub fn do_not_overwrite(mut self) -> Self {
        self.manager.overwrite = false;
        self
    }

    /// Enable gzip compression.
    pub fn gzip(mut self) -> Self {
        self.manager.compression_format = CompressionFormat::Gzip;
        self
    }

    /// Disable compression.
    pub fn do_not_compress(mut self) -> Self {
        self.manager.compression_format = CompressionFormat::None;
        self
    }
}
