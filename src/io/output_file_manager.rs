//! This module contains the OutputFileManager, which generates [`RecordWriter`] objects

use std::{
    fs::{create_dir_all, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use flate2::{write::GzEncoder, Compression};

use crate::{error::Error, logical::model::Identifier};

use super::RecordWriter;

/// Compression level for gzip output, cf. gzip(1):
///
/// > Regulate the speed of compression using the specified digit #,
/// > where -1 or --fast indicates the fastest compression method (less
/// > compression) and -9 or --best indicates the slowest compression
/// > method (best compression).  The default compression level is -6
/// > (that is, biased towards high compression at expense of speed).
const GZIP_COMPRESSION_LEVEL: Compression = Compression::new(6);

/// Represent the file-format of a file
#[derive(Debug, Copy, Clone)]
pub enum FileFormat {
    /// Delimiter Separated Values with the delimiter as u8
    DSV(u8),
}

impl FileFormat {
    fn file_name(&self, file: PathBuf) -> PathBuf {
        let ext = match self {
            FileFormat::DSV(delimiter) => match delimiter {
                b',' => "csv",
                b'\t' => "tsv",
                _ => "dsv",
            },
        };
        file.with_extension(append_extension(&file, ext))
    }

    fn create_writer<W: Write>(&self, writer: W) -> impl RecordWriter {
        match self {
            FileFormat::DSV(delimiter) => csv::WriterBuilder::new()
                .delimiter(*delimiter)
                .from_writer(writer),
        }
    }
}

/// Represent the compression of a file
#[derive(Debug, Copy, Clone)]
pub enum FileCompression {
    /// No file compression
    None,
    /// Compress with Gzip
    Gzip,
}

impl FileCompression {
    /// returns the file_name with right extensions
    pub fn file_name(&self, path: PathBuf) -> PathBuf {
        match self {
            FileCompression::None => path,
            FileCompression::Gzip => path.with_extension(append_extension(&path, "gz")),
        }
    }

    /// Create a writer, that compresses the output stream with the set compression    
    pub fn create_writer(
        &self,
        path: PathBuf,
        options: OpenOptions,
    ) -> Result<Box<dyn Write>, Error> {
        match self {
            FileCompression::None => {
                let writer = options.open(path)?;
                Ok(Box::new(writer))
            }
            FileCompression::Gzip => {
                let writer = GzEncoder::new(options.open(path)?, GZIP_COMPRESSION_LEVEL);
                Ok(Box::new(writer))
            }
        }
    }
}

/// Appends the extension to the path
fn append_extension(file: &Path, ext: &str) -> String {
    if let Some(existing_ext) = file.extension() {
        format!("{}.{ext}", existing_ext.to_str().expect("valid utf-8"))
    } else {
        ext.into()
    }
}

/// Contains all the needed information, to create output file writers
#[derive(Debug)]
pub struct OutputFileManager<'a> {
    /// The path to where the results shall be written to.
    path: &'a PathBuf,
    /// Overwrite files, note that the target folder will be emptied if `overwrite` is set to [true].
    overwrite: bool,
    /// Compression and file format.
    pub compression_format: FileCompression,
    /// Data format used (csv, dsv, ...)
    pub data_format: FileFormat,
}

impl<'a> OutputFileManager<'a> {
    /// Instantiate an [`OutputFileManager`].
    ///
    /// Instantiates a new [`OutputFileManager`] if the given `path` is writable. Otherwise an [`Error`] is thrown.
    pub fn try_new(path: &'a PathBuf, overwrite: bool, gzip: bool) -> Result<Self, Error> {
        create_dir_all(path)?;
        let data_format = FileFormat::DSV(b',');
        let compression_format = if gzip {
            FileCompression::Gzip
        } else {
            FileCompression::None
        };
        Ok(OutputFileManager {
            path,
            overwrite,
            compression_format,
            data_format,
        })
    }
}

impl OutputFileManager<'_> {
    /// Get the output file name for the given predicate, including all extensions
    pub fn get_output_file_name(&self, pred: &Identifier) -> PathBuf {
        let mut pred_path = pred.sanitised_file_name(self.path.to_path_buf());
        pred_path = self.data_format.file_name(pred_path);
        pred_path = self.compression_format.file_name(pred_path);
        pred_path
    }

    /// Creates a file for predicate.
    ///
    /// The created file follows the previously stated file format and compression method.on.
    /// Returns a [`RecordWriter`] for a file named appropriately for the given [`predicate`][Identifier]
    pub fn create_file_writer(&self, pred: &Identifier) -> Result<impl RecordWriter, Error> {
        let mut options = OpenOptions::new();
        options.write(true);
        if self.overwrite {
            options.create(true).truncate(true);
        } else {
            options.create_new(true);
        };

        let pred_path = self.get_output_file_name(pred);
        log::info!("Creating {} as {pred_path:?}", pred.name());

        let file_writer = self.compression_format.create_writer(pred_path, options)?;
        let record_writer = self.data_format.create_writer(file_writer);

        Ok(record_writer)
    }
}
