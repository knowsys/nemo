//! This module contains utility functions, like importing data from various sources

use std::path::{Path, PathBuf};

pub mod builder_proxy;
pub mod dsv;
pub mod parser;

/// Represent the file-format of a file
#[derive(Debug, Copy, Clone)]
pub enum FileFormat {
    /// Delimiter Separated Values with the delimiter as String
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
        file.with_extension(concat_extensions(&file, ext))
    }
}
/// Represent the compression of a file
#[derive(Debug, Copy, Clone)]
pub enum FileCompression {
    /// No file compression
    None(FileFormat),
    /// Compress with Gzip
    Gzip(FileFormat),
}

impl FileCompression {
    /// Creates a new [`FileCompression`]
    pub fn new(gzip: bool, file_format: FileFormat) -> Self {
        if gzip {
            Self::Gzip(file_format)
        } else {
            Self::None(file_format)
        }
    }

    /// returns the file_name with right extensions
    pub fn file_name(&self, file: PathBuf) -> PathBuf {
        match self {
            FileCompression::None(file_format) => file_format.file_name(file),
            FileCompression::Gzip(file_format) => {
                let file = file_format.file_name(file);
                file.with_extension(concat_extensions(&file, "gz"))
            }
        }
    }
}

fn concat_extensions(file: &Path, ext: &str) -> String {
    if let Some(existing_ext) = file.extension() {
        format!(
            "{}.{ext}",
            existing_ext.to_str().expect("Writable Symbol expected")
        )
    } else {
        ext.into()
    }
}
