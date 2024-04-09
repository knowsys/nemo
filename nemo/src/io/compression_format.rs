//! Handling of compression during import and export.

use std::{
    fs::OpenOptions,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};

use flate2::bufread::MultiGzDecoder;
use flate2::{write::GzEncoder, Compression};
use nemo_physical::resource::Resource;

use crate::error::Error;

/// Compression level for gzip output, cf. gzip(1):
///
/// > Regulate the speed of compression using the specified digit #,
/// > where -1 or --fast indicates the fastest compression method (less
/// > compression) and -9 or --best indicates the slowest compression
/// > method (best compression).  The default compression level is -6
/// > (that is, biased towards high compression at expense of speed).
const GZIP_COMPRESSION_LEVEL: Compression = Compression::new(6);

/// Represent the compression of a file
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub enum CompressionFormat {
    /// No file compression
    #[default]
    None,
    /// Compress with Gzip
    Gzip,
}

impl CompressionFormat {
    /// Derive a compression format from the file extension of the given resource,
    /// and return the compression format and the resource string without this extenions.
    pub fn from_resource(resource: &Resource) -> (CompressionFormat, Resource) {
        match resource {
            resource if resource.ends_with(".gz") => (
                CompressionFormat::Gzip,
                resource.as_str()[0..resource.len() - 3].to_string(),
            ),
            _ => (CompressionFormat::None, resource.to_owned()),
        }
    }

    /// Create a writer that compresses the output stream according to this compression.
    ///
    /// The path is used as is. To make sure the path ends in the compression-format specific
    /// extension, use [CompressionFormat::path_with_extension].
    pub(crate) fn file_writer(
        &self,
        path: PathBuf,
        options: OpenOptions,
    ) -> Result<Box<dyn Write>, Error> {
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

    /// Return a reader that decompresses the input, or `None` if the input could
    /// not be decompressed. The input reader is consumed in any case, since we need
    /// to look into the data to check if decompression works.
    ///
    /// For [CompressionFormat::None], the input reader is returned unchanged.
    ///
    /// This is public so that it can be used by external resource providers.
    pub fn try_decompression<R: BufRead + 'static>(&self, read: R) -> Option<Box<dyn BufRead>> {
        match self {
            Self::None => Some(Box::new(read)),
            Self::Gzip => {
                let gz_reader = MultiGzDecoder::new(read);
                if gz_reader.header().is_some() {
                    Some(Box::new(BufReader::new(gz_reader)))
                } else {
                    None
                }
            }
        }
    }

    /// Returns the file extension to be used in files of this compression format.
    pub(crate) fn extension(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Gzip => Some("gz"),
        }
    }

    /// Ensure that the [path][PathBuf] ends with the specific extension for this compression
    /// format. Existing extentions that are different from the new extension are kept
    /// as part of the file name.
    pub(crate) fn path_with_extension(&self, path: PathBuf) -> PathBuf {
        match self.extension() {
            Some(new_ext) => path.with_extension(match path.extension() {
                Some(cur_ext) => {
                    let cur_ext = cur_ext.to_str().expect("valid UTF-8");
                    if cur_ext == new_ext {
                        cur_ext.to_string()
                    } else {
                        format!("{cur_ext}.{new_ext}")
                    }
                }
                None => new_ext.to_string(),
            }),
            None => path,
        }
    }
}

impl std::fmt::Display for CompressionFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None (no compression)"),
            Self::Gzip => write!(f, "GZip"),
        }
    }
}
