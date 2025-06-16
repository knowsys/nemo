//! Handling of compression during import and export.

use std::{
    fmt::Display,
    io::{Read, Write},
};

use flate2::Compression;

use gzip::Gzip;
use nemo_physical::{
    error::ReadingError,
    resource::{Resource, ResourceBuilder},
};

use crate::syntax::import_export::{attribute, file_format};

use enum_assoc::Assoc;

pub(crate) mod gzip;

/// Compression formats
#[derive(Assoc, Debug, Copy, Clone, PartialEq, Eq, Default)]
#[func(pub(crate) fn name(&self) -> &'static str)]
#[func(pub(crate) fn from_name(name: &str) -> Option<Self>)]
#[func(pub(crate) fn extension(&self) -> Option<&str>)]
#[func(pub(crate) fn media_type_addition(&self) -> Option<&str>)]
#[func(pub(crate) fn implementation(&self) -> Box<dyn CompressionImpl>)]
pub enum CompressionFormat {
    /// No compression
    #[default]
    #[assoc(name = attribute::VALUE_COMPRESSION_NONE)]
    #[assoc(implementation = Box::new(PassThrough))]
    None,
    /// GZip compression
    #[assoc(name = attribute::VALUE_COMPRESSION_GZIP)]
    #[assoc(from_name = attribute::VALUE_COMPRESSION_GZIP)]
    #[assoc(extension = file_format::EXTENSION_GZ)]
    #[assoc(media_type_addition = "gzip")]
    #[assoc(implementation = Box::new(Gzip::default()))]
    GZip,
}

impl Display for CompressionFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Compression level for gzip output, cf. gzip(1):
///
/// > Regulate the speed of compression using the specified digit #,
/// > where -1 or --fast indicates the fastest compression method (less
/// > compression) and -9 or --best indicates the slowest compression
/// > method (best compression).  The default compression level is -6
/// > (that is, biased towards high compression at expense of speed).
const GZIP_COMPRESSION_LEVEL: Compression = Compression::new(6);

impl CompressionFormat {
    fn from_file_extension(file_extension: Option<&str>) -> Self {
        match file_extension {
            Some(file_format::EXTENSION_GZ) => Self::GZip,
            _ => Self::None,
        }
    }

    /// Derive a compression format from the file extension of the
    /// given resource.
    pub fn from_resource(resource: &Resource) -> Self {
        Self::from_file_extension(resource.file_extension().as_deref())
    }

    /// Derive a compression format from the file extension of the
    /// given resource builder.
    pub fn from_resource_builder(builder: &Option<ResourceBuilder>) -> Self {
        let Some(builder) = builder else {
            return Self::None;
        };

        if !builder.supports_compression() {
            return Self::None;
        }

        Self::from_file_extension(builder.file_extension().as_deref())
    }

    /// Return the file extension of the uncompressed file obtained
    /// from the given resource.
    pub fn uncompressed_file_extension(&self, resource: &Resource) -> Option<String> {
        resource.file_extension_uncompressed(self.extension())
    }
}

/// Implementation of a compression format
pub trait CompressionImpl {
    /// Returns a stream which applies decompression to the input.
    fn decompress(&self, stream: Box<dyn Read>) -> Result<Box<dyn Read>, ReadingError>;

    /// Returns a stream which applies compression to the output.
    fn compress(&self, stream: Box<dyn Write>) -> Box<dyn Write>;
}

struct PassThrough;

impl CompressionImpl for PassThrough {
    fn decompress(&self, stream: Box<dyn Read>) -> Result<Box<dyn Read>, ReadingError> {
        Ok(stream)
    }

    fn compress(&self, stream: Box<dyn Write>) -> Box<dyn Write> {
        stream
    }
}
