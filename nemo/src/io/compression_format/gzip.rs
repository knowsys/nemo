use std::io::{Read, Write};

use flate2::{read::MultiGzDecoder, write::GzEncoder, Compression};
use nemo_physical::error::{ReadingError, ReadingErrorKind};

use crate::syntax::import_export::attribute::VALUE_COMPRESSION_GZIP;

use super::{CompressionImpl, GZIP_COMPRESSION_LEVEL};

#[derive(Debug, Clone, Copy)]
pub struct Gzip {
    compression_level: Compression,
}

impl Default for Gzip {
    fn default() -> Self {
        Self {
            compression_level: GZIP_COMPRESSION_LEVEL,
        }
    }
}

impl CompressionImpl for Gzip {
    fn decompress(&self, stream: Box<dyn Read>) -> Result<Box<dyn Read>, ReadingError> {
        let decoder = MultiGzDecoder::new(stream);

        if decoder.header().is_none() {
            return Err(ReadingError::new(ReadingErrorKind::Decompression {
                decompression_format: VALUE_COMPRESSION_GZIP.to_owned(),
            }));
        }

        Ok(Box::new(decoder))
    }

    fn compress(&self, stream: Box<dyn Write>) -> Box<dyn Write> {
        Box::new(GzEncoder::new(stream, self.compression_level))
    }
}
