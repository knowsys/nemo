//! This module defines the [CompressionFormat]s that are supported.
#![allow(missing_docs)]

use std::fmt::Display;

use enum_assoc::Assoc;

use crate::syntax::import_export::attribute;

/// Compression formats
#[derive(Assoc, Debug, Copy, Clone, PartialEq, Eq)]
#[func(pub fn name(&self) -> &'static str)]
pub enum CompressionFormat {
    /// No compression
    #[assoc(name = attribute::VALUE_COMPRESSION_NONE)]
    None,
    /// GZip compression
    #[assoc(name = attribute::VALUE_COMPRESSION_GZIP)]
    GZip,
}

impl Display for CompressionFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
