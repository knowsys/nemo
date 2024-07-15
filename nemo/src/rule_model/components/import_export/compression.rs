//! This module defines the [CompressionFormat]s that are supported.
#![allow(missing_docs)]

use std::fmt::Display;

use enum_assoc::Assoc;

use crate::rule_model::syntax::import_export::compression;

/// Compression formats
#[derive(Assoc, Debug, Copy, Clone, PartialEq, Eq)]
#[func(pub fn name(&self) -> &'static str)]
#[func(pub fn from_name(name: &str) -> Option<Self>)]
pub enum CompressionFormat {
    /// No compression
    #[assoc(name = compression::VALUE_COMPRESSION_NONE)]
    None,
    /// GZip compression
    #[assoc(name = compression::VALUE_COMPRESSION_GZIP)]
    GZip,
}

impl Display for CompressionFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
