//! This module defines

use std::hash::Hash;

pub(crate) type ExternalReference = usize;

/// Origin of a program component
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Origin {
    /// Component was created via a constructor
    Created,
    /// Component was created due translation from an external input, e.g., parsing
    External(ExternalReference),
}

impl Default for Origin {
    fn default() -> Self {
        Self::Created
    }
}
