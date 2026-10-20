//! The diagnostic codes of the IO layer, and the registry of every code it reports.
//!
//! There is one constant per kind of `IoError`, in the `N01xx` range.

use std::sync::LazyLock;

use nemo_physical::diagnostics::Registry;

use crate::error::IoError;

pub use crate::error::codes::*;

/// The registry of every code the IO layer reports,
/// the physical layer's included.
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    Registry::new()
        .with_registry(&nemo_physical::diagnostics::REGISTRY)
        .with_kinds::<IoError>()
});
