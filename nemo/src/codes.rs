//! The diagnostic codes of the logical layer,
//! and the registry documenting every code an engine can report.
//!
//! Codes are stable, public identifiers: tooling and tests match on them,
//! and documentation references them.
//! There is one constant per kind, named after it,
//! with the stage added where two stages have a kind of the same name (`MODULES_TOO_DEEP`).
//! The numbers group by stage:
//! - parsing from `N0210`, and `N0202`,
//! - translation from `N0250`, and `N0203` and `N0204`,
//! - modules from `N0400`,
//! - types from `N0450`, and `N0205`,
//! - the engine from `N0480`, and `N0206` to `N0208`,
//! - tracing from `N0500`, and `N0209`.
//!
//! The IO layer's `N01xx` and the physical layer's `N0001` and `N0002` lie below,
//! and the command line's `N09xx` above.

use std::sync::LazyLock;

use nemo_physical::diagnostics::Registry;

use crate::diagnostics::{
    EngineError, ModuleError, ParseError, TraceError, TranslationError, TypeError,
};

pub use crate::diagnostics::{
    engine::codes::*, modules::codes::*, parse::codes::*, trace::codes::*, translation::codes::*,
    types::codes::*,
};
pub use nemo_io::codes::*;

/// The registry of every code an engine can report:
/// the codes of the IO and physical layers, and those of this crate.
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    Registry::new()
        .with_registry(&nemo_io::codes::REGISTRY)
        .with_kinds::<ParseError>()
        .with_kinds::<TranslationError>()
        .with_kinds::<ModuleError>()
        .with_kinds::<TypeError>()
        .with_kinds::<EngineError>()
        .with_kinds::<TraceError>()
});

#[cfg(test)]
mod tests {
    use super::*;

    use nemo_physical::diagnostics::Code;

    /// Every code has an explanation, and the registry finds the codes of every layer.
    ///
    /// Building the registry already rejects a code documented twice.
    #[test]
    fn every_code_is_explained() {
        for entry in REGISTRY.entries() {
            assert!(
                !entry.explanation.is_empty(),
                "{} ({}) has no explanation",
                entry.code,
                entry.name
            );
            // A line break in the literal without a `\` keeps the next line's indentation.
            assert!(
                !entry.explanation.contains("  "),
                "{} ({}) has a run of spaces in its explanation",
                entry.code,
                entry.name
            );
        }
        assert_eq!(
            REGISTRY.explain(EXPECTED).map(|entry| entry.name),
            Some("Expected")
        );
        assert_eq!(
            REGISTRY.explain(UNREADABLE).map(|entry| entry.name),
            Some("Unreadable")
        );
        assert_eq!(
            REGISTRY.explain(Code::INTERNAL).map(|entry| entry.name),
            Some("Internal")
        );
        assert_eq!(REGISTRY.explain(Code::new(9999)), None);
    }
}
