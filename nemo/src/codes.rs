//! The diagnostic codes of the logical layer,
//! and the registry that documents every code an engine can report.
//!
//! Codes are stable, public identifiers, so that tools and tests can match on them.
//! The `# Errors` sections of the public API name them.
//! Each kind has one constant, named after the kind.
//! Where two stages have kinds of the same name, the constant of one of them also names its
//! stage, as in `MODULES_TOO_DEEP`.
//! The numbers are grouped by stage:
//! - parsing: from `N0210`, and `N0202`;
//! - translation: from `N0250`, and `N0203` and `N0204`;
//! - modules: from `N0400`;
//! - types: from `N0450`, and `N0205`;
//! - the engine: from `N0480`, and `N0206` to `N0208`;
//! - tracing: from `N0500`, and `N0209`.
//!
//! The lower numbers belong to the IO layer (`N01xx`) and the physical layer (`N0001` and
//! `N0002`), and `N09xx` is left to the front ends.

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

/// The registry of every code an engine can report.
///
/// It contains the codes of the IO and physical layers, and those of this crate.
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

    /// Every code has an explanation without runs of spaces,
    /// and the registry finds the codes of every layer.
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
            // A line break in a string literal without a `\` before it keeps the indentation
            // of the next line.
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
