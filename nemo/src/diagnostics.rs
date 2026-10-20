//! The diagnostics the engine's public API reports.
//!
//! The diagnostic types of the physical and IO layers are re-exported here,
//! so a dependency on this crate suffices to handle them.
//! Codes are listed per layer in [codes](crate::codes) and its physical and IO counterparts,
//! and [codes::REGISTRY](crate::codes::REGISTRY) documents every code an engine reports.
//! [render] renders diagnostics as text for people.
//!
//! ```
//! use nemo::diagnostics::{Diagnostic, FileId, Severity, Span};
//!
//! let here = Span { file: FileId::new(0), start: 3, end: 4 };
//! let report = Diagnostic::error(nemo::codes::EXPECTED, "expected a term")
//!     .with_label(here, "here");
//! assert_eq!(report.severity(), Severity::Error);
//! ```
//!
//! Within the crate, problems are declared as kinds:
//! one enum per stage, each variant a kind with the data its message needs,
//! and every diagnostic is built from one.
//! Two diagnostic fields are shared by many kinds:
//! the [resource](AboutResource) a problem is about, defined by the IO layer,
//! and the [predicate](AboutPredicate) a problem is about, defined here.

pub(crate) mod engine;
pub(crate) mod modules;
pub(crate) mod parse;
pub mod render;
pub(crate) mod trace;
pub(crate) mod translation;
pub(crate) mod types;

pub use nemo_io::AboutResource;
pub use nemo_physical::diagnostics::{
    Code, CodeEntry, Diagnostic, DiagnosticKind, Field, FileId, Label, Registry, Severity, Span,
};

pub(crate) use self::{
    engine::EngineError, modules::ModuleError, parse::ParseError, trace::TraceError,
    translation::TranslationError, types::TypeError,
};

/// The name of the field naming the predicate a problem is about.
const PREDICATE: &str = "predicate";

/// Diagnostics that name the predicate they are about.
///
/// The predicate is the field named `predicate`,
/// declared on a kind, or added to a diagnostic raised by a lower layer.
pub trait AboutPredicate: Sized {
    /// Names the predicate the problem is about.
    fn with_predicate(self, predicate: impl Into<String>) -> Self;

    /// Returns the predicate the problem is about, if named.
    fn predicate(&self) -> Option<&str>;
}

impl AboutPredicate for Diagnostic {
    fn with_predicate(self, predicate: impl Into<String>) -> Self {
        self.with_field(PREDICATE, predicate)
    }

    fn predicate(&self) -> Option<&str> {
        self.field(PREDICATE)
    }
}
