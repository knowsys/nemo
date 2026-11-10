//! The diagnostics that the engine's public API reports.
//!
//! The diagnostic types of the physical and IO layers are re-exported here,
//! so that a dependency on this crate suffices to handle them.
//! The codes are constants in [codes](crate::codes),
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
//! Within the crate, problems are declared as *kinds*.
//! Each stage has one enum of kinds,
//! and each variant is a kind with the data that its message needs.
//! Every diagnostic is built from a kind.
//! Many kinds share two fields of a diagnostic:
//! - the [resource](AboutResource) that a problem is about, defined by the IO layer;
//! - the [predicate](AboutPredicate) that a problem is about, defined here.

pub(crate) mod engine;
pub(crate) mod modules;
pub(crate) mod parse;
pub mod render;
pub(crate) mod trace;
pub(crate) mod translation;
pub(crate) mod types;

pub use nemo_io::AboutResource;
pub use nemo_physical::diagnostics::{
    Code, CodeEntry, Diagnostic, DiagnosticKind, Field, FileId, Label, Location, Registry,
    Severity, Span,
};

pub(crate) use self::{
    engine::EngineError, modules::ModuleError, parse::ParseError, trace::TraceError,
    translation::TranslationError, types::TypeError,
};

/// The name of the diagnostic field that names the predicate a problem is about.
const PREDICATE: &str = "predicate";

/// Diagnostics that name the predicate they are about.
///
/// The predicate is the diagnostic's field `predicate`.
/// Either the diagnostic's kind declares that field,
/// or [with_predicate](Self::with_predicate) adds it, for example to a diagnostic of a lower
/// layer.
pub trait AboutPredicate: Sized {
    /// Names the predicate the problem is about.
    fn with_predicate(self, predicate: impl Into<String>) -> Self;

    /// Returns the predicate the problem is about, or `None` if none is named.
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
