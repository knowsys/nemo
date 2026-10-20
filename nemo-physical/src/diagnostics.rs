//! Datastructures for errors and warnings.
//!
//! A [Diagnostic] represents one error or warning:
//! its [Code] names the kind of problem,
//! its message says what happened,
//! [Span]s point into source texts, if the problem lies in one,
//! and [Field]s hold additional context information.
//! Notes and hints explain the problem further.
//!
//! Each crate declares the kinds of problem it reports
//! with [diagnostic_kinds](crate::diagnostic_kinds):
//! an enum with one variant per kind,
//! implementing [DiagnosticKind].
//! A [Registry] documents the diagnostics that the crate reports,
//! built from the registries below it and the kinds it declares.
//!
//! Messages follow the style of rustc:
//! lowercase, without a trailing period, code in backticks,
//! and mismatches as "expected …, found …".
//! A note adds context, a hint says what to do.

mod code;
mod count;
mod diagnostic;
mod kind;
mod macros;
mod registry;
mod span;

pub use code::Code;
pub use count::Count;
pub use diagnostic::{Diagnostic, Field, Label, Severity};
pub use kind::DiagnosticKind;
pub use registry::{CodeEntry, REGISTRY, Registry};
pub use span::{FileId, Span};
