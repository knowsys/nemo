//! Data structures for errors and warnings.
//!
//! A [Diagnostic] is one error or warning.
//! Its [Code] names the kind of problem, and its message says what happened.
//! Its [Label]s point at [Span]s of source texts, if the problem lies in one,
//! its [Field]s give named values of the problem, and its notes and hints explain it further.
//!
//! A crate declares the kinds of problem that it reports
//! with [diagnostic_kinds](crate::diagnostic_kinds),
//! as an enum with one variant per kind that implements [DiagnosticKind].
//! A [Registry] collects the documentation of the kinds by code.
//!
//! Messages follow the style of rustc:
//! lowercase, without a trailing period, with code in backticks,
//! and with mismatches written as "expected …, found …".
//! A note adds context, and a hint says what to do.

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
