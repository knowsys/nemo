//! The engine through its public API: one module per subject, sharing
//! the helpers of [common].
//!
//! The corpus of test programs runs in a binary of its own
//! (`testcases.rs`), whose harness registers one test per program.

mod common;

mod consolidation;
mod grammar;
mod io;
mod multistage;
#[cfg(feature = "sparql")]
mod sparql;
mod stages;
mod tracing;
mod values;
