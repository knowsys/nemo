//! The engine through its public API: one module per subject, sharing
//! the helpers of [common].
//!
//! The tests that read the engine's private parts are unit tests,
//! in `src/engine/tests` under the same module names.
//!
//! The corpus of test programs runs in a binary of its own
//! (`testcases.rs`), whose harness registers one test per program.

mod common;

mod aggregates;
mod checking;
mod consolidation;
mod diagnostics;
mod documentation;
mod grammar;
mod imported_rows;
mod imports;
mod io;
mod methods;
mod multistage;
mod parameters;
mod rules;
mod sparql;
mod stages;
mod structures;
mod term_syntax;
mod tracing;
mod types;
mod values;
