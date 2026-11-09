//! The command line interface of the Nemo rule engine.
//!
//! `nmo` runs rule programs.
//! It reads them, loads what they import, reasons to the fixpoint, writes what they export,
//! and reports what happened.
//! It reports every problem with a program, such as a parse error, an unsafe rule or a dropped
//! row, as a [diagnostic](nemo_physical::diagnostics) shown against the program's source.

mod bug;
mod dashboard;
mod error;
mod explain;
mod logging;
mod run;
mod stdout;

use std::{process::ExitCode, thread};

use clap::Parser;
// The arguments are defined in the library, and this import lets the binary's modules
// refer to them as `crate::arguments`.
use nemo_cli::arguments;

use crate::{arguments::Arguments, run::Run};

/// The stack size of the run's thread, in bytes.
///
/// Tracing recurses once per level of a proof, so a long chain of derivations needs a deep
/// stack, deeper than the main thread's stack of one megabyte on Windows.
/// The reservation is only address space, and memory is used only as deep as the run goes.
const STACK_BYTES: usize = 1 << 30;

/// Runs the program the command line describes.
fn main() -> ExitCode {
    bug::report_panics();
    let arguments = Arguments::parse();
    thread::Builder::new()
        .stack_size(STACK_BYTES)
        .spawn(move || Run::new(&arguments).execute())
        .expect("the run's thread starts")
        .join()
        // The panic hook has reported the panic, so exit with the exit code of a panic.
        .unwrap_or(ExitCode::from(101))
}
