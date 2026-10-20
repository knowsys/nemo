//! The command line interface of the Nemo rule engine.
//!
//! `nmo` runs rule programs:
//! it reads them, loads what they import, reasons to the fixpoint, writes what they export,
//! and reports what happened.
//! Everything it says about a program (parse errors, unsafe rules, dropped rows)
//! is a [diagnostic](nemo_physical::diagnostics) rendered against the source it came from.

mod bug;
mod dashboard;
mod error;
mod explain;
mod logging;
mod run;
mod stdout;

use std::{process::ExitCode, thread};

use clap::Parser;
// The library's, so that the modules here reach it as `crate::arguments`.
use nemo_cli::arguments;

use crate::{arguments::Arguments, run::Run};

/// The stack the run's thread reserves.
///
/// Tracing recurses once per level of a proof, and a proof is as deep as its derivation,
/// which the main thread's stack (a megabyte on Windows) does not hold for long chains.
/// The reservation is address space, used only as deep as the run goes.
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
        // The hook reported the panic: exit as a panic does.
        .unwrap_or(ExitCode::from(101))
}
