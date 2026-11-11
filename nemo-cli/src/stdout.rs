//! Writing a run's data to standard output, where a closed pipe is not an error.

use std::io::{self, IsTerminal, Write};

use nemo_physical::diagnostics::Diagnostic;

use crate::error::CliError;

/// Writes all of `text` to standard output, and flushes it.
///
/// A closed pipe ends the write without an error.
pub(crate) fn write(text: &str) -> Result<(), Diagnostic> {
    let mut out = io::stdout().lock();
    out.write_all(text.as_bytes())
        .and_then(|()| out.flush())
        .or_else(interrupted)
}

/// Returns whether standard output and the error stream are both a terminal,
/// so that what a run writes to them appears on one screen.
pub(crate) fn shares_terminal() -> bool {
    io::stdout().is_terminal() && io::stderr().is_terminal()
}

/// Returns the outcome of a failed write to standard output.
///
/// A closed pipe means that the reader wants no more output, so it is not an error.
/// Any other failure is a [PrintFailed](CliError::PrintFailed) error.
pub(crate) fn interrupted(error: io::Error) -> Result<(), Diagnostic> {
    if error.kind() == io::ErrorKind::BrokenPipe {
        Ok(())
    } else {
        Err(CliError::PrintFailed {
            reason: error.to_string(),
        }
        .diagnostic())
    }
}
