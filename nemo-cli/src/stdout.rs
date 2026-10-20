//! The standard output, the data stream of a run,
//! and what a failed write to it amounts to.

use std::io::{self, Write};

use nemo_physical::diagnostics::Diagnostic;

use crate::error::CliError;

/// Writes a text to the standard output, whole.
///
/// A closed pipe ends it quietly.
pub(crate) fn write(text: &str) -> Result<(), Diagnostic> {
    let mut out = io::stdout().lock();
    out.write_all(text.as_bytes())
        .and_then(|()| out.flush())
        .or_else(interrupted)
}

/// Returns what a failed write to the standard output amounts to: a closed pipe
/// means the reader has all it wants, anything else is reported.
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
