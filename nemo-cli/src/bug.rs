//! Reporting a panic as an internal error.
//!
//! A panic means a bug in Nemo, not a problem with the program or its data.
//! It is shown like any other diagnostic, with a note that asks the user to report the bug,
//! and the process exits with 101, the exit code of a panic.

use std::{
    backtrace::{Backtrace, BacktraceStatus},
    panic::{self, PanicHookInfo},
};

use nemo::diagnostics::render::{Renderer, SourceTexts};
use nemo_physical::diagnostics::Diagnostic;

/// Installs the panic hook that reports a panic as an internal error.
///
/// The hook only writes the report to the error stream, and the panicking thread still
/// unwinds.
pub(crate) fn report_panics() {
    panic::set_hook(Box::new(|info| eprint!("{}", report(info))));
}

/// Returns the report of a panic:
/// the rendered internal error, followed by a backtrace if `RUST_BACKTRACE` enables one.
fn report(info: &PanicHookInfo<'_>) -> String {
    let backtrace = Backtrace::capture();
    let shown = backtrace.status() == BacktraceStatus::Captured;
    let diagnostic = internal_error(
        info.payload_as_str().unwrap_or("a panic without a message"),
        info.location().map(ToString::to_string),
        shown,
    );

    let mut report = Renderer::new(&SourceTexts::new()).show(&diagnostic);
    if shown {
        report.push_str(&format!("{backtrace}\n"));
    }

    report
}

/// Returns the internal error that reports a panic with `message`.
///
/// `location` is the place in Nemo's source where it panicked, if it is known,
/// and `backtrace` is whether a backtrace is shown.
/// Without a backtrace, a hint says how to get one.
fn internal_error(message: &str, location: Option<String>, backtrace: bool) -> Diagnostic {
    let mut diagnostic = Diagnostic::internal(message);
    if let Some(location) = location {
        diagnostic = diagnostic.with_note(format!("Nemo panicked at {location}"));
    }
    if !backtrace {
        diagnostic = diagnostic.with_hint("rerun with `RUST_BACKTRACE=1` to show a backtrace");
    }

    diagnostic
}

#[cfg(test)]
mod tests {
    use super::*;

    use nemo_physical::diagnostics::Code;

    /// A panic is reported as an internal error with its message, its location,
    /// and a hint on getting a backtrace, which is left out if a backtrace is shown.
    #[test]
    fn panics_report_as_internal_errors() {
        let diagnostic = internal_error(
            "unknown or dropped table id",
            Some("nemo-physical/src/management.rs:1:1".to_owned()),
            false,
        );
        assert_eq!(diagnostic.code(), Code::INTERNAL);

        let shown = Renderer::plain(&SourceTexts::new()).show(&diagnostic);
        assert!(shown.contains("unknown or dropped table id"), "{shown}");
        assert!(shown.contains("please report it"), "{shown}");
        assert!(
            shown.contains("panicked at nemo-physical/src/management.rs:1:1"),
            "{shown}"
        );
        assert!(shown.contains("RUST_BACKTRACE=1"), "{shown}");

        // With a backtrace shown, there is no hint.
        let traced = internal_error("boom", None, true);
        assert!(traced.hints().is_empty());
    }
}
