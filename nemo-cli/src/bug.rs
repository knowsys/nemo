//! The report of a bug in Nemo: a panic, shown as an internal error.
//!
//! A panic means that Nemo broke a promise of its own, not that the program
//! or its data are wrong. The run reports it like any other diagnostic,
//! with the note asking the user to report the bug, and exits with 101,
//! the exit code of a panic.

use std::{
    backtrace::{Backtrace, BacktraceStatus},
    panic::{self, PanicHookInfo},
};

use nemo::diagnostics::render::{Renderer, SourceTexts};
use nemo_physical::diagnostics::Diagnostic;

/// Installs the panic hook that reports a panic as an internal error.
///
/// The run's thread still unwinds, and the process exits with 101.
pub(crate) fn report_panics() {
    panic::set_hook(Box::new(|info| eprint!("{}", report(info))));
}

/// Returns the report of a panic:
/// the rendered internal error, followed by a backtrace if `RUST_BACKTRACE` asks for one.
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

/// Returns the internal error reporting a panic with the given message,
/// at the given source location of Nemo if it is known.
///
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

        // With a backtrace shown, the hint goes.
        let traced = internal_error("boom", None, true);
        assert!(traced.hints().is_empty());
    }
}
