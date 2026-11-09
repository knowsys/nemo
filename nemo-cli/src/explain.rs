//! Printing the explanation of a diagnostic code, for `--explain CODE`.

use std::sync::LazyLock;

use nemo::diagnostics::render::TerminalStyle;
use nemo_physical::diagnostics::{Code, Diagnostic, Registry};

use crate::{error::CliError, stdout};

/// The registry of every code that the command line reports, both the engine's and its own.
static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    Registry::new()
        .with_registry(&nemo::codes::REGISTRY)
        .with_kinds::<CliError>()
});

/// Prints the registry entry of `code` to standard output.
///
/// The entry consists of the code's name, its message template, its explanation,
/// and the note and hint of its diagnostics.
/// The code is written as `N0204`, and a lowercase `n` is accepted too.
/// Fails for an unknown code, or if standard output cannot be written.
pub(crate) fn print(code: &str) -> Result<(), Diagnostic> {
    let unknown = || {
        CliError::UnknownCode {
            code: code.to_owned(),
        }
        .diagnostic()
    };
    let number = code
        .trim()
        .strip_prefix('N')
        .or_else(|| code.trim().strip_prefix('n'))
        .and_then(|digits| digits.parse::<u16>().ok())
        .ok_or_else(|| {
            unknown().with_hint("write a code as `N` followed by four digits, as `N0204`")
        })?;
    let entry = REGISTRY.explain(Code::new(number)).ok_or_else(unknown)?;
    let TerminalStyle {
        strong,
        accent,
        reset,
        ..
    } = TerminalStyle::detect();
    // The summary is a message template, in which literal braces are doubled.
    let summary = entry.summary.replace("{{", "{").replace("}}", "}");
    let mut text = format!("{strong}{}{reset} {}\n{summary}\n", entry.code, entry.name);
    if !entry.explanation.is_empty() {
        text.push('\n');
        text.push_str(entry.explanation);
        text.push('\n');
    }
    // The note and hint are labelled as in rendered diagnostics.
    if entry.note.is_some() || entry.hint.is_some() {
        text.push('\n');
    }
    if let Some(note) = entry.note {
        text.push_str(&format!("{accent}Note:{reset} {note}\n"));
    }
    if let Some(hint) = entry.hint {
        text.push_str(&format!("{accent}Help:{reset} {hint}\n"));
    }
    stdout::write(&text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::codes;

    /// Every code of the command line and the engine has an explanation.
    ///
    /// Building the registry also checks that the two share one numbering,
    /// since it panics for a code that names two different kinds.
    #[test]
    fn every_code_is_explained() {
        for entry in REGISTRY.entries() {
            assert!(
                !entry.explanation.is_empty(),
                "{} ({}) has no explanation",
                entry.code,
                entry.name
            );
        }
    }

    /// The registry finds the codes of the command line and of the engine,
    /// and no code that is not declared.
    #[test]
    fn codes_of_every_layer_are_found() {
        assert_eq!(
            REGISTRY
                .explain(codes::UNKNOWN_CODE)
                .map(|entry| entry.name),
            Some("UnknownCode")
        );
        assert_eq!(
            REGISTRY.explain(Code::new(204)).map(|entry| entry.name),
            Some("UnsafeVariable")
        );
        assert_eq!(REGISTRY.explain(Code::new(9999)), None);
    }
}
