//! `--explain CODE`: what a diagnostic code means.

use std::sync::LazyLock;

use nemo::diagnostics::render::TerminalStyle;
use nemo_physical::diagnostics::{Code, Diagnostic, Registry};

use crate::{error::CliError, stdout};

/// The registry of every code the command line reports: the engine's
/// and its own.
static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    Registry::new()
        .with_registry(&nemo::codes::REGISTRY)
        .with_kinds::<CliError>()
});

/// Prints the registry entry of a code to the standard output:
/// its name, its message template, the explanation written on its declaration,
/// and the note and hint its diagnostics carry.
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
    // The summary is a message template, its literal braces doubled.
    let summary = entry.summary.replace("{{", "{").replace("}}", "}");
    let mut text = format!("{strong}{}{reset} {}\n{summary}\n", entry.code, entry.name);
    if !entry.explanation.is_empty() {
        text.push('\n');
        text.push_str(entry.explanation);
        text.push('\n');
    }
    // Labelled as the rendered diagnostics label them.
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

    /// The command line's codes share one numbering with the engine's:
    /// building the registry rejects a code documented twice, and every
    /// kind explains itself.
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
