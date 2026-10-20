//! The exception hierarchy, and diagnostics as Python data.
//!
//! A failure of the engine raises `NemoError` or a subclass,
//! a value that does not convert raises `ValueError` or `TypeError`,
//! and a panic, a bug in Nemo, raises pyo3's `PanicException`.
//! Diagnostics cross as structured [Diagnostic] values
//! (code, severity, message, resolved locations),
//! with the terminal rendering as their string form,
//! so that nothing is flattened to an opaque message.

use pyo3::{create_exception, exceptions::PyValueError, prelude::*};

use nemo::diagnostics::render::SourceTexts;

create_exception!(
    nmo,
    NemoError,
    pyo3::exceptions::PyException,
    "The base of every error the engine raises."
);

create_exception!(
    nmo,
    DiagnosticsError,
    NemoError,
    "A failure with structured diagnostics: `diagnostics` holds them."
);

/// Returns a `ValueError` for a value that does not convert.
pub(crate) fn value_error(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

/// One resolved label of a diagnostic:
/// its message, and its location if it is known.
#[pyclass(frozen, get_all, skip_from_py_object, module = "nmo")]
#[derive(Debug, Clone)]
pub struct Label {
    /// The label's message.
    pub message: String,
    /// The name of the source the span points into, if known.
    pub source: Option<String>,
    /// The 1-based line, if the span could be located.
    pub line: Option<u32>,
    /// The 1-based column (in characters), if the span could be located.
    pub column: Option<u32>,
}

#[pymethods]
impl Label {
    fn __repr__(&self) -> String {
        match (&self.source, self.line, self.column) {
            (Some(source), Some(line), Some(column)) => {
                format!("Label({:?}, {source}:{line}:{column})", self.message)
            }
            _ => format!("Label({:?})", self.message),
        }
    }
}

/// One diagnostic, as plain data: code, severity, message, labels, notes, hints,
/// and the rendered terminal form as its string.
#[pyclass(frozen, get_all, skip_from_py_object, module = "nmo")]
#[derive(Debug, Clone)]
pub struct Diagnostic {
    /// The stable code, like `"N0204"`.
    pub code: String,
    /// `"error"` or `"warning"`.
    pub severity: String,
    /// The headline message.
    pub message: String,
    /// The labels, resolved against the run's sources.
    pub labels: Vec<Label>,
    /// Additional notes.
    pub notes: Vec<String>,
    /// Hints toward a fix.
    pub hints: Vec<String>,
    /// The full rendering (snippets included), plain.
    pub rendered: String,
}

#[pymethods]
impl Diagnostic {
    fn __str__(&self) -> &str {
        &self.rendered
    }

    fn __repr__(&self) -> String {
        format!("Diagnostic({}, {:?})", self.code, self.message)
    }
}

/// Resolves a diagnostic against the sources of its run.
pub(crate) fn resolve(
    diagnostic: &nemo::diagnostics::Diagnostic,
    sources: &SourceTexts,
) -> Diagnostic {
    let resolved = nemo::diagnostics::render::ResolvedDiagnostic::new(diagnostic, sources);
    Diagnostic {
        code: resolved.code,
        severity: resolved.severity.to_string(),
        message: resolved.message,
        labels: resolved
            .labels
            .into_iter()
            .map(|label| Label {
                message: label.message,
                source: label.source,
                line: label.line,
                column: label.column,
            })
            .collect(),
        notes: resolved.notes,
        hints: resolved.hints,
        rendered: resolved.rendered,
    }
}

/// Raises the diagnostics of a failed call: their renderings form the
/// message, and all of them are attached as `diagnostics`.
pub(crate) fn raise(
    py: Python<'_>,
    diagnostics: Vec<nemo::diagnostics::Diagnostic>,
    sources: &SourceTexts,
) -> PyErr {
    let resolved: Vec<Diagnostic> = diagnostics
        .iter()
        .map(|diagnostic| resolve(diagnostic, sources))
        .collect();
    let message = resolved
        .iter()
        .map(|diagnostic| diagnostic.rendered.trim_end())
        .collect::<Vec<_>>()
        .join("\n");
    let error = DiagnosticsError::new_err(message);
    if let Err(attach) = (|| -> PyResult<()> {
        error.value(py).setattr("diagnostics", resolved)?;
        Ok(())
    })() {
        return attach;
    }
    error
}
