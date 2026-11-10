//! The exceptions of the `nmo` module, and diagnostics as Python data.
//!
//! - A failure of the engine raises `NemoError` or a subclass.
//! - A value that does not convert raises `ValueError` or `TypeError`.
//! - A panic, which is a bug in Nemo, raises pyo3's `PanicException`.
//!
//! Diagnostics are passed to Python as structured [Diagnostic] values,
//! with their code, severity, message and the places they point at,
//! and the string form of a diagnostic is its rendering as a terminal shows it.

use pyo3::{create_exception, exceptions::PyValueError, prelude::*};

use nemo::diagnostics::render::{Renderer, SourceTexts, notes};

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

/// Returns a `ValueError` with `message`, for a value that does not convert.
pub(crate) fn value_error(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

/// A label of a diagnostic: its message, and the place it points at if that is known.
#[pyclass(frozen, get_all, skip_from_py_object, module = "nmo")]
#[derive(Debug, Clone)]
pub struct Label {
    /// The label's message.
    pub message: String,
    /// The name of the source that the label points into, if it is known.
    pub source: Option<String>,
    /// The line, counted from 1, if the place is known.
    pub line: Option<u32>,
    /// The column, in characters and counted from 1, if the place is known.
    pub column: Option<u32>,
}

#[pymethods]
impl Label {
    /// Returns the label as `Label("message", source:line:column)`,
    /// or as `Label("message")` if its place is not known.
    fn __repr__(&self) -> String {
        match (&self.source, self.line, self.column) {
            (Some(source), Some(line), Some(column)) => {
                format!("Label({:?}, {source}:{line}:{column})", self.message)
            }
            _ => format!("Label({:?})", self.message),
        }
    }
}

/// A diagnostic as plain data.
///
/// Its string form is its full rendering, as a terminal shows it.
#[pyclass(frozen, get_all, skip_from_py_object, module = "nmo")]
#[derive(Debug, Clone)]
pub struct Diagnostic {
    /// The stable code, like `"N0204"`.
    pub code: String,
    /// The severity, `"error"` or `"warning"`.
    pub severity: String,
    /// The main message.
    pub message: String,
    /// The labels, with the places they point at.
    pub labels: Vec<Label>,
    /// Additional notes.
    pub notes: Vec<String>,
    /// Hints on how to fix the problem.
    pub hints: Vec<String>,
    /// The full rendering, with source snippets and without colors.
    pub rendered: String,
}

#[pymethods]
impl Diagnostic {
    /// Returns the full rendering.
    fn __str__(&self) -> &str {
        &self.rendered
    }

    /// Returns the diagnostic as `Diagnostic(code, "message")`.
    fn __repr__(&self) -> String {
        format!("Diagnostic({}, {:?})", self.code, self.message)
    }
}

/// Returns a diagnostic as Python data, with its rendering against `sources`.
///
/// The place of a label is the location that the label has.
pub(crate) fn resolve(
    diagnostic: &nemo::diagnostics::Diagnostic,
    sources: &SourceTexts,
) -> Diagnostic {
    Diagnostic {
        code: diagnostic.code().to_string(),
        severity: diagnostic.severity().to_string(),
        message: diagnostic.message().to_owned(),
        labels: diagnostic
            .labels()
            .iter()
            .map(|label| {
                let location = label.location.as_ref();
                Label {
                    message: label.message.clone(),
                    source: location
                        .and_then(|location| location.source.as_deref())
                        .map(str::to_owned),
                    line: location.map(|location| location.line),
                    column: location.map(|location| location.column),
                }
            })
            .collect(),
        notes: notes(diagnostic),
        hints: diagnostic.hints().to_vec(),
        rendered: Renderer::plain(sources).show(diagnostic),
    }
}

/// Returns a diagnostic whose hints name only what the Python module has.
///
/// The module has no `resume`, and a run that is suspended for data from the network
/// continues with `reason()`.
/// So the hint of [METHOD_SUSPENDED](nemo::codes::METHOD_SUSPENDED) is replaced.
/// The hint of [PARAMETER_WITHOUT_VALUE](nemo::codes::PARAMETER_WITHOUT_VALUE) also names
/// the command line, so it is replaced as well.
fn with_python_hints(diagnostic: nemo::diagnostics::Diagnostic) -> nemo::diagnostics::Diagnostic {
    let code = diagnostic.code();
    if code == nemo::codes::METHOD_SUSPENDED {
        diagnostic
            .without_hints()
            .with_hint("call `reason()` to continue the run, or `discard_pending()` to drop it")
    } else if code == nemo::codes::PARAMETER_WITHOUT_VALUE {
        diagnostic
            .without_hints()
            .with_hint("give it a value with `set_parameter`")
    } else {
        diagnostic
    }
}

/// Returns the `DiagnosticsError` for the diagnostics of a failed call.
///
/// The error's message joins the renderings of the diagnostics,
/// and its `diagnostics` attribute lists them.
/// Hints that name engine methods the module lacks are replaced, as [with_python_hints] does.
/// If the attribute cannot be set, the error of setting it is returned instead.
pub(crate) fn raise(
    py: Python<'_>,
    diagnostics: Vec<nemo::diagnostics::Diagnostic>,
    sources: &SourceTexts,
) -> PyErr {
    let resolved: Vec<Diagnostic> = diagnostics
        .into_iter()
        .map(|diagnostic| resolve(&with_python_hints(diagnostic), sources))
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
