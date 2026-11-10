//! Parsed programs, which can be inspected without an engine.

use pyo3::prelude::*;

use nemo::diagnostics::render::SourceTexts;

use crate::errors::{Diagnostic, resolve};

/// A parsed program, with its text and the diagnostics of parsing.
///
/// Parsing never fails: a statement that does not parse becomes a diagnostic,
/// and the other statements form the program.
/// Inspecting a program needs no engine and reads no data.
/// An engine that loads the program parses its text again under the engine's own file id,
/// so that later diagnostics point into the right source.
#[pyclass(frozen, module = "nmo")]
#[derive(Debug)]
pub struct Program {
    /// The source text.
    pub text: String,
    /// The name that diagnostics show for the program.
    pub name: String,
    /// The parsed program, whose spans are in file 0.
    pub model: nemo::model::Program,
    /// The diagnostics of parsing.
    pub diagnostics: Vec<Diagnostic>,
}

#[pymethods]
impl Program {
    /// Parses a program file, named after its path, as `parse` does.
    ///
    /// Raises `OSError` if the file cannot be read.
    #[staticmethod]
    pub fn parse_file(path: &str) -> PyResult<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(Self::parse(text, path))
    }

    /// Parses program text, which diagnostics refer to as `name`.
    ///
    /// It never raises: `diagnostics` lists what did not parse,
    /// and `ok` says whether parsing found no errors.
    #[staticmethod]
    #[pyo3(signature = (text, name = "<program>"))]
    pub fn parse(text: String, name: &str) -> Self {
        let (model, diagnostics) = nemo::model::Program::parse(&text);
        let sources = SourceTexts::detached(name, text.clone());
        let diagnostics = diagnostics
            .iter()
            .map(|diagnostic| resolve(diagnostic, &sources))
            .collect();
        Self {
            text,
            name: name.to_owned(),
            model,
            diagnostics,
        }
    }

    /// Returns the diagnostics of parsing, both errors and warnings.
    #[getter]
    fn diagnostics(&self) -> Vec<Diagnostic> {
        self.diagnostics.clone()
    }

    /// Returns whether the whole text parsed without errors.
    #[getter]
    fn ok(&self) -> bool {
        self.diagnostics
            .iter()
            .all(|diagnostic| diagnostic.severity != "error")
    }

    /// Returns the predicates that the program's atoms name, each with its number of columns,
    /// sorted.
    ///
    /// A predicate written with two numbers of columns is listed twice.
    #[getter]
    fn predicates(&self) -> Vec<(String, usize)> {
        self.model.predicates()
    }

    /// Returns the number of statements that parsed.
    fn __len__(&self) -> usize {
        self.model.statements.len()
    }

    /// Returns the program in Nemo syntax, printed from the parsed statements rather than
    /// copied from the original text.
    fn __str__(&self) -> String {
        self.model.to_string()
    }

    /// Returns the program as `Program("name", N statements)`.
    fn __repr__(&self) -> String {
        format!(
            "Program({:?}, {} statements)",
            self.name,
            self.model.statements.len()
        )
    }
}
