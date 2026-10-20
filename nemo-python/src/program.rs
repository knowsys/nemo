//! Programs: parsed text, inspectable before any engine exists.

use pyo3::prelude::*;

use nemo::{diagnostics::render::SourceTexts, model::FileId};

use crate::errors::{Diagnostic, resolve};

/// A parsed program: the text it came from, and what parsing had to say.
///
/// Parsing is total:
/// unparsable statements become diagnostics, and the rest becomes the program.
/// So inspection needs no engine and reads no data.
/// Loading it into an engine parses the (kept) text again under the engine's own file
/// identity, so that diagnostics later point into the right source.
#[pyclass(frozen, module = "nmo")]
#[derive(Debug)]
pub struct Program {
    /// The source text.
    pub text: String,
    /// The display name diagnostics show for it.
    pub name: String,
    /// The parsed model (spans numbered from file 0).
    pub model: nemo::model::Program,
    /// The parse diagnostics, resolved against the text.
    pub diagnostics: Vec<Diagnostic>,
}

#[pymethods]
impl Program {
    /// Parses a program file, named after its path,
    /// and reports what did not parse through `diagnostics` as [parse](Self::parse) does.
    ///
    /// Raises `OSError` if the file cannot be read.
    #[staticmethod]
    pub fn parse_file(path: &str) -> PyResult<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(Self::parse(text, path))
    }

    /// Parses program text.
    ///
    /// It never raises: `diagnostics` reports what did not parse, and `ok` is whether nothing
    /// did.
    #[staticmethod]
    #[pyo3(signature = (text, name = "<program>"))]
    pub fn parse(text: String, name: &str) -> Self {
        let (model, diagnostics) = nemo::model::Program::parse(&text, FileId::new(0));
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

    /// Returns the parse diagnostics (errors and warnings).
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

    /// Returns the predicates the program's atoms name, with their numbers of columns, sorted,
    /// an inspection without loading anything.
    #[getter]
    fn predicates(&self) -> Vec<(String, usize)> {
        self.model.predicates()
    }

    /// Returns the number of statements that parsed.
    fn __len__(&self) -> usize {
        self.model.statements.len()
    }

    /// Returns the program in Nemo syntax (as parsed, not the original text).
    fn __str__(&self) -> String {
        self.model.to_string()
    }

    fn __repr__(&self) -> String {
        format!(
            "Program({:?}, {} statements)",
            self.name,
            self.model.statements.len()
        )
    }
}
