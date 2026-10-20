//! What a check reports.

use nemo_physical::diagnostics::Diagnostic;

use super::InferredTypes;

/// What a [check](crate::engine::Engine::check) of a program reports:
/// the problems it found, and the types the analysis inferred.
#[derive(Debug, Clone, Default)]
pub struct CheckReport {
    /// What the check reported, warnings included.
    diagnostics: Vec<Diagnostic>,
    /// The types the analysis inferred, if the check got that far.
    types: Option<InferredTypes>,
}

impl CheckReport {
    /// Creates a report.
    pub(crate) fn new(diagnostics: Vec<Diagnostic>, types: Option<InferredTypes>) -> Self {
        Self { diagnostics, types }
    }

    /// Returns what the check reported, warnings included.
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    /// Returns what the check reported, warnings included, dropping the types.
    pub fn into_diagnostics(self) -> Vec<Diagnostic> {
        self.diagnostics
    }

    /// Returns the types the analysis inferred,
    /// or `None` if the check ended before the analysis ran.
    ///
    /// A check continues past a statement that does not translate,
    /// so the types are those of the statements that did.
    pub fn types(&self) -> Option<&InferredTypes> {
        self.types.as_ref()
    }

    /// Splits the report into its diagnostics and its types.
    pub(crate) fn into_parts(self) -> (Vec<Diagnostic>, Option<InferredTypes>) {
        (self.diagnostics, self.types)
    }
}
