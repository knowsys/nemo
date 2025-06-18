//! This module defines [ProgramRevision].

use crate::rule_model::{error::ValidationReport, pipeline::id::ProgramComponentId};

/// Version of a program within a [super::ProgramPipeline]
#[derive(Debug, Clone, Default)]
pub struct ProgramRevision {
    /// The ids of statements that are valid in this revision
    statements: Vec<ProgramComponentId>,

    /// Validation errors of this revision
    report: ValidationReport,
}

impl ProgramRevision {
    /// Create a new [ProgramRevision].
    pub fn new(statements: Vec<ProgramComponentId>, report: ValidationReport) -> Self {
        Self { statements, report }
    }

    /// Return a reference to the [ValidationReport].
    pub fn report(&self) -> &ValidationReport {
        &self.report
    }

    /// Return an iterator over the statement ids in this revision.
    pub fn statements(&self) -> impl Iterator<Item = &ProgramComponentId> {
        self.statements.iter()
    }

    /// For a given index, return the statement id at that index,
    /// if it exists.
    pub fn statement(&self, index: usize) -> Option<ProgramComponentId> {
        self.statements.get(index).cloned()
    }
}
