//! This module defines [ProgramCommit].

use std::rc::Rc;

use crate::rule_model::{
    components::{statement::Statement, ComponentIdentity},
    error::ValidationReport,
    pipeline::{revision::ProgramRevision, ProgramPipeline},
    programs::{handle::ProgramHandle, ProgramWrite},
};

use super::id::ProgramComponentId;

/// Differentiate new statements from existing statements
#[derive(Debug, Clone, Copy)]
enum StatementStatus {
    /// Statement already exists within [super::ProgramPipeline]
    Existing(ProgramComponentId),
    /// Statement will be added with this commit
    Fresh,
}

/// Defines a commit adding a new program into
/// a [ProgramPipeline]
#[derive(Debug)]
pub struct ProgramCommit {
    /// Reference to the [ProgramPipeline]
    pipeline: Rc<ProgramPipeline>,

    /// List of new statements to add
    new: Vec<Statement>,
    /// List of statement ids representing the new program
    statements: Vec<StatementStatus>,

    /// Current [ValidationReport]
    report: ValidationReport,
}

impl ProgramCommit {
    /// Create a new [ProgramCommit] representing an empty program.
    pub fn empty(pipeline: Rc<ProgramPipeline>, report: ValidationReport) -> Self {
        Self {
            pipeline,
            new: Vec::default(),
            statements: Vec::default(),
            report,
        }
    }

    /// Create a new [ProgramCommit] containing existing statements.
    pub(crate) fn new<'a, StatementIter>(
        pipeline: Rc<ProgramPipeline>,
        statements: StatementIter,
        report: ValidationReport,
    ) -> Self
    where
        StatementIter: Iterator<Item = &'a ProgramComponentId>,
    {
        Self {
            pipeline,
            new: Vec::default(),
            statements: statements.cloned().map(StatementStatus::Existing).collect(),
            report,
        }
    }

    /// Return a refeference to the [ValidationReport].
    pub fn report(&self) -> &ValidationReport {
        &self.report
    }

    /// Return a mutable reference to the [ValidationReport].
    pub fn report_mut(&mut self) -> &mut ValidationReport {
        &mut self.report
    }

    /// Add an existing statement to this commit.
    pub fn keep<Component: ComponentIdentity>(&mut self, statement: &Component) {
        self.statements
            .push(StatementStatus::Existing(statement.id()));
    }

    /// Submit this commit to its associated [ProgramPipeline].
    pub fn submit(self) -> Result<ProgramHandle, ValidationReport> {
        if self.report.contains_errors() {
            return Err(self.report);
        }

        let mut statements = Vec::with_capacity(self.statements.len());
        let mut fresh = self.new.into_iter();

        for status in self.statements {
            match status {
                StatementStatus::Existing(id) => statements.push(id),
                StatementStatus::Fresh => {
                    if let Some(fresh_statement) = fresh.next() {
                        let id = self.pipeline.new_statement(fresh_statement);
                        statements.push(id);
                    }
                }
            }
        }

        let revision = self
            .pipeline
            .new_revision(ProgramRevision::new(statements, self.report));

        Ok(ProgramHandle::new(self.pipeline.clone(), revision))
    }
}

impl ProgramWrite for ProgramCommit {
    fn add_statement(&mut self, statement: Statement) {
        self.new.push(statement);
        self.statements.push(StatementStatus::Fresh)
    }
}
