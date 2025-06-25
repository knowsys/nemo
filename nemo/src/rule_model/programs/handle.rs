//! This module defines [ProgramHandle].

use std::rc::Rc;

use crate::{
    error::warned::Warned,
    parser::Parser,
    rule_file::RuleFile,
    rule_model::{
        components::statement::Statement,
        error::{TranslationReport, ValidationReport},
        pipeline::{
            commit::ProgramCommit, transformations::ProgramTransformation, ProgramPipeline,
        },
        programs::{program::Program, ProgramRead, ProgramWrite},
        translation::{ASTProgramTranslation, ProgramParseReport},
    },
};

/// Program-like object that is a spefic revision within [ProgramPipeline]
#[derive(Debug)]
pub struct ProgramHandle {
    /// Reference to the [ProgramPipeline]
    pipeline: Rc<ProgramPipeline>,

    /// Revision this object is a handle for
    revision: usize,
}

impl ProgramHandle {
    /// Create a new [ProgramHandle] from a reference to a [ProgramPipeline]
    /// and a revision number.
    pub(crate) fn new(pipeline: Rc<ProgramPipeline>, revision: usize) -> Self {
        Self { pipeline, revision }
    }

    /// Create a [ProgramHandle] from a [RuleFile].
    pub fn from_file(
        file: &RuleFile,
    ) -> Result<Warned<Self, TranslationReport>, ProgramParseReport> {
        let parser = Parser::initialize(file.content());
        let ast = parser.parse().map_err(|(_tail, report)| report)?;

        let mut commit = ProgramCommit::empty(ProgramPipeline::new(), ValidationReport::default());

        ASTProgramTranslation::default()
            .translate(&ast, &mut commit)
            .warned(commit.submit().expect("No validation has occurred yet"))
    }

    /// Create a new [ProgramCommit] representing an empty program
    /// that contains a copy of the [ValidationReport] from the given handle.
    pub fn fork(&self) -> ProgramCommit {
        let report = self.pipeline.revision(self.revision).report().clone();

        ProgramCommit::empty(self.pipeline.clone(), report)
    }

    /// Create a new [ProgramCommit] representing an exact copy of the given program.
    pub fn fork_full(&self) -> ProgramCommit {
        let report = self.pipeline.revision(self.revision).report().clone();
        let statements = self.pipeline.revision(self.revision).statements();

        ProgramCommit::new(self.pipeline.clone(), statements, report)
    }

    /// Apply a [ProgramTransformation] to this program.
    pub fn transform<Transformation: ProgramTransformation>(
        &self,
        transformation: Transformation,
    ) -> Result<ProgramHandle, ValidationReport> {
        transformation.apply(self)
    }

    /// Return a reference to the [ValidationReport]
    /// attached to this handle.
    pub fn report(&self) -> &ValidationReport {
        self.pipeline.revision(self.revision).report()
    }

    /// Create a [Program] from this handle.
    pub fn materialize(&self) -> Program {
        let mut program = Program::default();

        for statement in self.statements() {
            program.add_statement(statement.clone());
        }

        program
    }
}

impl ProgramRead for ProgramHandle {
    fn statements(&self) -> impl Iterator<Item = &Statement> {
        self.pipeline
            .revision(self.revision)
            .statements()
            .map(|&id| self.pipeline.statement(id))
    }
}

// struct StatementIterator<'a> {
//     pipeline: &'a Rc<RefCell<ProgramPipeline>>,
//     revision: usize,
//     current_statement: usize,
// }

// impl<'a> Iterator for StatementIterator<'a> {
//     type Item = Ref<'a, Statement>;

//     fn next(&mut self) -> Option<Self::Item> {
//         let id = self
//             .pipeline
//             .borrow()
//             .revision(self.revision)
//             .statement(self.current_statement);
//         self.current_statement += 1;

//         id.map(|id| Ref::map(self.pipeline.borrow(), |pipeline| pipeline.statement(id)))
//     }
// }
