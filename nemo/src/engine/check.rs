//! Checking a program without loading it, and what a
//! [check](crate::engine::Engine::check) reports:
//! its diagnostics, and the types that its analysis inferred.
//!
//! It also contains the rewrite through which a check reads the parameters without a value.

mod inferred_types;
mod report;
mod rule_types;
mod unvalued;

pub use self::{inferred_types::InferredTypes, report::CheckReport, rule_types::RuleTypes};

pub(in crate::engine) use self::unvalued::rewrite_unvalued;

use std::collections::HashMap;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::EngineError,
    engine::{
        Engine,
        compile::translation::{Purpose, TranslationState},
        written::WrittenProgram,
    },
    model::Program,
    pipeline::NameScope,
};

impl Engine {
    /// Registers, parses, links and checks a program text
    /// (see [check_source](Engine::check_source)).
    pub(in crate::engine) fn check_text(
        &mut self,
        name: impl Into<String>,
        text: impl Into<String>,
    ) -> CheckReport {
        let file = match self.add_source(name, text) {
            Ok(file) => file,
            Err(refused) => return CheckReport::new(vec![refused], None),
        };
        let (program, mut diagnostics) = self.parse_source(file);
        let mark = self.pipeline.mark_reports();
        let linked = self.link(program);
        diagnostics.extend(self.pipeline.take_reports(mark));
        let types = match linked {
            Ok(linked) => {
                let (checked, types) = self.check(linked).into_parts();
                diagnostics.extend(checked);
                types
            }
            Err(problem) => {
                diagnostics.push(problem);
                None
            }
        };

        CheckReport::new(diagnostics, types)
    }

    /// Runs a check: stages the program,
    /// and translates the written program into scratch stores.
    ///
    /// Returns the types that the translation's type analysis inferred.
    /// Fails for a problem that ends the check early:
    /// the engine holds a program, the program is not linked, or a pipeline stage fails.
    pub(in crate::engine) fn check_inner(
        &mut self,
        program: Program,
    ) -> Result<InferredTypes, Diagnostic> {
        if self.program.is_some() {
            return Err(EngineError::AlreadyLoaded.diagnostic());
        }

        tracing::debug!(statements = program.statements.len(), "checking a program");
        let staged = self.stage(program, &[])?;
        let (compiled, tables) = self.translate_written(
            staged.written,
            TranslationState::default(),
            HashMap::new(),
            Vec::new(),
            Purpose::Check,
        )?;
        let written = WrittenProgram::new(&self.pipeline, staged.written, staged.revision);
        let texts = self
            .pipeline
            .statements(staged.written)
            .map(|(_, statement)| statement);

        Ok(InferredTypes::of_check(
            NameScope::of(texts),
            written.predicates(),
            &tables,
            &compiled,
        ))
    }
}
