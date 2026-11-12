//! Extending the loaded program with new statements.
//!
//! An extension keeps everything that is stored.
//! The whole program is compiled again, and the rules of the loaded program continue their
//! semi-naive evaluation where they left off.

use std::collections::HashSet;

use nemo_physical::{
    diagnostics::Diagnostic,
    management::{Revision, TableId},
};

use crate::{
    diagnostics::EngineError,
    engine::{
        Engine,
        compile::{compiled::CompiledProgram, translation::Purpose},
        evaluate::extensional::ExtensionalOrigin,
        remote::{LoadKind, Progress, SuspendedLoad},
        staging::{
            Loaded, Staged, expect_linked, refuse_unstable,
            translate::{StagedProgram, Translated},
        },
        transform::sparql_rewrite,
    },
    model::{Directive, Program, Statement},
    pipeline::{RevisionId, StatementId},
};

impl Engine {
    /// Runs the pipeline stages over an extension and loads it.
    pub(in crate::engine) fn extend_program(
        &mut self,
        program: Program,
    ) -> Result<Progress, Diagnostic> {
        tracing::info!(
            statements = program.statements.len(),
            "extending the program"
        );
        if self.program.is_none() {
            return Err(EngineError::NoProgram
                .diagnostic()
                .with_hint("call `load()` first; `extend()` adds to a loaded program"));
        }

        self.start_loading(LoadKind::Extend, program)
    }

    /// Stages an extension: checks it, and appends it to the loaded revision.
    ///
    /// The extension's parameters first get the values set through
    /// [set_parameter](Engine::set_parameter).
    /// The check fails for an extension that is not linked,
    /// or that changes the `@base`, a prefix or a parameter of the loaded program.
    /// The pipeline stages then run over the whole program without rewriting the loaded
    /// statements.
    /// So a rule of the extension that reads a loaded import is rewritten with the loaded
    /// program in view, and the names that the stages introduce are new to the whole program.
    pub(super) fn stage_extension(&mut self, mut program: Program) -> Result<Staged, Diagnostic> {
        let held = self.revision.expect("a loaded program has a revision");
        expect_linked(&program)?;
        self.supply_parameters(&mut program, true)?;
        let addition = self.pipeline.load(program);
        self.check_extension(addition)?;
        let frozen = self.pipeline.statements(held).count();
        let merged = self.pipeline.merge(held, addition);
        let held_written = self
            .written
            .as_ref()
            .expect("a loaded program has a written program")
            .revision();
        let written = self.pipeline.merge(held_written, addition);
        let merged = self.revive_sparql_imports(held, merged);
        let imported = self.imported_predicates();
        let revision = self
            .pipeline
            .extend(&self.stages.0, merged, frozen, &imported)?;
        Ok(Staged { written, revision })
    }

    /// Restores the SPARQL imports without a `where` clause that the `sparql` stage removed
    /// from the loaded program because no rule read them, so that the extension's rules can
    /// read them.
    ///
    /// `held` is the loaded revision, and `merged` the revision with the extension appended.
    /// Returns the revision with the restored imports, or `merged` if there are none.
    /// The stage removes them again if they stay unread.
    fn revive_sparql_imports(&mut self, held: RevisionId, merged: RevisionId) -> RevisionId {
        let root = self.pipeline.lineage(held)[0];
        let live: HashSet<StatementId> = self.pipeline.statements(held).map(|(id, _)| id).collect();
        let revived: Vec<(StatementId, Statement)> = self
            .pipeline
            .statements(root)
            .filter(|(id, statement)| {
                !live.contains(id) && sparql_rewrite::is_plain_sparql_import(statement)
            })
            .map(|(id, statement)| (id, statement.clone()))
            .collect();
        if revived.is_empty() {
            return merged;
        }

        let mut commit = self.pipeline.commit(merged, "extension");
        for (id, statement) in revived {
            commit.derive(&[id], statement);
        }

        commit.finish()
    }

    /// Checks that an extension changes no `@base`, prefix, or parameter of the loaded
    /// program, which would change the meaning of the loaded statements or the stored values.
    ///
    /// `addition` is the revision that contains the extension alone.
    /// Returns an error for the first directive that would change one.
    fn check_extension(&self, addition: RevisionId) -> Result<(), Diagnostic> {
        for (_, statement) in self.pipeline.statements(addition) {
            let Statement::Directive(directive) = statement else {
                continue;
            };

            let refusal = match directive {
                Directive::Base(base)
                    if self.translation.scope.base() != Some(base.iri.as_str()) =>
                {
                    Some((EngineError::ExtensionDeclaresBase, directive.span()))
                }
                Directive::Prefix(prefix)
                    if self
                        .translation
                        .scope
                        .prefixes()
                        .get(&prefix.prefix)
                        .is_some_and(|iri| iri != &prefix.iri) =>
                {
                    Some((EngineError::ExtensionRebindsPrefix, directive.span()))
                }
                Directive::Parameter(parameter)
                    if self
                        .translation
                        .globals
                        .contains_key(&parameter.variable.name) =>
                {
                    Some((
                        EngineError::ExtensionRedefinesParameter {
                            name: parameter.variable.name.clone(),
                        },
                        directive.span(),
                    ))
                }
                _ => None,
            };
            if let Some((kind, span)) = refusal {
                let mut problem = kind.diagnostic();
                if let Some(span) = span {
                    problem = problem.with_label(span, "declared here");
                }

                return Err(problem);
            }
        }

        Ok(())
    }

    /// Translates the written program of a staged extension as
    /// [extend_staged](Self::extend_staged) translates the program that the stages made of it,
    /// but reads no data and stores nothing.
    ///
    /// The statements of the loaded program come first, and their data is stored.
    pub(super) fn validate_extension(&mut self, staged: &Staged) -> Result<(), Diagnostic> {
        let held_written = self
            .written
            .as_ref()
            .expect("a loaded program has a written program")
            .revision();
        let mut preloaded = vec![true; self.pipeline.statements(held_written).count()];
        preloaded.resize(self.pipeline.statements(staged.written).count(), false);
        let mut state = self.translation.clone();
        state.clear_statement_state();
        let predicates = self.predicates.clone();
        self.translate_written(
            staged.written,
            state,
            predicates,
            preloaded,
            Purpose::Validate,
        )?;
        Ok(())
    }

    /// Translates a staged extension and stores its data.
    ///
    /// The statements of the loaded program come first in the staged program,
    /// and their data is stored, so only the extension's facts and imports are read.
    pub(super) fn extend_staged(
        &mut self,
        load: &mut SuspendedLoad,
        program: &StagedProgram,
    ) -> Result<Loaded, Diagnostic> {
        let held = self.revision.expect("a loaded program has a revision");
        let mut preloaded = vec![true; self.pipeline.statements(held).count()];
        preloaded.resize(program.statements.len(), false);

        let checkpoint = self.checkpoint();
        let translated = match self.translate(load, program, preloaded, checkpoint)? {
            Translated::Done(translated) => *translated,
            Translated::Missing(missing) => return Ok(Loaded::Missing(missing)),
            Translated::Unstable(unstable) => return Err(refuse_unstable(&unstable)),
        };
        let mut compiled = translated.compiled;

        // The extension's rows go into one new revision.
        self.assert_stored_types_of(&compiled, &translated.facts);
        let revision = Revision(self.step);
        let grown = self.store_input(translated.facts, revision, ExtensionalOrigin::Load);
        if !grown.is_empty() {
            self.step += 1;
        }

        self.dirty.extend(grown);
        self.adopt_extension(&mut compiled);
        self.fixpoint = false;

        Ok(Loaded::Done(compiled))
    }

    /// Copies the semi-naive progress of each loaded rule into the extended program.
    ///
    /// The loaded statements come first in the merged program,
    /// so the loaded rules keep their indices and the step of their last application,
    /// and semi-naive evaluation continues where it left off.
    /// A new rule may add facts to its head predicates for the first time.
    /// Their tables are conservatively marked as gaining input facts,
    /// so the derived facts that depend on them through negation or aggregation are dropped
    /// and derived again.
    fn adopt_extension(&mut self, compiled: &mut CompiledProgram) {
        let loaded = self
            .program
            .as_ref()
            .expect("an extension extends a program");
        for (rule, previous) in compiled.rules.iter_mut().zip(&loaded.rules) {
            rule.last_applied = previous.last_applied;
        }

        let added = compiled.rules.iter().skip(loaded.rules.len());
        let heads: Vec<TableId> = added.flat_map(Self::head_tables).collect();
        self.dirty.extend(heads);
    }
}
