//! Reloading: replacing the loaded program by a changed one, keeping the stored data that the
//! change does not affect.
//!
//! The new statements are compared with the loaded ones.
//! The predicates whose defining statements changed are *rebuilt*:
//! their tables are dropped, and the new program loads them anew.
//! So is every predicate without input facts to keep,
//! which the new program may then use with another number of columns,
//! and every imported predicate that was stored without something the new program reads.
//! Every other predicate keeps its input facts and loses its derived facts.

use std::collections::{HashMap, HashSet};

use nemo_physical::{
    diagnostics::Diagnostic,
    management::{Revision, TableId},
};

use crate::{
    diagnostics::EngineError,
    engine::{
        Engine,
        compile::translation::{Purpose, TranslationState},
        evaluate::extensional::ExtensionalOrigin,
        registered::Registered,
        remote::{LoadKind, Progress, SuspendedLoad},
        staging::{
            Loaded, Staged,
            translate::{StagedProgram, Translated},
        },
    },
    model::{Directive, Identifier, Program, Statement},
    pipeline::RevisionId,
};

/// What a reload must rebuild, according to its comparison of the statements.
struct RebuildScope {
    /// Whether every predicate is rebuilt, since a changed directive can change the meaning
    /// of any value.
    all: bool,
    /// Otherwise, the predicates whose defining statements changed.
    predicates: HashSet<String>,
}

/// A table that a reload replaces: the old table of a rebuilt predicate,
/// or the scratch table of its bound import.
struct ReplacedTable {
    /// The resolved name of the predicate.
    predicate: String,
    /// The table.
    table: TableId,
}

/// Removes the predicates named in `names` from a translation state and the registered
/// predicates, and returns their tables, including the scratch tables of bound imports.
fn unregister_from(
    state: &mut TranslationState,
    predicates: &mut HashMap<String, Registered>,
    names: &[String],
) -> Vec<ReplacedTable> {
    let mut replaced: Vec<ReplacedTable> = Vec::new();
    for name in names {
        if let Some(Registered { table, .. }) = predicates.remove(name) {
            state.seeds.remove(&table);
            state.plan.remove(name);
            state.open.remove(name);
            replaced.push(ReplacedTable {
                predicate: name.clone(),
                table,
            });
        }
        if let Some(scratch) = state.scratch.remove(name) {
            replaced.push(ReplacedTable {
                predicate: name.clone(),
                table: scratch,
            });
        }
    }

    replaced
}

/// Returns the predicate that a statement gives input rows, which is that of a fact or an
/// import, or `None` for any other statement.
fn input_predicate(statement: &Statement) -> Option<&Identifier> {
    match statement {
        Statement::Fact(fact) => Some(&fact.predicate),
        Statement::Directive(Directive::Import(import)) => Some(&import.predicate),
        _ => None,
    }
}

/// Returns, per statement, whether its data is already stored,
/// which holds for a fact or an import of a predicate that stays registered.
///
/// The predicate names are resolved by `state`, the translation state from before the reload.
fn preloaded(
    statements: &[Statement],
    state: &TranslationState,
    predicates: &HashMap<String, Registered>,
) -> Vec<bool> {
    statements
        .iter()
        .map(|statement| {
            input_predicate(statement)
                .and_then(|predicate| state.resolve(predicate))
                .is_some_and(|name| predicates.contains_key(&name))
        })
        .collect()
}

impl Engine {
    /// Runs the pipeline stages over a replacement program and rebuilds the stored data for it.
    pub(in crate::engine) fn reload_program(
        &mut self,
        program: Program,
    ) -> Result<Progress, Diagnostic> {
        tracing::info!(
            statements = program.statements.len(),
            "reloading the program"
        );
        if self.program.is_none() {
            return Err(EngineError::NoProgram
                .diagnostic()
                .with_hint("call `load()` first; `reload()` replaces a loaded program"));
        }

        self.start_loading(LoadKind::Reload, program)
    }

    /// Translates the written program of a staged replacement as
    /// [reload_staged](Self::reload_staged) first translates the program that the stages made
    /// of it, but reads no data and stores nothing.
    pub(super) fn validate_reload(&mut self, staged: &Staged) -> Result<(), Diagnostic> {
        let held = self.revision.expect("a loaded program has a revision");
        let new_statements = self.statements_of(staged.revision);
        let rebuild = self.rebuild_by_diff(&self.statements_of(held), &new_statements);
        let rebuilt = self.rebuilt_predicates(&rebuild, &HashSet::new(), &new_statements);
        let mut state = self.translation.clone();
        let mut predicates = self.predicates.clone();
        unregister_from(&mut state, &mut predicates, &rebuilt);
        let preloaded = preloaded(
            &self.statements_of(staged.written),
            &self.translation,
            &predicates,
        );
        state.clear_statement_state();
        self.translate_written(
            staged.written,
            state,
            predicates,
            preloaded,
            Purpose::Validate,
        )?;
        Ok(())
    }

    /// Returns the statements of a revision.
    fn statements_of(&self, revision: RevisionId) -> Vec<Statement> {
        self.pipeline
            .statements(revision)
            .map(|(_, statement)| statement.clone())
            .collect()
    }

    /// Translates a staged replacement and rebuilds the stored data.
    ///
    /// The new program is translated against the kept predicates,
    /// whose facts and imports are preloaded:
    /// their rows are kept, and their resources are not read again.
    /// A kept import may have been stored without columns or rows that the new rules read.
    /// The translation then lists these imports,
    /// and is repeated with their predicates rebuilt as well.
    /// `load` keeps them, for a reload that is suspended before it commits.
    pub(super) fn reload_staged(
        &mut self,
        load: &mut SuspendedLoad,
        program: &StagedProgram,
    ) -> Result<Loaded, Diagnostic> {
        let held = self.revision.expect("a loaded program has a revision");
        let rebuild = self.rebuild_by_diff(&self.statements_of(held), &program.statements);

        let (translated, replaced) = loop {
            let rebuilt = self.rebuilt_predicates(&rebuild, &load.unstable, &program.statements);

            // The rebuilt predicates are unregistered, so that the new program registers
            // them anew. Their tables are dropped only once the translation is done.
            let checkpoint = self.checkpoint();
            let replaced = unregister_from(&mut self.translation, &mut self.predicates, &rebuilt);
            let preloaded = preloaded(
                &program.statements,
                &checkpoint.translation,
                &self.predicates,
            );

            match self.translate(load, program, preloaded, checkpoint)? {
                Translated::Done(translated) => break (*translated, replaced),
                Translated::Missing(missing) => return Ok(Loaded::Missing(missing)),
                Translated::Unstable(unstable) => {
                    // A rebuilt predicate has no stored import,
                    // so each repetition rebuilds more predicates than the one before.
                    let before = load.unstable.len();
                    let names = unstable.iter().map(|import| import.predicate.clone());
                    load.unstable.extend(names);
                    if load.unstable.len() == before {
                        return Err(super::refuse_unstable(&unstable));
                    }
                }
            }
        };

        self.replace_store(replaced);
        self.assert_stored_types_of(&translated.compiled, &translated.facts);

        // The new rows go into one new revision, after the revision of the first load.
        self.step = self.step.max(1);
        let revision = Revision(self.step);
        let grown = self.store_input(translated.facts, revision, ExtensionalOrigin::Load);
        if !grown.is_empty() {
            self.step += 1;
        }

        self.forget_runs();

        Ok(Loaded::Done(translated.compiled))
    }

    /// Returns what a reload rebuilds, by comparing the old and the new statements.
    ///
    /// The comparison is between rendered statements.
    /// A rendered statement parses back to the same statement,
    /// so two statements are the same, apart from their spans, if they render the same.
    /// A statement is unchanged if its rendering occurs as often in the old program as in
    /// the new one.
    /// A predicate is rebuilt if a statement defining it changed,
    /// and every predicate is rebuilt if a changed directive can change the meaning of any
    /// value, or if a changed statement names a predicate that cannot be resolved.
    fn rebuild_by_diff(
        &self,
        old_statements: &[Statement],
        new_statements: &[Statement],
    ) -> RebuildScope {
        let mut net: HashMap<String, i64> = HashMap::new();
        for statement in old_statements {
            *net.entry(statement.to_string()).or_default() += 1;
        }

        for statement in new_statements {
            *net.entry(statement.to_string()).or_default() -= 1;
        }

        let changed =
            |statement: &Statement| net.get(&statement.to_string()).copied().unwrap_or(0) != 0;

        let mut rebuild = RebuildScope {
            all: false,
            predicates: HashSet::new(),
        };
        let touch = |predicate: &Identifier, rebuild: &mut RebuildScope| match self
            .translation
            .resolve(predicate)
        {
            Some(name) => {
                rebuild.predicates.insert(name);
            }
            None => rebuild.all = true,
        };
        for statement in old_statements.iter().chain(new_statements) {
            if !changed(statement) {
                continue;
            }

            match statement {
                Statement::Fact(fact) => touch(&fact.predicate, &mut rebuild),
                // Rules are compiled from the new program anyway.
                Statement::Rule(_) => {}
                Statement::Directive(directive) => match directive {
                    Directive::Import(import) => touch(&import.predicate, &mut rebuild),
                    Directive::Declare(declare) => touch(&declare.predicate, &mut rebuild),
                    Directive::Input(input) => touch(&input.predicate, &mut rebuild),
                    // These store no rows.
                    Directive::Output(_) | Directive::Export(_) => {}
                    _ => rebuild.all = true,
                },
            }
        }

        rebuild
    }

    /// Returns the registered predicates that a reload rebuilds.
    ///
    /// They are every registered predicate if `rebuild` says so.
    /// Otherwise, they are:
    /// - the changed predicates;
    /// - those in `unstable`, whose stored import does not cover what the new rules read;
    /// - every predicate without input rows to keep, which is one that no fact or import among
    ///   `statements`, the new program's, names, and that holds no rows added through
    ///   [import_rows](crate::engine::Engine::import_rows).
    ///
    /// A predicate of the last kind holds only derived rows, which a reload drops.
    /// The new program registers it anew if it uses it, with the number of columns that it
    /// uses it with.
    fn rebuilt_predicates(
        &self,
        rebuild: &RebuildScope,
        unstable: &HashSet<String>,
        statements: &[Statement],
    ) -> Vec<String> {
        if rebuild.all {
            return self.predicates.keys().cloned().collect();
        }

        let with_input_statements: HashSet<String> = statements
            .iter()
            .filter_map(input_predicate)
            .filter_map(|predicate| self.translation.resolve(predicate))
            .collect();

        self.predicates
            .keys()
            .filter(|name| {
                let keeps_input_rows =
                    with_input_statements.contains(*name) || self.holds_imported_rows(name);
                rebuild.predicates.contains(*name) || unstable.contains(*name) || !keeps_input_rows
            })
            .cloned()
            .collect()
    }

    /// Drops the replaced tables with their extensional blocks,
    /// and the derived rows of every kept table.
    ///
    /// The rows that were added through [import_rows](crate::engine::Engine::import_rows) to a
    /// replaced table cannot be reproduced from statements,
    /// so dropping them is reported with a warning.
    fn replace_store(&mut self, replaced: Vec<ReplacedTable>) {
        for ReplacedTable { predicate, table } in replaced {
            let blocks = self.extensional.remove(&table).unwrap_or_default();
            if blocks
                .iter()
                .any(|block| block.origin == ExtensionalOrigin::ImportRows)
            {
                self.pipeline
                    .report(EngineError::ImportedRowsDropped { predicate }.diagnostic());
            }

            self.manager.drop_table(table);
        }

        let tables: Vec<TableId> = self
            .predicates
            .values()
            .map(|&Registered { table, .. }| table)
            .collect();
        for table in tables {
            self.drop_derived(table);
        }
    }

    /// Forgets what the runs of the replaced program left behind:
    /// no rule was applied, no table is marked as having gained input facts,
    /// the fixpoint is not reached,
    /// and the application log, the run contexts and the derivation times are empty.
    fn forget_runs(&mut self) {
        self.last_applied.clear();
        self.dirty.clear();
        self.fixpoint = false;
        self.applications.clear();
        self.contexts.clear();
        self.derivation_time.clear();
    }
}
