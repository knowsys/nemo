//! Tracing why facts hold:
//! proof search over the stored derivations,
//! and the derivation-structure queries behind the visual proof display.
//!
//! Both resolve the requested facts with [ground_facts](Engine::ground_facts).

pub(super) mod proof;
pub(super) mod query;

use std::collections::HashMap;

use nemo_physical::{diagnostics::Diagnostic, management::TableId, value::DataValue};

use crate::{
    diagnostics::{EngineError, TraceError},
    engine::{Engine, compile::translation},
    model::{Fact, Term},
};

impl Engine {
    /// Returns the wall time in nanoseconds of the rule applications that stored facts of a
    /// predicate, given by its resolved name, or 0 if no rule derived any.
    pub(super) fn derivation_time(&self, predicate: &str) -> u64 {
        self.predicates
            .get(predicate)
            .and_then(|registered| self.derivation_time.get(&registered.table))
            .copied()
            .unwrap_or(0)
    }

    /// Resolves facts to their tables and values,
    /// checked as a fact of the program is (name, arity, values).
    ///
    /// Nothing is stored or registered.
    /// A fact is an error if its predicate is unknown, was introduced by a pipeline stage,
    /// or is not stored with all its columns.
    /// The first error is returned.
    pub(super) fn ground_facts(
        &mut self,
        facts: &[Fact],
    ) -> Result<Vec<(TableId, Vec<DataValue>)>, Diagnostic> {
        for fact in facts {
            if let Some(name) = self.translation.resolve(&fact.predicate) {
                self.whole_predicate(&name)?;
            }
        }
        let state = std::mem::take(&mut self.translation);
        let (stores, setup) = self.translation_parts(translation::Purpose::Load);
        let mut translator = translation::Translator::new(stores, setup, state, Vec::new());
        let mut resolved = Vec::with_capacity(facts.len());
        let mut problem = None;
        for fact in facts {
            match translator.trace_fact(fact) {
                Ok(entry) => resolved.push(entry),
                Err(refusal) => {
                    problem = Some(refusal);
                    break;
                }
            }
        }
        let (state, _) = translator.finish();
        self.translation = state;
        match problem {
            Some(problem) => Err(problem),
            None => Ok(resolved),
        }
    }

    /// Returns the canonical rendering of written constants,
    /// evaluated as the translator evaluates constants of the program.
    ///
    /// A constant that fails to evaluate is left out.
    pub(super) fn render_constants(&mut self, constants: Vec<Term>) -> HashMap<Term, String> {
        if constants.is_empty() {
            return HashMap::new();
        }
        let state = std::mem::take(&mut self.translation);
        let (stores, setup) = self.translation_parts(translation::Purpose::Load);
        let translator = translation::Translator::new(stores, setup, state, Vec::new());
        let values: Vec<_> = constants
            .into_iter()
            .filter_map(|constant| {
                let value = translator.evaluate(&constant).ok()?;
                Some((constant, value))
            })
            .collect();
        let (state, _) = translator.finish();
        self.translation = state;
        values
            .into_iter()
            .map(|(constant, value)| {
                let text = self.term_text(value.as_ref());
                (constant, text)
            })
            .collect()
    }

    /// Checks that the engine can answer a trace: a program is loaded,
    /// reasoning has processed every update, and the last run reached the fixpoint.
    pub(in crate::engine) fn traceable(&self) -> Result<(), Diagnostic> {
        if self.program.is_none() {
            return Err(EngineError::NoProgram.diagnostic());
        }
        if !self.dirty.is_empty() {
            return Err(TraceError::PendingUpdates.diagnostic());
        }
        if !self.fixpoint {
            return Err(TraceError::Unfinished.diagnostic());
        }
        Ok(())
    }
}
