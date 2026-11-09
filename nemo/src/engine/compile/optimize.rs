//! The optimization passes, which reduce the work of loading and evaluating a program
//! without changing what its rules derive.
//!
//! Since what the rules derive stays the same, tracing does not need to reverse the passes.
//! There are two kinds of passes:
//! - *Import passes* run over the resolved statements before any rule is compiled or any
//!   import is executed.
//!   They compute the [ImportPlan]: what the load of each imported predicate may leave out
//!   because no rule can observe it.
//! - *Program passes* run over the compiled rules after their join orders are chosen and
//!   before stratification.
//!
//! What is left out is never read.
//! A pruned column's cells are never parsed or checked against the declaration.
//! A row whose only malformed or undeclared value is in a pruned column is therefore stored
//! (and derived from), where a load without pruning would have dropped and reported it.
//! The rows the rules read are the same, but error counts and warnings may differ.
//!
//! [retain_imports](crate::engine::Engine::retain_imports) turns the import passes off,
//! for tables that must be stored as their files contain them.
//! The program passes always run.

mod filter_pushdown;
mod fold;
mod projection;
mod survey;

pub(crate) use filter_pushdown::{column_variable, conjoin};
pub(crate) use survey::ImportSurvey;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    engine::{
        compile::compiled::CompiledRule, compile::import_plan::ImportPlan,
        compile::translation::Translator,
    },
    model::Span,
};

/// An import pass, which decides what the loads of imported predicates may leave out
/// (see [ImportPlan]).
pub(crate) trait ImportPass: std::fmt::Debug + Send + Sync {
    /// Returns the pass's name, for logs.
    fn name(&self) -> &'static str;

    /// Records in `plan` what the loads may leave out, as the pass finds it in `survey`.
    fn run(
        &self,
        survey: &ImportSurvey,
        translator: &Translator<'_>,
        plan: &mut ImportPlan,
    ) -> Result<(), Diagnostic>;
}

/// A program pass, which rewrites the compiled rules.
pub(crate) trait ProgramPass: std::fmt::Debug + Send + Sync {
    /// Returns the pass's name, for logs.
    fn name(&self) -> &'static str;

    /// Rewrites the rules in place, and adds the diagnostics for the user to `reports`.
    ///
    /// `spans` contains the source position of each rule, if it is known.
    fn run(
        &self,
        rules: &mut [CompiledRule],
        spans: &[Option<Span>],
        reports: &mut Vec<Diagnostic>,
    );
}

/// The passes that an engine runs, in order.
#[derive(Debug)]
pub(crate) struct OptimizationPasses {
    /// The import passes, or none to store every import whole.
    pub(crate) imports: Vec<Box<dyn ImportPass>>,
    /// The program passes.
    pub(crate) program: Vec<Box<dyn ProgramPass>>,
}

impl Default for OptimizationPasses {
    /// Returns the standard passes.
    ///
    /// The import passes are projection pushdown and then filter pushdown,
    /// and the program pass is constant folding.
    fn default() -> Self {
        Self {
            imports: vec![
                Box::new(projection::ProjectionPushdown),
                Box::new(filter_pushdown::FilterPushdown),
            ],
            program: vec![Box::new(fold::ConstantFold)],
        }
    }
}

impl OptimizationPasses {
    /// Runs the import passes over a surveyed program, and returns the plan they compute.
    pub(crate) fn plan_imports(
        &self,
        survey: &ImportSurvey,
        translator: &Translator<'_>,
    ) -> Result<ImportPlan, Diagnostic> {
        let mut plan = ImportPlan::default();
        for pass in &self.imports {
            pass.run(survey, translator, &mut plan)?;
            tracing::debug!(pass = pass.name(), entries = plan.len(), "import pass ran");
        }
        Ok(plan)
    }

    /// Runs the program passes over the compiled rules,
    /// and returns their diagnostics for the user.
    pub(crate) fn rewrite(
        &self,
        rules: &mut [CompiledRule],
        spans: &[Option<Span>],
    ) -> Vec<Diagnostic> {
        let mut reports = Vec::new();
        for pass in &self.program {
            pass.run(rules, spans, &mut reports);
            tracing::debug!(pass = pass.name(), "program pass ran");
        }
        reports
    }
}
