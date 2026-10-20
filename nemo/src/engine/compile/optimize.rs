//! The optimization passes: what the compiler does to a program beyond translating it.
//!
//! Passes leave what the rules derive unchanged, so tracing need not reverse them.
//!
//! Passes run at two points.
//! *Import passes* run over the resolved statements before any rule compiles or import
//! executes, and decide the [ImportPlan]:
//! what each import-fed predicate's loader may leave out because no rule can observe it.
//! *Program passes* run over the compiled rules after their join orders are chosen and before
//! stratification.
//!
//! What is left out is never read.
//! A pruned column's cells are never parsed or checked against the declaration.
//! A row whose only malformed or undeclared value is in a pruned column is therefore stored
//! (and derived from), where a load without pruning would have dropped and reported it.
//! The rows the rules read are the same, but error counts and warnings may differ.
//!
//! [retain_imports](crate::engine::Engine::retain_imports) drops the import passes,
//! for tables that must be stored as their files hold them.
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

/// A pass over the program's imports, deciding what loaders may leave out (see [ImportPlan]).
pub(crate) trait ImportPass: std::fmt::Debug + Send + Sync {
    /// Returns the pass's name, for logs.
    fn name(&self) -> &'static str;

    /// Refines the plan from the surveyed program.
    fn run(
        &self,
        survey: &ImportSurvey,
        translator: &Translator<'_>,
        plan: &mut ImportPlan,
    ) -> Result<(), Diagnostic>;
}

/// A pass over the compiled rules.
pub(crate) trait ProgramPass: std::fmt::Debug + Send + Sync {
    /// Returns the pass's name, for logs.
    fn name(&self) -> &'static str;

    /// Rewrites the rules in place, adding diagnostics for the user to `reports`.
    ///
    /// `spans` holds the source position of each rule, if known.
    fn run(
        &self,
        rules: &mut [CompiledRule],
        spans: &[Option<Span>],
        reports: &mut Vec<Diagnostic>,
    );
}

/// The passes one engine runs, in order.
#[derive(Debug)]
pub(crate) struct OptimizationPasses {
    /// The import passes, or none to store every import whole.
    pub(crate) imports: Vec<Box<dyn ImportPass>>,
    /// The program passes.
    pub(crate) program: Vec<Box<dyn ProgramPass>>,
}

impl Default for OptimizationPasses {
    /// Returns the standard passes: projection then filter pushdown into
    /// imports, constant folding over rules.
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
    /// Runs the import passes over a surveyed program into a fresh plan.
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

    /// Runs the program passes over compiled rules, returning their
    /// diagnostics for the user.
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
