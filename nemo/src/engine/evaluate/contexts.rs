//! The evaluation contexts of the reasoning runs,
//! kept for the explanations that re-execute their applications.

use nemo_physical::function::EvaluationContext;

/// The evaluation context of each reasoning run, by the step of its first application.
///
/// Every `NOW()` in one run returns the same instant, and each run has its own.
/// An explanation re-executes an application in the context of the run that performed it,
/// so its functions return the values they returned then.
#[derive(Debug, Default)]
pub(crate) struct RunContexts(Vec<(u64, EvaluationContext)>);

impl RunContexts {
    /// Records the context of a run whose first application has the given step.
    pub(crate) fn begin(&mut self, step: u64, context: EvaluationContext) {
        // A run that applied nothing is replaced by the next one.
        if let Some(last) = self.0.last_mut()
            && last.0 == step
        {
            *last = (step, context);
            return;
        }
        self.0.push((step, context));
    }

    /// Returns the context of the run that performed the application at the given step.
    ///
    /// Before any run, there is no derived row to explain, and a fresh context is returned.
    pub(crate) fn at(&self, step: u64) -> EvaluationContext {
        let after = self.0.partition_point(|&(first, _)| first <= step);
        self.0
            .get(after.saturating_sub(1))
            .map_or_else(EvaluationContext::now, |&(_, context)| context)
    }

    /// Returns the context of the latest run.
    ///
    /// Before any run, a fresh context is returned.
    pub(crate) fn latest(&self) -> EvaluationContext {
        self.0
            .last()
            .map_or_else(EvaluationContext::now, |&(_, context)| context)
    }

    /// Forgets the context of every run.
    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }
}

#[cfg(test)]
mod tests {
    use nemo_physical::value::{DateTime, TimezoneOffset};

    use super::*;

    /// Returns the context at the given millisecond.
    fn at(millis: i64) -> EvaluationContext {
        EvaluationContext::at(DateTime::from_unix_millis(
            millis,
            Some(TimezoneOffset::UTC),
        ))
    }

    #[test]
    fn a_step_reads_the_context_of_its_run() {
        let mut runs = RunContexts::default();
        runs.begin(1, at(10));
        runs.begin(5, at(20));
        assert_eq!(runs.at(1), at(10));
        assert_eq!(runs.at(4), at(10));
        assert_eq!(runs.at(5), at(20));
        assert_eq!(runs.at(9), at(20));
        assert_eq!(runs.latest(), at(20));
    }

    #[test]
    fn a_run_without_applications_is_replaced_by_the_next() {
        let mut runs = RunContexts::default();
        runs.begin(3, at(10));
        runs.begin(3, at(20));
        assert_eq!(runs.at(3), at(20));
        assert_eq!(runs.0.len(), 1);
    }
}
