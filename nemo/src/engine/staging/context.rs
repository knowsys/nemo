//! The evaluation context in which the loading methods translate, and which the next run
//! shares with them.

use nemo_physical::{diagnostics::Diagnostic, function::EvaluationContext};

use crate::engine::{
    Engine,
    remote::{Progress, Suspension},
};

impl Engine {
    /// Returns the evaluation context in which a loading method translates.
    ///
    /// If no loading method has chosen a context since the last run began,
    /// it chooses one at the current instant.
    /// The next run evaluates in the same context.
    pub(in crate::engine) fn loading_context(&mut self) -> EvaluationContext {
        *self
            .run
            .next_context
            .get_or_insert_with(EvaluationContext::now)
    }

    /// Runs a loading method, and settles its choice of the context by how it ended
    /// (see [settle_context](Self::settle_context)).
    pub(in crate::engine) fn choosing_context(
        &mut self,
        call: impl FnOnce(&mut Self) -> Result<Progress, Diagnostic>,
    ) -> Result<Progress, Diagnostic> {
        let chose = self.run.next_context.is_none();
        let outcome = call(self);
        self.settle_context(chose, &outcome);
        outcome
    }

    /// Settles a loading method's choice of the context by how the method ended.
    ///
    /// `chose` says whether the method chose the context.
    /// A method suspended for data from the network keeps the choice until it is resumed
    /// or discarded.
    /// A method that failed undoes the choice, so that the next loading method chooses again.
    pub(in crate::engine) fn settle_context(
        &mut self,
        chose: bool,
        outcome: &Result<Progress, Diagnostic>,
    ) {
        if !chose {
            return;
        }

        match outcome {
            Ok(Progress::Done) => {}
            Ok(Progress::Suspended(_)) => {
                if let Some(Suspension::Load(load)) = &mut self.suspension {
                    load.chose_context = true;
                }
            }
            Err(_) => self.run.next_context = None,
        }
    }
}
