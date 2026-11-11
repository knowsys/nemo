//! Finding a row: executing a plan until its output has a row that is accepted.
//!
//! The steps before the output's step run as in every execution.
//! If the output's step is a unit whose joins emit the output's rows,
//! each row is offered as its join finds it, and the join ends with the first row accepted.
//! Any other output is materialized, and its rows are offered from its trie.

use std::ops::ControlFlow;

use crate::{
    dictionary::Dictionary,
    execution::{JoinArena, RowEvaluator, Verdict},
    function::EvaluationContext,
    management::TableManager,
    plan::{
        OperationPlan, PlanLayout,
        execute::{Execution, StepContext, results::Accounting, unit::MemberJoin},
        layout::{Delivery, UnitLayout},
    },
    storage::StorageValue,
};

/// The failure of a search among the rows that joins emit:
/// the dictionary cannot store a computed value of the accepted row,
/// so the row stage drops the row, and it is no row of the output.
#[derive(Debug)]
struct Unstorable;

impl<'a> Execution<'a> {
    /// Runs a plan with its layout until its only output has a row that `judge` accepts,
    /// as [OperationPlan::find_row] describes.
    pub(crate) fn find(
        plan: &'a OperationPlan,
        plan_layout: &'a PlanLayout,
        manager: &'a mut TableManager,
        dictionary: &mut Dictionary,
        evaluation: EvaluationContext,
        arena: &JoinArena,
        mut judge: impl FnMut(&TableManager, &[StorageValue]) -> Verdict,
    ) -> Option<Vec<StorageValue>> {
        let output = plan.only_output();

        let mut context = StepContext {
            dictionary,
            evaluation,
            arena,
            accounting: Accounting::default(),
        };
        let mut execution = Self::new(plan, plan_layout, manager, &mut context.accounting);

        // Every step leads to the output, so the output's step is the last.
        let (last, earlier) = plan_layout
            .steps
            .split_last()
            .expect("a plan with an output has a step");
        debug_assert_eq!(last.node(), output, "the last step executes the output");
        for (position, step) in earlier.iter().enumerate() {
            execution.run_step(position, step, &mut context);
        }

        if let Some(unit) = plan_layout.emitting_unit()
            && let Ok(found) = execution.find_emitted(unit, &mut context, &mut judge)
        {
            return found;
        }

        let materialized = execution.materialized(last, &mut context);
        let canonical = materialized
            .output
            .expect("declared outputs keep their canonical trie");

        let mut found = None;
        materialized.tries[canonical].for_each_row_skipping(|row| {
            match judge(execution.manager, row) {
                Verdict::Accept => {
                    found = Some(row.to_vec());
                    ControlFlow::Break(())
                }
                Verdict::Reject { prefix } => ControlFlow::Continue(prefix),
            }
        });

        found
    }

    /// Offers the rows of a unit whose joins [emit them](PlanLayout::emitting_unit) to `judge`,
    /// each as its join finds it, and returns the first row accepted,
    /// or `None` if none is.
    ///
    /// Each member's join runs sequentially, and ends with the row accepted.
    /// Fails if the dictionary cannot store a computed value of the row accepted.
    fn find_emitted(
        &self,
        unit: &UnitLayout,
        context: &mut StepContext<'_>,
        judge: &mut impl FnMut(&TableManager, &[StorageValue]) -> Verdict,
    ) -> Result<Option<Vec<StorageValue>>, Unstorable> {
        let manager = self.manager;
        let arena = context.arena;

        for member in &unit.members {
            let dictionary: &Dictionary = context.dictionary;
            let mut evaluator = match &member.delivery {
                Delivery::Direct => None,
                Delivery::RowStage(stage) => Some(RowEvaluator::new(
                    stage,
                    member.emitted,
                    dictionary,
                    context.evaluation,
                )),
                Delivery::Sink(_) => unreachable!("a unit that emits its rows has no sink"),
            };
            // A row that the row stage drops is rejected alone, without being offered.
            let mut every_row_rejected = false;
            let mut offer = |row: &[StorageValue], changed: usize| {
                let passes = evaluator
                    .as_mut()
                    .is_none_or(|evaluator| evaluator.passes(row, changed));
                if !passes {
                    return Verdict::for_row(row, false);
                }

                let verdict = judge(manager, row);
                every_row_rejected |= verdict == Verdict::Reject { prefix: 0 };
                verdict
            };

            let blocks = self.member_blocks(member, arena.bump());
            let found = match MemberJoin::over(member, blocks, dictionary, context.evaluation) {
                MemberJoin::Empty => None,
                MemberJoin::EmptyRow => (offer(&[], 0) == Verdict::Accept).then(Vec::new),
                MemberJoin::Template(template) => template
                    .build(blocks.inputs, arena)
                    .find(&mut offer)
                    .map(<[StorageValue]>::to_vec),
            };
            let Some(row) = found else {
                // A rejection of every row also covers the rows of the members that follow.
                if every_row_rejected {
                    return Ok(None);
                }
                continue;
            };

            // The join has ended, so nothing borrows the dictionary,
            // and the computed values of the row can be encoded.
            let unstored = evaluator.map(RowEvaluator::unstored).unwrap_or_default();
            for value in &unstored {
                context
                    .dictionary
                    .encode(value.as_ref())
                    .ok_or(Unstorable)?;
            }

            return Ok(Some(row));
        }

        Ok(None)
    }
}
