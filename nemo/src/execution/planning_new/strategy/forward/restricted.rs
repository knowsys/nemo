//! This module defines [StrategyRestricted].

use std::collections::HashSet;

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning_new::{
        RuntimeInformation,
        analysis::variable_order::VariableOrder,
        normalization::atom::head::HeadAtom,
        operations::{
            restricted_frontier::GeneratorRestrictedFrontier,
            restricted_head::GeneratorRestrictedHead,
            restricted_null::GeneratorRestrictedNull,
            union::{GeneratorUnion, UnionRange},
        },
    },
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
    table_manager::{SubtableExecutionPlan, SubtableIdentifier},
};

/// Generator of an execution plan that uses the restricted chase
/// to compute the new derivations of a rule
#[derive(Debug)]
pub struct StrategyRestricted {
    /// New satisfied matches
    new_satisfied: GeneratorRestrictedHead,
    /// All satisfied matches
    all_satisfied: GeneratorUnion,

    /// frontier
    frontier: GeneratorRestrictedFrontier,

    /// Generator for the null columns
    nulls: GeneratorRestrictedNull,
}

impl StrategyRestricted {
    /// Create a new [StrategyRestricted].
    pub fn new(
        head: &[HeadAtom],
        frontier: HashSet<Variable>,
        order: &VariableOrder,
        rule_id: usize,
    ) -> Self {
        let new_satisfied = GeneratorRestrictedHead::new(head, frontier.clone(), order, rule_id);
        let all_satisfied = GeneratorUnion::new(
            new_satisfied.predicate().0,
            new_satisfied.output_variables(),
            UnionRange::All,
        );

        let frontier =
            GeneratorRestrictedFrontier::new(order.restrict_to(&frontier).as_ordered_list());

        let nulls = GeneratorRestrictedNull::new(head);

        Self {
            new_satisfied,
            all_satisfied,
            frontier,
            nulls,
        }
    }

    /// Return an iterator over all special predicates needed to execute this strategy.
    pub fn special_predicates(&self) -> impl Iterator<Item = (Tag, usize)> {
        std::iter::once(self.new_satisfied.predicate())
    }

    /// Append this operation to the plan.
    pub fn create_plan<'a>(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation<'a>,
    ) -> ExecutionNodeRef {
        let node_new_satisfied_matches_frontier = self.new_satisfied.create_plan(plan, runtime);
        let node_all_satisfied_matches_frontier = self.all_satisfied.create_plan_with(
            plan,
            node_new_satisfied_matches_frontier.clone(),
            runtime,
        );
        let node_matches_frontier = self.frontier.create_plan(plan, input_node, runtime);

        let node_unsatisfied_matches_frontier = plan.plan_mut().subtract(
            node_matches_frontier,
            vec![node_all_satisfied_matches_frontier],
        );

        let node_newer_satisfied_matches_frontier = plan.plan_mut().union(
            node_new_satisfied_matches_frontier.markers_cloned(),
            vec![
                node_new_satisfied_matches_frontier,
                node_unsatisfied_matches_frontier.clone(),
            ],
        );

        plan.add_permanent_table(
            node_newer_satisfied_matches_frontier,
            "Head (Restricted): Updated Sat. Frontier",
            "Restricted Chase Helper Table",
            SubtableIdentifier::new(self.new_satisfied.predicate().0, runtime.step_current),
        );

        self.nulls
            .create_plan(plan, node_unsatisfied_matches_frontier, runtime)
    }
}
