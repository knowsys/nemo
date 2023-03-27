//! Module defining the strategy for calculating all body matches for a rule application.

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Atom, Filter, Rule},
        program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
        table_manager::SubtableExecutionPlan,
        TableManager,
    },
    physical::{dictionary::Dictionary, management::execution_plan::ExecutionNodeRef},
};

use super::{BodyStrategy, SeminaiveJoinGenerator};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub struct SeminaiveStrategy {
    is_existential: bool,
    join_generator: SeminaiveJoinGenerator,
}

impl SeminaiveStrategy {
    /// Create new [`SeminaiveStrategy`] object.
    pub fn initialize(rule: &Rule, analysis: &RuleAnalysis) -> Self {
        // Since we don't support negation yet, we can just turn the literals into atoms
        // TODO: Think about negation here
        let atoms: Vec<Atom> = rule.body().iter().map(|l| l.atom().clone()).collect();
        let filters: Vec<Filter> = rule.filters().to_vec();

        let join_generator = SeminaiveJoinGenerator {
            atoms,
            filters,
            variables: analysis.body_variables.clone(),
        };

        Self {
            is_existential: analysis.is_existential,
            join_generator,
        }
    }
}

impl<Dict: Dictionary> BodyStrategy<Dict> for SeminaiveStrategy {
    fn add_plan_body(
        &self,
        table_manager: &TableManager<Dict>,
        current_plan: &mut SubtableExecutionPlan,
        rule_info: &RuleInfo,
        mut variable_order: VariableOrder,
        step_number: usize,
    ) -> Option<ExecutionNodeRef> {
        // let mut tree = ExecutionTree::new_temporary("Body Join");

        if self.is_existential {
            variable_order = variable_order.restrict_to(&self.join_generator.variables);
        }

        let node_seminaive = self.join_generator.seminaive_join(
            current_plan.plan_mut(),
            table_manager,
            rule_info.step_last_applied,
            step_number,
            &variable_order,
        );

        if let Some(node) = node_seminaive.clone() {
            current_plan.add_temporary_table(node, "Body Join");
        }

        node_seminaive
    }
}
