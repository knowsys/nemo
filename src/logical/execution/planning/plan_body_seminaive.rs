//! Module defining the strategy for calculating all body matches for a rule application.

use std::collections::HashSet;

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Atom, Filter, Rule, Variable},
        program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
        table_manager::TableKey,
        TableManager,
    },
    physical::{
        dictionary::Dictionary,
        management::execution_plan::{ExecutionResult, ExecutionTree},
    },
};

use super::{plan_util::BODY_JOIN, seminaive_join, BodyStrategy};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub struct SeminaiveStrategy {
    body: Vec<Atom>,
    filters: Vec<Filter>,
    
    is_existential: bool,
    body_variables: HashSet<Variable>
}

impl SeminaiveStrategy {
    /// Create new [`SeminaiveStrategy`] object.
    pub fn initialize(rule: &Rule, analysis: &RuleAnalysis) -> Self {
        // Since we don't support negation yet, we can just turn the literals into atoms
        // TODO: Think about negation here
        let body: Vec<Atom> = rule.body().iter().map(|l| l.atom().clone()).collect();
        let filters: Vec<Filter> = rule.filters().to_vec();

        Self {
            body,
            filters,
            is_existential: analysis.is_existential,
            body_variables: analysis.body_variables.clone()
        }
    }
}

impl<Dict: Dictionary> BodyStrategy<Dict> for SeminaiveStrategy {
    fn execution_tree(
        &self,
        table_manager: &TableManager<Dict>,
        rule_info: &RuleInfo,
        mut variable_order: VariableOrder,
        step_number: usize,
    ) -> ExecutionTree<TableKey> {
        let mut tree = ExecutionTree::<TableKey>::new(
            "Body Join".to_string(),
            ExecutionResult::Temp(BODY_JOIN),
        );

        if self.is_existential {
            variable_order = variable_order.restrict_to(&self.body_variables);
        }

        let node_seminaive = if let Some(node) = seminaive_join(
            &mut tree,
            table_manager,
            rule_info.step_last_applied,
            step_number,
            &variable_order,
            &self.body_variables,
            &self.body,
            &self.filters,
        ) {
            node
        } else {
            return tree;
        };

        tree.set_root(node_seminaive);

        tree
    }
}
