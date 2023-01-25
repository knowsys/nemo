//! Module defining the strategy for calculating all body matches for a rule application.

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Atom, Filter, Rule},
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
pub struct SeminaiveStrategy<'a> {
    body: Vec<&'a Atom>,
    filters: Vec<&'a Filter>,

    analysis: &'a RuleAnalysis,
}

impl<'a> SeminaiveStrategy<'a> {
    /// Create new [`SeminaiveStrategy`] object.
    pub fn initialize(rule: &'a Rule, analysis: &'a RuleAnalysis) -> Self {
        // Since we don't support negation yet, we can just turn the literals into atoms
        // TODO: Think about negation here
        let body: Vec<&Atom> = rule.body().iter().map(|l| l.atom()).collect();
        let filters = rule.filters().iter().collect::<Vec<&Filter>>();

        Self {
            body,
            filters,
            analysis,
        }
    }
}

impl<'a, Dict: Dictionary> BodyStrategy<'a, Dict> for SeminaiveStrategy<'a> {
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

        if self.analysis.is_existential {
            variable_order = variable_order.restrict_to(&self.analysis.body_variables);
        }

        let node_seminaive = if let Some(node) = seminaive_join(
            &mut tree,
            table_manager,
            rule_info.step_last_applied,
            step_number,
            &variable_order,
            &self.analysis.body_variables,
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
