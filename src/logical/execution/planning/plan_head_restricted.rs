//! Module defining the strategies used to
//! derive the new facts for a rule application with existential variables in the head.

use std::collections::HashSet;

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Rule, Variable},
        program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
        table_manager::TableKey,
        TableManager,
    },
    physical::{
        dictionary::Dictionary,
        management::execution_plan::{ExecutionResult, ExecutionTree},
        util::Reordering,
    },
};

use super::{plan_util::BODY_JOIN, HeadStrategy};

/// Strategy for the restricted chase.
#[derive(Debug)]
pub struct RestrictedChaseStrategy<'a> {
    frontier_variables: HashSet<Variable>,

    analysis: &'a RuleAnalysis,
}

impl<'a> RestrictedChaseStrategy<'a> {
    /// Create a new [`RestrictedChaseStrategy`] object.
    pub fn initialize(_rule: &'a Rule, analysis: &'a RuleAnalysis) -> Self {
        let frontier_variables = analysis
            .body_variables
            .intersection(&analysis.head_variables)
            .cloned()
            .collect();

        RestrictedChaseStrategy {
            frontier_variables,
            analysis,
        }
    }
}

const HEAD_TREE: usize = BODY_JOIN + 1;

impl<'a, Dict: Dictionary> HeadStrategy<Dict> for RestrictedChaseStrategy<'a> {
    fn execution_tree(
        &self,
        table_manager: &TableManager<Dict>,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> Vec<ExecutionTree<TableKey>> {
        let mut trees = Vec::<ExecutionTree<TableKey>>::new();

        // Resulting trie will contain all the non-satisfied body matches
        // Input from the generated head tables will be projected from this
        let mut head_tree = ExecutionTree::<TableKey>::new(
            String::from("Head (Restricted)"),
            ExecutionResult::Temp(HEAD_TREE),
        );

        // 0. Fetch the temporary table that represents all the matches for this rule application
        let node_fetch_body = head_tree.fetch_temp(BODY_JOIN);

        // 1. Compute the projection of the body join to the frontier variables

        // Get a vector of all the body variables but sorted in the variable order
        let mut body_variables_in_order: Vec<&Variable> =
            self.analysis.body_variables.iter().collect();
        body_variables_in_order.sort_by(|a, b| {
            variable_order
                .get(a)
                .unwrap()
                .cmp(variable_order.get(b).unwrap())
        });

        // Get the appropriate `Reordering` object that
        let body_projection_reorder = Reordering::new(
            body_variables_in_order
                .iter()
                .enumerate()
                .filter(|(_, v)| self.frontier_variables.contains(v))
                .map(|(i, _)| i)
                .collect(),
            self.analysis.body_variables.len(),
        );

        // Build the project node
        let node_body_project = head_tree.project(node_fetch_body, body_projection_reorder);

        trees
    }
}
