//! Module defining the strategies used to
//! derive the new facts for a rule application with existential variables in the head.

use std::collections::HashSet;

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Atom, Filter, Rule, Variable},
        program_analysis::{
            analysis::RuleAnalysis, normalization::normalize_atom_vector,
            variable_order::VariableOrder,
        },
        table_manager::{ColumnOrder, TableKey},
        TableManager,
    },
    physical::{
        dictionary::Dictionary,
        management::execution_plan::{ExecutionResult, ExecutionTree},
        util::Reordering,
    },
};

use super::{
    plan_util::{subtree_union, BODY_JOIN},
    seminaive_join, HeadStrategy,
};

/// Strategy for the restricted chase.
#[derive(Debug)]
pub struct RestrictedChaseStrategy<'a> {
    normalized_head_atoms: Vec<Atom>,
    normalized_head_filters: Vec<Filter>,

    frontier_variables: HashSet<Variable>,

    analysis: &'a RuleAnalysis,
}

impl<'a> RestrictedChaseStrategy<'a> {
    /// Create a new [`RestrictedChaseStrategy`] object.
    pub fn initialize(rule: &'a Rule, analysis: &'a RuleAnalysis) -> Self {
        let frontier_variables = analysis
            .body_variables
            .intersection(&analysis.head_variables)
            .cloned()
            .collect();

        let normalized_head = normalize_atom_vector(
            &rule.head().iter().by_ref().collect::<Vec<&Atom>>(),
            &vec![],
        );

        RestrictedChaseStrategy {
            frontier_variables,
            normalized_head_atoms: normalized_head.atoms,
            normalized_head_filters: normalized_head.filters,
            analysis,
        }
    }
}

const HEAD_START: usize = BODY_JOIN + 1;
const HEAD_JOIN: usize = HEAD_START;
const HEAD_UNSAT: usize = HEAD_START + 2;

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
        let mut tree_unsatisfied = ExecutionTree::<TableKey>::new(
            String::from("Head (Restricted): Unsat"),
            ExecutionResult::Temp(HEAD_UNSAT),
        );

        // 0. Fetch the temporary table that represents all the matches for this rule application
        let node_fetch_body = tree_unsatisfied.fetch_temp(BODY_JOIN);

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
        let node_body_project = tree_unsatisfied.project(node_fetch_body, body_projection_reorder);

        // 2. Compute the head matches projected to the frontier variables

        // Find the matches for the head by performing a seminaive join
        let mut tree_head_join = ExecutionTree::<TableKey>::new(
            String::from("Head Join"),
            ExecutionResult::Temp(HEAD_JOIN),
        );

        if let Some(node_head_join) = seminaive_join(
            &mut tree_head_join,
            table_manager,
            rule_info.step_last_applied,
            step_number,
            &variable_order,
            &self.analysis.head_variables,
            &self
                .normalized_head_atoms
                .iter()
                .by_ref()
                .collect::<Vec<&Atom>>(),
            &self
                .normalized_head_filters
                .iter()
                .by_ref()
                .collect::<Vec<&Filter>>(),
        ) {
            tree_head_join.set_root(node_head_join);
        }

        trees.push(tree_head_join);

        // Project it to the frontier variables and save the result in an extra table (also removing duplicates)
        let head_projected_order = ColumnOrder::default(self.frontier_variables.len());

        let head_projected_name = table_manager.get_table_name(
            self.analysis.head_matches_identifier,
            step_number..step_number + 1,
        );
        let head_projected_key = TableKey::from_name(head_projected_name, head_projected_order);
        let mut tree_head_projected = ExecutionTree::<TableKey>::new(
            "Head Projection".to_string(),
            ExecutionResult::Save(head_projected_key),
        );

        // Get a vector of all the body variables but sorted in the variable order
        let mut head_variables_in_order: Vec<&Variable> =
            self.analysis.head_variables.iter().collect();
        head_variables_in_order.sort_by(|a, b| {
            variable_order
                .get(a)
                .unwrap()
                .cmp(variable_order.get(b).unwrap())
        });

        // Get the appropriate `Reordering` object that
        let head_projection_reorder = Reordering::new(
            head_variables_in_order
                .iter()
                .enumerate()
                .filter(|(_, v)| self.frontier_variables.contains(v))
                .map(|(i, _)| i)
                .collect(),
            self.analysis.body_variables.len(),
        );

        // Do the projection operation
        let node_fetch_join = tree_head_projected.fetch_temp(HEAD_JOIN);
        let node_project = tree_head_projected.project(node_fetch_join, head_projection_reorder);

        // Remove the duplicates
        let node_old_matches = subtree_union(
            &mut tree_head_projected,
            table_manager,
            self.analysis.head_matches_identifier,
            0..step_number,
            &Reordering::default(self.frontier_variables.len()),
        );
        let node_project_minus = tree_head_projected.minus(node_project, node_old_matches);

        // Add tree
        tree_head_projected.set_root(node_project_minus);
        trees.push(tree_head_projected);

        // 3.Compute the unsatisfied matches by taking the difference between the projected body and projected head matches
        let node_satisfied = subtree_union(
            &mut tree_unsatisfied,
            table_manager,
            self.analysis.head_matches_identifier,
            0..(step_number + 1),
            &Reordering::default(self.frontier_variables.len()),
        );
        let node_unsatisfied = tree_unsatisfied.minus(node_body_project, node_satisfied);

        // Add tree
        tree_unsatisfied.set_root(node_unsatisfied);
        trees.push(tree_unsatisfied);

        // 4. Project the unsatisfied head atoms to each

        trees
    }
}
