//! Module defining the strategies used to
//! derive the new facts for a rule application with existential variables in the head.

use std::collections::{HashMap, HashSet};

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Atom, Filter, Identifier, Rule, Term, Variable},
        program_analysis::{
            analysis::RuleAnalysis, normalization::normalize_atom_vector,
            variable_order::VariableOrder,
        },
        table_manager::{ColumnOrder, TableKey},
        TableManager,
    },
    physical::{
        dictionary::Dictionary,
        management::execution_plan::{ExecutionNodeRef, ExecutionResult, ExecutionTree},
        util::Reordering,
    },
};

use super::{
    plan_util::{head_instruction_from_atom, subtree_union, HeadInstruction, BODY_JOIN},
    seminaive_join, HeadStrategy,
};

/// Strategy for the restricted chase.
#[derive(Debug)]
pub struct RestrictedChaseStrategy {
    normalized_head_atoms: Vec<Atom>,
    normalized_head_filters: Vec<Filter>,
    normalized_head_variables: HashSet<Variable>,

    frontier_variables: HashSet<Variable>,

    predicate_to_instructions: HashMap<Identifier, Vec<HeadInstruction>>,
    predicate_to_full_existential: HashMap<Identifier, bool>,

    analysis: RuleAnalysis,
}

impl RestrictedChaseStrategy {
    /// Create a new [`RestrictedChaseStrategy`] object.
    pub fn initialize(rule: &Rule, analysis: &RuleAnalysis) -> Self {
        let frontier_variables = analysis
            .body_variables
            .intersection(&analysis.head_variables)
            .cloned()
            .collect();

        let normalized_head =
            normalize_atom_vector(&rule.head().iter().by_ref().collect::<Vec<&Atom>>(), &[]);
        let mut normalized_head_variables = HashSet::new();
        for atom in &normalized_head.atoms {
            for term in atom.terms() {
                if let Term::Variable(v) = term {
                    normalized_head_variables.insert(*v);
                }
            }
        }

        let mut predicate_to_instructions = HashMap::<Identifier, Vec<HeadInstruction>>::new();
        let mut predicate_to_full_existential = HashMap::<Identifier, bool>::new();

        for head_atom in rule.head() {
            let is_existential = head_atom
                .terms()
                .iter()
                .any(|t| matches!(t, Term::Variable(Variable::Existential(_))));

            let instructions = predicate_to_instructions
                .entry(head_atom.predicate())
                .or_insert(Vec::new());
            instructions.push(head_instruction_from_atom(head_atom));

            let is_full_existential = predicate_to_full_existential
                .entry(head_atom.predicate())
                .or_insert(true);
            *is_full_existential &= is_existential;
        }

        RestrictedChaseStrategy {
            frontier_variables,
            normalized_head_atoms: normalized_head.atoms,
            normalized_head_filters: normalized_head.filters,
            normalized_head_variables,
            predicate_to_instructions,
            predicate_to_full_existential,
            analysis: analysis.clone(),
        }
    }
}

const HEAD_START: usize = BODY_JOIN + 1;
const HEAD_JOIN: usize = HEAD_START;
const HEAD_UNSAT: usize = HEAD_START + 2;

impl<Dict: Dictionary> HeadStrategy<Dict> for RestrictedChaseStrategy {
    fn execution_tree(
        &self,
        table_manager: &TableManager<Dict>,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> Vec<ExecutionTree<TableKey>> {
        // TODO: We need like three versions of the same variable order in this function
        // Clearly, some more thinking is needed
        let normalized_head_variable_order = compute_normalized_variable_order(
            &self.normalized_head_atoms,
            variable_order.restrict_to(&self.analysis.head_variables),
        );

        let mut trees = Vec::<ExecutionTree<TableKey>>::new();

        // Resulting trie will contain all the non-satisfied body matches
        // Input from the generated head tables will be projected from this
        let mut tree_unsatisfied = ExecutionTree::<TableKey>::new(
            String::from("Head (Restricted): Unsatisfied"),
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

        // Get the appropriate `Reordering` object that projects from the body join to the frontier variables
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
            String::from("Head (Restricted): Satisfied"),
            ExecutionResult::Temp(HEAD_JOIN),
        );

        if let Some(node_head_join) = seminaive_join(
            &mut tree_head_join,
            table_manager,
            rule_info.step_last_applied,
            step_number,
            &normalized_head_variable_order,
            &self.normalized_head_variables,
            &self.normalized_head_atoms,
            &self.normalized_head_filters,
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
            "Head (Restricted): Sat. Frontier".to_string(),
            ExecutionResult::Save(head_projected_key.clone()),
        );

        // Get a vector of all the normalized head variables but sorted in the variable order
        let mut head_variables_in_order: Vec<&Variable> =
            normalized_head_variable_order.iter().collect();
        head_variables_in_order.sort_by(|a, b| {
            normalized_head_variable_order
                .get(a)
                .unwrap()
                .cmp(normalized_head_variable_order.get(b).unwrap())
        });

        // Get the appropriate `Reordering` object that projects the head join down to the frontier variables
        let head_projection_reorder = Reordering::new(
            head_variables_in_order
                .iter()
                .enumerate()
                .filter(|(_, v)| self.frontier_variables.contains(v))
                .map(|(i, _)| i)
                .collect(),
            head_variables_in_order.len(),
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
        let mut node_satisfied = subtree_union(
            &mut tree_unsatisfied,
            table_manager,
            self.analysis.head_matches_identifier,
            0..step_number,
            &Reordering::default(self.frontier_variables.len()),
        );

        // The above does not include the table computed in this iteration, so we need to load and include it
        let node_fetch_sat = tree_unsatisfied.fetch_table(head_projected_key);
        node_satisfied.add_subnode(node_fetch_sat);

        let node_unsatisfied = tree_unsatisfied.minus(node_body_project, node_satisfied);

        // Append new nulls
        let node_with_nulls =
            tree_unsatisfied.append_nulls(node_unsatisfied, self.analysis.num_existential);

        // Add tree
        tree_unsatisfied.set_root(node_with_nulls);
        trees.push(tree_unsatisfied);

        // 4. Project the new entries to each head atom
        for (&predicate, head_instructions) in self.predicate_to_instructions.iter() {
            let predicate_arity = head_instructions[0].arity;
            // We just pick the default order
            // TODO: Is there a better pick?
            let head_order = ColumnOrder::default(predicate_arity);

            let head_table_name =
                table_manager.get_table_name(predicate, step_number..step_number + 1);
            let head_table_key = TableKey::from_name(head_table_name, head_order.clone());
            let mut head_tree = ExecutionTree::<TableKey>::new(
                "Head (Restricted): Result Project".to_string(),
                ExecutionResult::Save(head_table_key),
            );

            let mut unsat_variable_order = VariableOrder::new();
            for &variable in &body_variables_in_order {
                if matches!(variable, Variable::Universal(_))
                    && self.frontier_variables.contains(variable)
                {
                    unsat_variable_order.push(*variable);
                }
            }
            for &variable in &head_variables_in_order {
                if matches!(variable, Variable::Existential(_)) {
                    unsat_variable_order.push(*variable);
                }
            }

            let mut final_head_nodes =
                Vec::<ExecutionNodeRef<TableKey>>::with_capacity(head_instructions.len());
            for head_instruction in head_instructions {
                // TODO:
                // This is just the `atom_binding` function. However you cannot use it, since it cannot deal with reduced atoms.
                let head_binding: Vec<usize> = head_instruction.reduced_atom.terms()
                .iter()
                .map(|t| {
                    if let Term::Variable(variable) = t {
                        *unsat_variable_order.get(variable).unwrap()
                    } else {
                        panic!("It is assumed that this function is only called on atoms which only contain variables.");
                    }
                })
                .collect();

                let head_reordering =
                    Reordering::new(head_binding.clone(), unsat_variable_order.len());

                let fetch_node = head_tree.fetch_temp(HEAD_UNSAT);
                let project_node = head_tree.project(fetch_node, head_reordering);
                let append_node = head_tree
                    .append_columns(project_node, head_instruction.append_instructions.clone());

                final_head_nodes.push(append_node);
            }

            let new_tables_union = head_tree.union(final_head_nodes);

            if *self.predicate_to_full_existential.get(&predicate).unwrap() {
                // Since every new entry will contain a fresh null no duplcate elimination is needed
                head_tree.set_root(new_tables_union);
            } else {
                // Duplicate elimination for atoms thats do not contain existential variables
                // Same as in plan_head_datalog
                let old_tables_keys: Vec<TableKey> = table_manager
                    .cover_whole_table(predicate)
                    .into_iter()
                    .map(|r| TableKey::new(predicate, r, head_order.clone()))
                    .collect();
                let old_table_nodes: Vec<ExecutionNodeRef<TableKey>> = old_tables_keys
                    .into_iter()
                    .map(|k| head_tree.fetch_table(k))
                    .collect();
                let old_table_union = head_tree.union(old_table_nodes);

                let remove_duplicate_node = head_tree.minus(new_tables_union, old_table_union);
                head_tree.set_root(remove_duplicate_node);
            }

            trees.push(head_tree);
        }
        trees
    }
}

/// Helper function to compute a variable order for a normalized
fn compute_normalized_variable_order(atoms: &[Atom], mut order: VariableOrder) -> VariableOrder {
    for atom in atoms {
        for term in atom.terms() {
            if let Term::Variable(variable) = term {
                if order.get(variable).is_none() {
                    order.push(*variable);
                }
            }
        }
    }

    order
}
