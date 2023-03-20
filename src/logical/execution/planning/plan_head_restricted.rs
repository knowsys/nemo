//! Module defining the strategies used to
//! derive the new facts for a rule application with existential variables in the head.

use std::collections::{HashMap, HashSet};

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Atom, Identifier, Rule, Term, Variable},
        program_analysis::{
            analysis::RuleAnalysis, normalization::normalize_atom_vector,
            variable_order::VariableOrder,
        },
        table_manager::{SubtableExecutionPlan, SubtableIdentifier},
        TableManager,
    },
    physical::{
        dictionary::Dictionary,
        management::{
            database::{ColumnOrder, TableId},
            execution_plan::ExecutionNodeRef,
        },
        tabular::operations::triescan_project::ProjectReordering,
    },
};

use super::{
    plan_util::{head_instruction_from_atom, subplan_union, HeadInstruction},
    HeadStrategy, SeminaiveJoinGenerator,
};

/// Strategy for the restricted chase.
#[derive(Debug)]
pub struct RestrictedChaseStrategy {
    join_generator: SeminaiveJoinGenerator,
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

        let join_generator = SeminaiveJoinGenerator {
            variables: normalized_head_variables,
            atoms: normalized_head.atoms,
            filters: normalized_head.filters,
        };

        RestrictedChaseStrategy {
            frontier_variables,
            join_generator,
            predicate_to_instructions,
            predicate_to_full_existential,
            analysis: analysis.clone(),
        }
    }
}

impl<Dict: Dictionary> HeadStrategy<Dict> for RestrictedChaseStrategy {
    fn add_plan_head(
        &self,
        table_manager: &TableManager<Dict>,
        current_plan: &mut SubtableExecutionPlan,
        body: ExecutionNodeRef,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step: usize,
    ) {
        // TODO: We need like three versions of the same variable order in this function
        // Clearly, some more thinking is needed
        let normalized_head_variable_order = compute_normalized_variable_order(
            &self.join_generator.atoms,
            variable_order.restrict_to(&self.analysis.head_variables),
        );

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
        let body_projection_reorder = ProjectReordering::from_vector(
            body_variables_in_order
                .iter()
                .enumerate()
                .filter(|(_, v)| self.frontier_variables.contains(v))
                .map(|(i, _)| i)
                .collect(),
            self.analysis.body_variables.len(),
        );

        // Build the project node
        let node_body_project = current_plan
            .plan_mut()
            .project(body.clone(), body_projection_reorder);

        // 2. Compute the head matches projected to the frontier variables

        // Find the matches for the head by performing a seminaive join
        // let mut tree_head_join = ExecutionTree::new_temporary("Head (Restricted): Satisfied");

        let node_head_join = if let Some(node) = self.join_generator.seminaive_join(
            current_plan.plan_mut(),
            table_manager,
            rule_info.step_last_applied,
            step,
            &normalized_head_variable_order,
        ) {
            node
        } else {
            return;
        };

        current_plan.add_temporary_table(node_head_join.clone(), "Head (Restricted): Satisifed");

        // Project it to the frontier variables and save the result in an extra table (also removing duplicates)

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
        let head_projection_reorder = ProjectReordering::from_vector(
            head_variables_in_order
                .iter()
                .enumerate()
                .filter(|(_, v)| self.frontier_variables.contains(v))
                .map(|(i, _)| i)
                .collect(),
            head_variables_in_order.len(),
        );

        // Do the projection operation
        let node_head_project = current_plan
            .plan_mut()
            .project(node_head_join, head_projection_reorder);

        // Remove the duplicates
        let node_old_matches = subplan_union(
            current_plan.plan_mut(),
            table_manager,
            self.analysis.head_matches_identifier,
            &(0..step),
        );
        let node_project_minus = current_plan
            .plan_mut()
            .minus(node_head_project.clone(), node_old_matches);

        // Marking the node as a permanent output
        let head_projected_order = ColumnOrder::default();
        let head_projected_name = table_manager.generate_table_name(
            self.analysis.head_matches_identifier,
            &head_projected_order,
            step,
        );
        current_plan.add_permanent_table(
            node_project_minus.clone(),
            "Head (Restricted): Sat. Frontier",
            &head_projected_name,
            SubtableIdentifier::new(self.analysis.head_matches_identifier, step),
        );

        // 3.Compute the unsatisfied matches by taking the difference between the projected body and projected head matches
        let mut node_satisfied = subplan_union(
            current_plan.plan_mut(),
            table_manager,
            self.analysis.head_matches_identifier,
            &(0..step),
        );

        // The above does not include the table computed in this iteration, so we need to load and include it
        // let node_fetch_sat = tree_unsatisfied.fetch_new(head_projected_id);
        node_satisfied.add_subnode(node_head_project);

        let node_unsatisfied = current_plan
            .plan_mut()
            .minus(node_body_project, node_satisfied);

        // Append new nulls
        let node_with_nulls = current_plan
            .plan_mut()
            .append_nulls(node_unsatisfied, self.analysis.num_existential);

        // Add tree
        current_plan.add_temporary_table(node_with_nulls.clone(), "Head (Restricted): Unsatisfied");

        // 4. Project the new entries to each head atom
        for (&predicate, head_instructions) in self.predicate_to_instructions.iter() {
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
                Vec::<ExecutionNodeRef>::with_capacity(head_instructions.len());
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

                let head_reordering = ProjectReordering::from_vector(
                    head_binding.clone(),
                    unsat_variable_order.len(),
                );

                let project_node = current_plan
                    .plan_mut()
                    .project(node_with_nulls.clone(), head_reordering);
                let append_node = current_plan
                    .plan_mut()
                    .append_columns(project_node, head_instruction.append_instructions.clone());

                final_head_nodes.push(append_node);
            }

            let new_tables_union = current_plan.plan_mut().union(final_head_nodes);

            // We just pick the default order
            // TODO: Is there a better pick?
            let head_order = ColumnOrder::default();
            let head_table_name = table_manager.generate_table_name(predicate, &head_order, step);
            // let mut head_tree =
            // ExecutionTree::new_permanent("Head (Restricted): Result Project", &head_table_name);

            if *self.predicate_to_full_existential.get(&predicate).unwrap() {
                // Since every new entry will contain a fresh null no duplcate elimination is needed
                current_plan.add_permanent_table(
                    new_tables_union,
                    "Head (Restricted): Result Project",
                    &head_table_name,
                    SubtableIdentifier::new(predicate, step),
                );
            } else {
                // Duplicate elimination for atoms thats do not contain existential variables
                // Same as in plan_head_datalog
                let old_tables: Vec<TableId> = table_manager.tables_in_range(predicate, &(0..step));
                let old_table_nodes: Vec<ExecutionNodeRef> = old_tables
                    .into_iter()
                    .map(|id| current_plan.plan_mut().fetch_existing(id))
                    .collect();
                let old_table_union = current_plan.plan_mut().union(old_table_nodes);

                let remove_duplicate_node = current_plan
                    .plan_mut()
                    .minus(new_tables_union, old_table_union);

                current_plan.add_permanent_table(
                    remove_duplicate_node,
                    "Head (Restricted): Result Project",
                    &head_table_name,
                    SubtableIdentifier::new(predicate, step),
                );
            }
        }
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
