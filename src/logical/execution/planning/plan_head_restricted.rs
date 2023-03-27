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
    plan_util::{atom_binding, head_instruction_from_atom, subplan_union, HeadInstruction},
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
        node_matches: ExecutionNodeRef,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step: usize,
    ) {
        // High-level description of the strategy:
        //   * We take as a given that a node representing all the new matches for this application
        //     is given as input to this function
        //     [We refer to this subtable as "Matches"]
        //   * In order to compute the matches that are not satisfied, we need to project "Matches" to the frontier variables
        //     [We refer to this subtable as "Matches Frontier"]
        //   * We maintain a table of frontier matches that are already satisfied by the head
        //     [We refer to this subtable as "Satisfied Matches Frontier"]
        //   * The above table is computed in a seminaive fashion similar to the body matches,
        //     whereby the join is only executed with the part of the head tables that are new
        //     since the last application of the rule
        //     [We refer to the table containing the currently computed satisifed matches as "New Satisfied Matches"]
        //     [We refer to the tables containing the satisifed matches for previous rule applications as "Old Satisfied Matches"].
        //   * The difference of "Matches Frontier" and "Satisfied Matches Frontier"
        //     results in a table that contains the frontier assignments for the current unsatsified matches
        //     [We refer to this subtable as "Unsatisfied Matches Frontier"]
        //   * We append to "Unsatisfied Matches Frontier" a number of columns with null values
        //     [We refer to this subtable as "Unsatisfied Matches Nulls"]
        //   * For each atom in the head:
        //     If it contains an existential varianormalize_atom_veble: Project from "Unsatisfied Matches Nulls" and append constants when needed
        //     If it does not contain an existential: Project from "Unsatisfied Matches Nulls", append constants and perform duplicate elimation

        // 1. Compute the table "New Satisfied Matches"

        // For this we will need to compute the seminaive join of the CQ expressed in the head of the rule
        // The problem is that the join assumes that the atom vector is "normalized", i.e.
        // does not contain any contants or repeat variable within one atom.
        // Hence we normalize the head in the constructor of this object.
        // However the variable order does not know about the potential new variables introduced during normalization
        // Hence we compute a new one
        let normalized_head_variable_order = append_unknown_variables(
            &self.join_generator.atoms,
            variable_order.restrict_to(&self.analysis.head_variables),
        );

        let node_new_satisfied_matches = self.join_generator.seminaive_join(
            current_plan.plan_mut(),
            table_manager,
            rule_info.step_last_applied,
            step,
            &normalized_head_variable_order,
        );

        current_plan.add_temporary_table(
            node_new_satisfied_matches.clone(),
            "Head (Restricted): Satisifed",
        );

        // 2. Compute the table "Satisfied Matches Frontier"

        // The order of variables in the table "Satisfied Matches"
        let variables_satisifed_matches = normalized_head_variable_order.as_ordered_list();
        // The above order but without non-fronier variables
        let variables_satisifed_matches_frontier = normalized_head_variable_order
            .restrict_to(&self.frontier_variables)
            .as_ordered_list();
        // Below is the reordering that would project the non-fronier columns away
        let satisfied_matches_frontier_reordering = ProjectReordering::from_transformation(
            &variables_satisifed_matches,
            &variables_satisifed_matches_frontier,
        );

        let node_new_satisfied_matches_frontier = current_plan.plan_mut().project(
            node_new_satisfied_matches,
            satisfied_matches_frontier_reordering,
        );

        // The above node represents the new satisfied matches which might still contain duplicates
        // In the following they are removed
        let node_old_satisfied_matches_frontier = subplan_union(
            current_plan.plan_mut(),
            table_manager,
            self.analysis.head_matches_identifier,
            &(0..step),
        );
        let node_new_satisfied_matches_frontier = current_plan.plan_mut().minus(
            node_new_satisfied_matches_frontier.clone(),
            node_old_satisfied_matches_frontier.clone(),
        );

        current_plan.add_permanent_table(
            node_new_satisfied_matches_frontier.clone(),
            "Head (Restricted): Sat. Frontier",
            "Restricted Chase Helper Table",
            SubtableIdentifier::new(self.analysis.head_matches_identifier, step),
        );

        let mut node_satisfied_matches_frontier = current_plan.plan_mut().union_empty();
        node_satisfied_matches_frontier.add_subnode(node_old_satisfied_matches_frontier);
        node_satisfied_matches_frontier.add_subnode(node_new_satisfied_matches_frontier);

        // 3. Compute "Matches Frontier"

        let variables_matches = variable_order
            .restrict_to(&self.analysis.body_variables)
            .as_ordered_list();
        let variables_matches_frontier = variable_order
            .restrict_to(&self.frontier_variables)
            .as_ordered_list();
        let matches_frontier_reordering =
            ProjectReordering::from_transformation(&variables_matches, &variables_matches_frontier);

        let node_matches_frontier = current_plan
            .plan_mut()
            .project(node_matches, matches_frontier_reordering);

        // 4. Compute "Unsatisfied Matches Frontier"

        // Matches that are not satisfied are unsatisfied
        let node_unsatisfied_matches_frontier = current_plan
            .plan_mut()
            .minus(node_matches_frontier, node_satisfied_matches_frontier);

        // 5. Compute "Unsatisfied Matches Nulls"

        let node_unsatisfied_matches_nulls = current_plan.plan_mut().append_nulls(
            node_unsatisfied_matches_frontier,
            self.analysis.num_existential,
        );

        current_plan.add_temporary_table(
            node_unsatisfied_matches_nulls.clone(),
            "Head (Restricted): Unsat. Matches",
        );

        let variables_unsatisfied_matches_nulls = append_existential_at_the_end(
            variable_order.restrict_to(&self.frontier_variables),
            &self.analysis.head_variables,
        );

        // 6. For each head atom project from "Unsatisfied Matches Nulls"
        for (&predicate, head_instructions) in self.predicate_to_instructions.iter() {
            let mut final_head_nodes =
                Vec::<ExecutionNodeRef>::with_capacity(head_instructions.len());

            for head_instruction in head_instructions {
                let head_binding = atom_binding(
                    &head_instruction.reduced_atom,
                    &variables_unsatisfied_matches_nulls,
                );
                let head_reordering = ProjectReordering::from_vector(
                    head_binding.clone(),
                    variables_unsatisfied_matches_nulls.len(),
                );

                let project_node = current_plan
                    .plan_mut()
                    .project(node_unsatisfied_matches_nulls.clone(), head_reordering);
                let append_node = current_plan
                    .plan_mut()
                    .append_columns(project_node, head_instruction.append_instructions.clone());

                final_head_nodes.push(append_node);
            }

            let new_tables_union = current_plan.plan_mut().union(final_head_nodes);

            // We just pick the default order
            // TODO: Is there a better pick?
            let result_order = ColumnOrder::default();
            let result_table_name =
                table_manager.generate_table_name(predicate, &result_order, step);
            let result_subtable_id = SubtableIdentifier::new(predicate, step);

            if *self.predicate_to_full_existential.get(&predicate).unwrap() {
                // Since every new entry will contain a fresh null no duplcate elimination is needed
                current_plan.add_permanent_table(
                    new_tables_union,
                    "Head (Restricted): Result Project",
                    &result_table_name,
                    result_subtable_id,
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
                    &result_table_name,
                    result_subtable_id,
                );
            }
        }
    }
}

/// Helper function that puts variables not already contained in the given variable order at the end of it.
fn append_unknown_variables(atoms: &[Atom], mut order: VariableOrder) -> VariableOrder {
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

fn append_existential_at_the_end(
    mut order: VariableOrder,
    variables: &HashSet<Variable>,
) -> VariableOrder {
    for variable in variables {
        if matches!(variable, Variable::Existential(_)) {
            order.push(variable.clone());
        }
    }

    order
}
