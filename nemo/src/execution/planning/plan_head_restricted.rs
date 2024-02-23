//! Module defining the strategies used to
//! derive the new facts for a rule application with existential variables in the head.

use std::collections::{HashMap, HashSet};

use nemo_physical::{
    management::{
        database::id::PermanentTableId,
        execution_plan::{ColumnOrder, ExecutionNodeRef},
    },
    tabular::operations::OperationTable,
};

use crate::{
    execution::{
        execution_engine::RuleInfo,
        planning::operations::{
            append::node_head_instruction, filter::node_filter, join::node_join,
            union::subplan_union,
        },
        rule_execution::VariableTranslation,
    },
    model::{
        chase_model::{ChaseAtom, ChaseRule, VariableAtom},
        Constraint, Identifier, PrimitiveTerm, Variable,
    },
    program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
    table_manager::{SubtableExecutionPlan, SubtableIdentifier, TableManager},
};

use super::{
    operations::append::{head_instruction_from_atom, HeadInstruction},
    HeadStrategy,
};

/// Strategy for the restricted chase.
#[derive(Debug)]
pub(crate) struct RestrictedChaseStrategy {
    /// Atoms for computing the table "new satisfied matches"
    head_join_atoms: Vec<VariableAtom>,
    /// Constraints associated with computing the table "new satisfied matches"
    head_join_constraints: Vec<Constraint>,

    predicate_to_instructions: HashMap<Identifier, Vec<HeadInstruction>>,
    predicate_to_full_existential: HashMap<Identifier, bool>,

    /// The calculation of "new statified matches" is represented by an auxillary rule
    /// "head -> aux_predicate(frontier_variables)".
    /// This is the order of those variables
    aux_head_order: VariableOrder,
    /// This is the predicate of the auxillary table containing the "satisfied matches"
    aux_predicate: Identifier,

    analysis: RuleAnalysis,
}

impl RestrictedChaseStrategy {
    /// Create a new [`RestrictedChaseStrategy`] object.
    pub(crate) fn initialize(rule: &ChaseRule, analysis: &RuleAnalysis) -> Self {
        let mut predicate_to_instructions = HashMap::<Identifier, Vec<HeadInstruction>>::new();
        let mut predicate_to_full_existential = HashMap::<Identifier, bool>::new();

        for head_atom in rule.head() {
            let is_existential = head_atom
                .terms()
                .iter()
                .any(|t| matches!(t, PrimitiveTerm::Variable(Variable::Existential(_))));

            let instructions = predicate_to_instructions
                .entry(head_atom.predicate())
                .or_default();
            instructions.push(head_instruction_from_atom(head_atom));

            let is_full_existential = predicate_to_full_existential
                .entry(head_atom.predicate())
                .or_insert(true);
            *is_full_existential &= is_existential;
        }

        let head_join_atoms = analysis.existential_aux_rule.positive_body().clone();
        let head_join_constraints = analysis.existential_aux_rule.positive_constraints().clone();

        let aux_head = &analysis.existential_aux_rule.head()[0];
        let mut aux_head_order = VariableOrder::new();
        let mut used_join_head_variables = HashSet::<Variable>::new();
        for term in aux_head.terms() {
            if let PrimitiveTerm::Variable(variable) = term {
                aux_head_order.push(variable.clone());
                used_join_head_variables.insert(variable.clone());
            } else {
                unreachable!("This atom should only conist of variables");
            }
        }

        let aux_predicate = aux_head.predicate();

        RestrictedChaseStrategy {
            head_join_atoms,
            head_join_constraints,
            predicate_to_instructions,
            predicate_to_full_existential,
            analysis: analysis.clone(),
            aux_predicate,
            aux_head_order,
        }
    }
}

impl HeadStrategy for RestrictedChaseStrategy {
    fn add_plan_head(
        &self,
        table_manager: &TableManager,
        current_plan: &mut SubtableExecutionPlan,
        variable_translation: &VariableTranslation,
        node_matches: ExecutionNodeRef,
        rule_info: &RuleInfo,
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
        //   * It is important to keep in mind that the entries "Unsatisfied Matches Frontier" (see below) will
        //     become satisfied after this rule application;
        //     hence can be appended to "new satisfied matches frontier" in this round and will not have to be recomputed next time.
        //   * The difference of "Matches Frontier" and "Satisfied Matches Frontier"
        //     results in a table that contains the frontier assignments for the current unsatsified matches
        //     [We refer to this subtable as "Unsatisfied Matches Frontier"]
        //   * We append to "Unsatisfied Matches Frontier" a number of columns with null values
        //     [We refer to this subtable as "Unsatisfied Matches Nulls"]
        //   * For each atom in the head:
        //     If it contains an existential variable: Project from "Unsatisfied Matches Nulls" and append constants when needed
        //     If it does not contain an existential: Project from "Unsatisfied Matches Nulls", append constants and perform duplicate elimation

        log::info!(
            "Existential Head Join Variable Order: {:?}",
            self.analysis.existential_aux_order
        );

        // 1. Compute the table "New Satisfied Matches"

        // For each rule we remember the step it was last applied in.
        // This information is used to split the subtables of a predicate into two groups
        // -- "old" and "new" with the goal of reducing duplicates while computing the join.
        // For the body join, any table derived in the current application of the rule
        // should be considered as a new table in the next (viz. applying the same rule twice in a row).
        // Here we compute the new satisfied matches since the last rule application.
        // However, after applying a rule at step i, every unsatisfied (at step i) match will be satisified in step i + 1.
        // This allows us to add the unsatisfied matches of the current rule execution
        // to the permanent table of satisfied matches for this rule step (see `node_newer_satisfied_matches_frontier` at step 5).
        // As a result, we do not consider the tables derived in step i as new tables in step i + 1.
        let step_last_applied = if rule_info.step_last_applied == 0 {
            // An exception to the above is the case where the rule was never applied.
            // Here we need to mark every table as new
            0
        } else {
            rule_info.step_last_applied + 1
        };

        // We use the same precomputed variable order every time
        let markers_new_satisfied_matches = variable_translation
            .operation_table(self.analysis.existential_aux_order.as_ordered_list().iter());

        let node_new_satisfied_matches = node_join(
            current_plan.plan_mut(),
            table_manager,
            variable_translation,
            step_last_applied,
            step,
            &self.head_join_atoms,
            markers_new_satisfied_matches,
        );

        let node_new_satisfied_matches = node_filter(
            current_plan.plan_mut(),
            variable_translation,
            node_new_satisfied_matches,
            &self.head_join_constraints,
        );

        current_plan.add_temporary_table(
            node_new_satisfied_matches.clone(),
            "Head (Restricted): Satisifed",
        );

        // 2. Compute the table "Satisfied Matches Frontier"

        // Same as `markers_new_satisfied_matches` but without non-fronier variables
        let markers_satisifed_matches_frontier =
            variable_translation.operation_table(self.aux_head_order.as_ordered_list().iter());

        let node_new_satisfied_matches_frontier = current_plan.plan_mut().projectreorder(
            markers_satisifed_matches_frontier.clone(),
            node_new_satisfied_matches,
        );

        // The above node represents the new satisfied matches which might still contain duplicates
        // In the following they are removed
        let node_old_satisfied_matches_frontier = subplan_union(
            current_plan.plan_mut(),
            table_manager,
            &self.aux_predicate,
            0..step,
            markers_satisifed_matches_frontier.clone(),
        );
        let node_new_satisfied_matches_frontier = current_plan.plan_mut().subtract(
            node_new_satisfied_matches_frontier,
            vec![node_old_satisfied_matches_frontier.clone()],
        );

        current_plan.add_temporary_table(
            node_new_satisfied_matches_frontier.clone(),
            "Head (Restricted): Sat. Frontier",
        );

        let mut node_satisfied_matches_frontier = current_plan
            .plan_mut()
            .union_empty(markers_satisifed_matches_frontier.clone());
        node_satisfied_matches_frontier.add_subnode(node_old_satisfied_matches_frontier);
        node_satisfied_matches_frontier.add_subnode(node_new_satisfied_matches_frontier.clone());

        // 3. Compute "Matches Frontier"

        let node_matches_frontier = current_plan
            .plan_mut()
            .projectreorder(markers_satisifed_matches_frontier.clone(), node_matches);

        // 4. Compute "Unsatisfied Matches Frontier"

        // Matches that are not satisfied are unsatisfied
        let node_unsatisfied_matches_frontier = current_plan
            .plan_mut()
            .subtract(node_matches_frontier, vec![node_satisfied_matches_frontier]);

        // 5. Save the newly computed "Satisfied matches frontier"

        let node_newer_satisfied_matches_frontier = current_plan.plan_mut().union(
            markers_satisifed_matches_frontier,
            vec![
                node_new_satisfied_matches_frontier,
                node_unsatisfied_matches_frontier.clone(),
            ],
        );

        current_plan.add_permanent_table(
            node_newer_satisfied_matches_frontier,
            "Head (Restricted): Updated Sat. Frontier",
            "Restricted Chase Helper Table",
            SubtableIdentifier::new(self.aux_predicate.clone(), step),
        );

        // 6. Compute "Unsatisfied Matches Nulls"

        let variables_unsatisfied_matches_nulls = append_existential_at_the_end(
            self.aux_head_order.clone(),
            &self.analysis.head_variables,
        );
        let markers_unsatisfied_matches_nulls = variable_translation
            .operation_table(variables_unsatisfied_matches_nulls.as_ordered_list().iter());

        let node_unsatisfied_matches_nulls = current_plan.plan_mut().null(
            markers_unsatisfied_matches_nulls.clone(),
            node_unsatisfied_matches_frontier,
        );

        current_plan.add_temporary_table(
            node_unsatisfied_matches_nulls.clone(),
            "Head (Restricted): Unsat. Matches",
        );

        // 7. For each head atom project from "Unsatisfied Matches Nulls"
        for (predicate, head_instructions) in self.predicate_to_instructions.iter() {
            let arity = head_instructions
                .first()
                .map(|instruction| instruction.arity)
                .unwrap_or(0);
            let result_markers = OperationTable::new_unique(arity);

            let final_head_nodes = head_instructions
                .iter()
                .map(|head_instruction| {
                    node_head_instruction(
                        current_plan.plan_mut(),
                        variable_translation,
                        node_unsatisfied_matches_nulls.clone(),
                        head_instruction,
                    )
                })
                .collect();

            let new_tables_union = current_plan
                .plan_mut()
                .union(result_markers.clone(), final_head_nodes);

            let result_table_name =
                table_manager.generate_table_name(predicate, &ColumnOrder::default(), step);
            let result_subtable_id = SubtableIdentifier::new(predicate.clone(), step);

            if *self.predicate_to_full_existential.get(predicate).unwrap() {
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
                let old_tables: Vec<PermanentTableId> =
                    table_manager.tables_in_range(predicate, &(0..step));
                let old_table_nodes: Vec<ExecutionNodeRef> = old_tables
                    .into_iter()
                    .map(|id| {
                        current_plan
                            .plan_mut()
                            .fetch_table(OperationTable::default(), id)
                    })
                    .collect();
                let old_table_union = current_plan
                    .plan_mut()
                    .union(result_markers, old_table_nodes);

                let remove_duplicate_node = current_plan
                    .plan_mut()
                    .subtract(new_tables_union, vec![old_table_union]);

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
