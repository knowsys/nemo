//! Module defining the strategies used to
//! derive the new facts for a rule application without existential variables in the head.

use std::collections::HashMap;

use nemo_physical::{
    management::execution_plan::{ColumnOrder, ExecutionNodeRef},
    tabular::operations::OperationTable,
};

use crate::{
    chase_model::{
        analysis::program_analysis::RuleAnalysis,
        components::{atom::ChaseAtom, order::ChaseOrder, rule::ChaseRule},
    },
    execution::{execution_engine::RuleInfo, rule_execution::VariableTranslation},
    rule_model::components::tag::Tag,
    table_manager::{SubtableExecutionPlan, SubtableIdentifier, TableManager},
};

use super::{
    operations::{
        append::{head_instruction_from_atom, node_head_instruction, HeadInstruction},
        operation::operation_term_to_function_tree,
    },
    HeadStrategy,
};

/// Strategy for computing the results for a datalog (non-existential) rule.
#[derive(Debug)]
pub(crate) struct DatalogStrategy {
    predicate_to_atoms: HashMap<Tag, Vec<(HeadInstruction, bool)>>,
    predicate_to_update: HashMap<Tag, ChaseOrder>,

    predicate_to_order_variable_translation: HashMap<Tag, VariableTranslation>,
}

impl DatalogStrategy {
    /// Create a new [DatalogStrategy] object.
    pub(crate) fn initialize(rule: &ChaseRule, _analysis: &RuleAnalysis) -> Self {
        let mut predicate_to_atoms = HashMap::<Tag, Vec<(HeadInstruction, bool)>>::new();

        for (head_index, head_atom) in rule.head().iter().enumerate() {
            let is_aggregate_atom = if let Some(aggregate_index) = rule.aggregate_head_index() {
                aggregate_index == head_index
            } else {
                false
            };

            let atoms = predicate_to_atoms.entry(head_atom.predicate()).or_default();
            atoms.push((head_instruction_from_atom(head_atom), is_aggregate_atom));
        }

        let predicate_to_update = rule.orders().clone();

        let mut predicate_to_order_variable_translation =
            HashMap::<Tag, VariableTranslation>::new();
        for (predicate, order) in rule.orders() {
            let mut translation = VariableTranslation::new();
            for variable in order
                ._variables_dominating()
                .iter()
                .chain(order._variables_dominated().iter())
                .cloned()
            {
                translation.add_marker(variable);
            }

            predicate_to_order_variable_translation.insert(predicate.clone(), translation);
        }

        Self {
            predicate_to_atoms,
            predicate_to_update,
            predicate_to_order_variable_translation,
        }
    }
}

impl HeadStrategy for DatalogStrategy {
    fn add_plan_head(
        &self,
        table_manager: &TableManager,
        current_plan: &mut SubtableExecutionPlan,
        variable_translation: &VariableTranslation,
        body: ExecutionNodeRef,
        aggregates: Option<ExecutionNodeRef>,
        _rule_info: &RuleInfo,
        step: usize,
    ) {
        for (predicate, head_instructions) in self.predicate_to_atoms.iter() {
            let head_table_name =
                table_manager.generate_table_name(predicate, &ColumnOrder::default(), step);
            let arity = head_instructions
                .first()
                .map(|(instruction, _)| instruction.arity)
                .unwrap_or(0);

            let project_append_nodes = head_instructions
                .iter()
                .map(|(head_instruction, is_aggregate_atom)| {
                    let base_node = if *is_aggregate_atom {
                        aggregates
                            .clone()
                            .expect("There must be an aggregate node in this case")
                    } else {
                        body.clone()
                    };

                    node_head_instruction(
                        current_plan.plan_mut(),
                        variable_translation,
                        base_node,
                        head_instruction,
                    )
                })
                .collect();

            let new_tables_union = current_plan
                .plan_mut()
                .union(OperationTable::new_unique(arity), project_append_nodes);

            let old_subtables = table_manager.tables_in_range(predicate, &(0..step));
            let old_table_nodes: Vec<ExecutionNodeRef> = old_subtables
                .into_iter()
                .map(|id| {
                    current_plan
                        .plan_mut()
                        .fetch_table(OperationTable::default(), id)
                })
                .collect();

            let final_node = if let Some(filter) = self.predicate_to_update.get(predicate) {
                let variable_translation = self
                    .predicate_to_order_variable_translation
                    .get(predicate)
                    .expect("");

                let filter = operation_term_to_function_tree(variable_translation, filter.filter());

                current_plan.plan_mut().update(
                    filter,
                    new_tables_union, // TODO: What with multihead rules?
                    old_table_nodes,
                )
            } else {
                let old_table_union = current_plan
                    .plan_mut()
                    .union(OperationTable::new_unique(arity), old_table_nodes);

                current_plan
                    .plan_mut()
                    .subtract(new_tables_union, vec![old_table_union])
            };

            // let update_node = current_plan.plan_mut().update(marked_columns, subnode)

            current_plan.add_permanent_table(
                final_node,
                "Duplicate Elimination (Datalog)",
                &head_table_name,
                SubtableIdentifier::new(predicate.clone(), step),
            );
        }
    }
}
