//! Module defining the strategies used to
//! derive the new facts for a rule application without existential variables in the head.

use std::collections::HashMap;

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Identifier, Rule},
        program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
        table_manager::{SubtableExecutionPlan, SubtableIdentifier},
        TableManager,
    },
    physical::{
        dictionary::Dictionary,
        management::{database::ColumnOrder, execution_plan::ExecutionNodeRef},
        tabular::operations::triescan_project::ProjectReordering,
    },
};

use super::{
    plan_util::{atom_binding, head_instruction_from_atom, HeadInstruction},
    HeadStrategy,
};

/// Strategy for computing the results for a datalog (non-existential) rule.
#[derive(Debug)]
pub struct DatalogStrategy {
    predicate_to_atoms: HashMap<Identifier, Vec<HeadInstruction>>,
    num_body_variables: usize,
}

impl DatalogStrategy {
    /// Create a new [`DatalogStrategy`] object.
    pub fn initialize(rule: &Rule, analysis: &RuleAnalysis) -> Self {
        let mut predicate_to_atoms = HashMap::<Identifier, Vec<HeadInstruction>>::new();

        for head_atom in rule.head() {
            let atoms = predicate_to_atoms
                .entry(head_atom.predicate())
                .or_insert(Vec::new());

            atoms.push(head_instruction_from_atom(head_atom));
        }

        let num_body_variables = analysis.body_variables.len();

        Self {
            predicate_to_atoms,
            num_body_variables,
        }
    }
}

impl<Dict: Dictionary> HeadStrategy<Dict> for DatalogStrategy {
    fn add_plan_head(
        &self,
        table_manager: &TableManager<Dict>,
        current_plan: &mut SubtableExecutionPlan,
        body: ExecutionNodeRef,
        _rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step: usize,
    ) {
        for (&predicate, head_instructions) in self.predicate_to_atoms.iter() {
            // We just pick the default order
            // TODO: Is there a better pick?
            let head_order = ColumnOrder::default();
            let head_table_name = table_manager.generate_table_name(predicate, &head_order, step);

            let execution_plan = current_plan.plan_mut();

            let mut project_append_nodes =
                Vec::<ExecutionNodeRef>::with_capacity(head_instructions.len());
            for head_instruction in head_instructions {
                let head_binding = atom_binding(&head_instruction.reduced_atom, &variable_order);
                let head_reordering =
                    ProjectReordering::from_vector(head_binding.clone(), self.num_body_variables);

                let project_node = execution_plan.project(body.clone(), head_reordering);
                let append_node = execution_plan
                    .append_columns(project_node, head_instruction.append_instructions.clone());

                project_append_nodes.push(append_node);
            }

            let new_tables_union = execution_plan.union(project_append_nodes);

            let old_subtables = table_manager.tables_in_range(predicate, &(0..step));
            let old_table_nodes: Vec<ExecutionNodeRef> = old_subtables
                .into_iter()
                .map(|id| execution_plan.fetch_existing(id))
                .collect();
            let old_table_union = execution_plan.union(old_table_nodes);

            let remove_duplicate_node = execution_plan.minus(new_tables_union, old_table_union);

            current_plan.add_permanent_table(
                remove_duplicate_node,
                "Head (Datalog)",
                &head_table_name,
                SubtableIdentifier::new(predicate, step),
            );
        }
    }
}
