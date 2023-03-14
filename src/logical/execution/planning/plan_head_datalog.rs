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
        management::{
            database::ColumnOrder,
            execution_plan::{ExecutionNodeRef, ExecutionTree},
        },
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
    fn add_head_trees(
        &self,
        table_manager: &TableManager<Dict>,
        current_plan: &mut SubtableExecutionPlan,
        body_id: usize,
        _rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step: usize,
    ) {
        for (&predicate, head_instructions) in self.predicate_to_atoms.iter() {
            // We just pick the default order
            // TODO: Is there a better pick?
            let head_order = ColumnOrder::default();
            let head_table_name = table_manager.generate_table_name(predicate, &head_order, step);

            let mut head_tree = ExecutionTree::new_permanent("Head (Datalog)", &head_table_name);

            let mut project_append_nodes =
                Vec::<ExecutionNodeRef>::with_capacity(head_instructions.len());
            for head_instruction in head_instructions {
                let head_binding = atom_binding(&head_instruction.reduced_atom, &variable_order);
                let head_reordering = ProjectReordering::from_vector(head_binding.clone());

                let fetch_node = head_tree.fetch_new(body_id);
                let project_node = head_tree.project(fetch_node, head_reordering);
                let append_node = head_tree
                    .append_columns(project_node, head_instruction.append_instructions.clone());

                project_append_nodes.push(append_node);
            }

            let new_tables_union = head_tree.union(project_append_nodes);

            let old_subtables = table_manager.tables_in_range(predicate, &(0..step));
            let old_table_nodes: Vec<ExecutionNodeRef> = old_subtables
                .into_iter()
                .map(|id| head_tree.fetch_existing(id))
                .collect();
            let old_table_union = head_tree.union(old_table_nodes);

            let remove_duplicate_node = head_tree.minus(new_tables_union, old_table_union);
            head_tree.set_root(remove_duplicate_node);

            current_plan.add_permanent_table(head_tree, SubtableIdentifier::new(predicate, step));
        }
    }
}
