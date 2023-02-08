//! Module defining the strategies used to
//! derive the new facts for a rule application without existential variables in the head.

use std::collections::HashMap;

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Identifier, Rule},
        program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
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
    plan_util::{atom_binding, head_instruction_from_atom, HeadInstruction, BODY_JOIN},
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
    fn execution_tree(
        &self,
        table_manager: &TableManager<Dict>,
        _rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> Vec<ExecutionTree<TableKey>> {
        let mut trees = Vec::<ExecutionTree<TableKey>>::new();

        for (&predicate, head_instructions) in self.predicate_to_atoms.iter() {
            let predicate_arity = head_instructions[0].arity;
            // We just pick the default order
            // TODO: Is there a better pick?
            let head_order = ColumnOrder::default(predicate_arity);

            let head_table_name =
                table_manager.get_table_name(predicate, step_number..step_number + 1);
            let head_table_key = TableKey::from_name(head_table_name, head_order.clone());
            let mut head_tree = ExecutionTree::<TableKey>::new(
                "Head (Datalog)".to_string(),
                ExecutionResult::Save(head_table_key),
            );

            let mut project_append_nodes =
                Vec::<ExecutionNodeRef<TableKey>>::with_capacity(head_instructions.len());
            for head_instruction in head_instructions {
                let head_binding = atom_binding(
                    &head_instruction.reduced_atom,
                    &Reordering::default(head_instruction.reduced_atom.terms().len()),
                    &variable_order,
                );
                let head_reordering =
                    Reordering::new(head_binding.clone(), self.num_body_variables);

                let fetch_node = head_tree.fetch_temp(BODY_JOIN);
                let project_node = head_tree.project(fetch_node, head_reordering);
                let append_node = head_tree
                    .append_columns(project_node, head_instruction.append_instructions.clone());

                project_append_nodes.push(append_node);
            }

            let new_tables_union = head_tree.union(project_append_nodes);

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

            trees.push(head_tree);
        }

        trees
    }
}
