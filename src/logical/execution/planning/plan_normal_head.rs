//! Module defining the strategies used to derive the new facts for a rule application.

use std::collections::HashMap;

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Atom, Identifier, Rule},
        program_analysis::{analysis::NormalRuleAnalysis, variable_order::VariableOrder},
        table_manager::{ColumnOrder, TableKey},
        TableManager,
    },
    physical::{
        management::execution_plan::{ExecutionNodeRef, ExecutionResult, ExecutionTree},
        tabular::operations::triescan_join::JoinBinding,
    },
};

use super::plan_util::{join_binding, BODY_JOIN};

/// Strategies for calculating the newly derived tables.
pub trait HeadStrategy<'a> {
    /// Do preperation work for the planning phase.
    fn initialize(rule: &'a Rule, analysis: &'a NormalRuleAnalysis) -> Self;

    /// Calculate the concrete plan given a variable order.
    fn execution_tree(
        &self,
        table_manager: &TableManager,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> Vec<ExecutionTree<TableKey>>;
}

/// Strategy for computing the results for a datalog (non-existential) rule.
#[derive(Debug)]
pub struct DatalogStrategy<'a> {
    predicate_to_atoms: HashMap<Identifier, Vec<&'a Atom>>,
}

impl<'a> HeadStrategy<'a> for DatalogStrategy<'a> {
    fn initialize(rule: &'a Rule, _analysis: &'a NormalRuleAnalysis) -> Self {
        let mut predicate_to_atoms = HashMap::<Identifier, Vec<&'a Atom>>::new();

        for head_atom in rule.head() {
            let atoms = predicate_to_atoms
                .entry(head_atom.predicate())
                .or_insert(Vec::new());

            atoms.push(head_atom);
        }

        Self { predicate_to_atoms }
    }

    fn execution_tree(
        &self,
        table_manager: &TableManager,
        _rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> Vec<ExecutionTree<TableKey>> {
        let mut trees = Vec::<ExecutionTree<TableKey>>::new();

        for (&predicate, head_atoms) in self.predicate_to_atoms.iter() {
            let predicate_arity = head_atoms[0].terms().len();

            // We just pick the default order
            // TODO: Is there a better pick?
            //          * One could try to save a projection the head is just a reordering of the body match
            //          * However, one would still need to reorder for the duplicate elimination ...
            let head_order = ColumnOrder::default(predicate_arity);

            let head_bindings: JoinBinding = head_atoms
                .iter()
                .map(|&a| join_binding(a, &ColumnOrder::default(predicate_arity), &variable_order))
                .collect();

            let head_table_name =
                table_manager.get_table_name(predicate, step_number..step_number + 1);
            let head_table_key =
                TableKey::from_name(head_table_name, ColumnOrder::default(predicate_arity));
            let mut head_tree =
                ExecutionTree::<TableKey>::new(ExecutionResult::Save(head_table_key));

            let mut project_nodes =
                Vec::<ExecutionNodeRef<TableKey>>::with_capacity(head_atoms.len());
            for head_atom_index in 0..head_atoms.len() {
                let head_binding = &head_bindings[head_atom_index];

                let fetch_node = head_tree.fetch_temp(BODY_JOIN);
                let project_node = head_tree.project(fetch_node, head_binding.clone());

                project_nodes.push(project_node);
            }

            let new_tables_union = head_tree.union(project_nodes);

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
