//! Module defining the strategy for calculating all body matches for a rule application.

use std::ops::Range;

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Atom, Filter, Identifier, Rule},
        program_analysis::{analysis::NormalRuleAnalysis, variable_order::VariableOrder},
        table_manager::{ColumnOrder, TableKey},
        TableManager,
    },
    physical::{
        management::execution_plan::{ExecutionNodeRef, ExecutionResult, ExecutionTree},
        tabular::operations::triescan_join::JoinBinding,
    },
};

use super::plan_util::{filters, join_binding, order_atom, BODY_JOIN};

/// Strategies for calculating all body matches.
pub trait BodyStrategy<'a> {
    /// Do preperation work for the planning phase.
    fn initialize(rule: &'a Rule, analysis: &'a NormalRuleAnalysis) -> Self;

    /// Calculate the concrete plan given a variable order.
    fn execution_tree(
        &self,
        table_manager: &TableManager,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> ExecutionTree<TableKey>;
}

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub struct SeminaiveStrategy<'a> {
    body: Vec<&'a Atom>,
    filters: Vec<&'a Filter>,

    analysis: &'a NormalRuleAnalysis,
}

impl<'a> SeminaiveStrategy<'a> {
    // Tries to figure out what the best
    fn best_atom_permutation(&self) -> Vec<usize> {
        // For now, just do nothing
        // TODO: Do something
        (0..self.body.len()).collect()
    }

    // Calculate a subtree consisting of a union of in-memory tables.
    fn subtree_union(
        &self,
        tree: &mut ExecutionTree<TableKey>,
        manager: &TableManager,
        predicate: Identifier,
        steps: Range<usize>,
        order: &ColumnOrder,
    ) -> ExecutionNodeRef<TableKey> {
        let base_tables: Vec<TableKey> = manager
            .get_table_covering(predicate, steps)
            .into_iter()
            .map(|r| TableKey::new(predicate, r, order.clone()))
            .collect();

        let mut union_node = tree.union_empty();
        for key in base_tables {
            let base_node = tree.fetch_table(key);
            union_node.add_subnode(base_node);
        }

        union_node
    }

    // Calculate a subtree consiting of join representing one variant of an seminaive evaluation.
    fn subtree_join(
        &self,
        tree: &mut ExecutionTree<TableKey>,
        manager: &TableManager,
        atoms: &[&Atom],
        atom_orders: &[ColumnOrder],
        rule_step: usize,
        overall_step: usize,
        mid: usize,
        join_binding: &JoinBinding,
    ) -> ExecutionNodeRef<TableKey> {
        let mut join_node = tree.join_empty(join_binding.clone());

        // For every atom before the mid point we take all the tables until the current `rule_step`
        for (atom_index, atom) in atoms.iter().take(mid).enumerate() {
            let subnode = self.subtree_union(
                tree,
                manager,
                atom.predicate(),
                0..overall_step,
                &atom_orders[atom_index],
            );

            join_node.add_subnode(subnode);
        }

        // For the middle atom we only take the new tables
        let midnode = self.subtree_union(
            tree,
            manager,
            atoms[mid].predicate(),
            rule_step..overall_step,
            &atom_orders[mid],
        );

        join_node.add_subnode(midnode);

        // For every atom past the mid point we take only the old tables
        for (atom_index, atom) in atoms.iter().enumerate().skip(mid + 1) {
            let subnode = self.subtree_union(
                tree,
                manager,
                atom.predicate(),
                0..rule_step,
                &atom_orders[atom_index],
            );

            join_node.add_subnode(subnode);
        }

        join_node
    }
}

impl<'a> BodyStrategy<'a> for SeminaiveStrategy<'a> {
    fn initialize(rule: &'a Rule, analysis: &'a NormalRuleAnalysis) -> Self {
        // Since we don't support negation yet, we can just turn the literals into atoms
        // TODO: Think about negation here
        let body: Vec<&Atom> = rule.body().iter().map(|l| l.atom()).collect();
        let filters = rule.filters().iter().map(|f| f).collect::<Vec<&Filter>>();

        Self {
            body,
            filters,
            analysis,
        }
    }

    fn execution_tree(
        &self,
        table_manager: &TableManager,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> ExecutionTree<TableKey> {
        let atom_permutation = self.best_atom_permutation();
        let body_reordered: Vec<&Atom> = atom_permutation.iter().map(|&i| self.body[i]).collect();

        let atom_orders: Vec<ColumnOrder> = body_reordered
            .iter()
            .map(|&a| order_atom(a, &variable_order))
            .collect();
        let join_binding: JoinBinding = body_reordered
            .iter()
            .enumerate()
            .map(|(i, &a)| join_binding(a, &atom_orders[i], &variable_order))
            .collect();

        let mut tree = ExecutionTree::<TableKey>::new(
            "Body Join".to_string(),
            ExecutionResult::Temp(BODY_JOIN),
        );

        let mut seminaive_union = tree.union_empty();
        for atom_index in 0..body_reordered.len() {
            let seminaive_node = self.subtree_join(
                &mut tree,
                table_manager,
                &body_reordered,
                &atom_orders,
                rule_info.step_last_applied,
                step_number,
                atom_index,
                &join_binding,
            );

            seminaive_union.add_subnode(seminaive_node);
        }

        let mut root_node = seminaive_union;

        if self.analysis.has_filters {
            let (filter_classes, filter_assignments) = filters(
                &self.analysis.body_variables,
                &variable_order,
                &self.filters,
            );

            if !filter_assignments.is_empty() {
                root_node = tree.select_value(root_node, filter_assignments);
            }

            if !filter_classes.is_empty() {
                root_node = tree.select_equal(root_node, filter_classes);
            }
        }

        tree.set_root(root_node);

        tree
    }
}
