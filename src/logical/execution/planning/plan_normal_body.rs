//! Module defining the strategy for calculating all body matches for a rule application.

use std::ops::Range;

use itertools::Itertools;

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Atom, Filter, Identifier, Rule},
        program_analysis::{analysis::NormalRuleAnalysis, variable_order::VariableOrder},
        table_manager::{ColumnOrder, TableKey},
        TableManager,
    },
    meta::logging::log_choose_atom_order,
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
    fn best_atom_permutation(
        &self,
        table_manager: &TableManager,
        atoms: &[&Atom],
        last_applied_step: usize,
        current_step: usize,
    ) -> ColumnOrder {
        // Maximum number of random permutations we check
        const MAX_NUM_PERMUTATIONS: usize = 3 * 2;

        if atoms.len() == 1 {
            return ColumnOrder::default(1);
        }

        let count_old: Vec<usize> = atoms
            .iter()
            .map(|&a| {
                table_manager
                    .get_table_covering(a.predicate(), 0..last_applied_step)
                    .len()
            })
            .collect();

        let count_new: Vec<usize> = atoms
            .iter()
            .map(|&a| {
                table_manager
                    .get_table_covering(a.predicate(), last_applied_step..current_step)
                    .len()
            })
            .collect();

        let count_all: Vec<usize> = atoms
            .iter()
            .map(|&a| {
                table_manager
                    .get_table_covering(a.predicate(), 0..current_step)
                    .len()
            })
            .collect();

        let inidices: Vec<usize> = (0..atoms.len()).collect();
        let mut best_perm = inidices.clone();
        let mut best_value = f64::INFINITY;
        let mut worst_value: f64 = 0.0;

        let mut counter: usize = 0;
        for permutation in inidices.iter().permutations(inidices.len()) {
            counter += 1;

            // Note: We do these calculations with floating point numbers
            // since using integers limits the number of multiplications one can do before overflowinf serverly
            // Because this is a heuristic anayways, a loss in precision is not as bad
            let mut value: f64 = 0.0;
            for mid_point in 0..atoms.len() {
                let mut product: f64 = 1.0;
                for atom_index in 0..atoms.len() {
                    let permuted_index = *permutation[atom_index];

                    if permuted_index < mid_point {
                        // Take all
                        product *= count_all[atom_index] as f64;
                    } else if permuted_index == mid_point {
                        // Take new
                        product *= count_new[atom_index] as f64;
                    } else {
                        // Take old
                        product *= count_old[atom_index] as f64;
                    }
                }

                value += product;
            }

            if value < best_value {
                best_perm = permutation.into_iter().map(|i| *i).collect();
                best_value = value;
            }

            if value > worst_value {
                worst_value = value;
            }

            if counter >= MAX_NUM_PERMUTATIONS {
                break;
            }
        }

        log_choose_atom_order(best_value, worst_value);

        ColumnOrder::new(best_perm)
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
        side_atoms: &[&Atom],
        main_atoms: &[&Atom],
        side_orders: &[ColumnOrder],
        main_orders: &[ColumnOrder],
        rule_step: usize,
        overall_step: usize,
        mid: usize,
        join_binding: &JoinBinding,
    ) -> ExecutionNodeRef<TableKey> {
        let mut join_node = tree.join_empty(join_binding.clone());

        // For every atom that did not receive any update since the last rule application take all available elements
        for (atom_index, atom) in side_atoms.iter().enumerate() {
            let subnode = self.subtree_union(
                tree,
                manager,
                atom.predicate(),
                0..rule_step,
                &side_orders[atom_index],
            );

            join_node.add_subnode(subnode);
        }

        // For every atom before the mid point we take all the tables until the current `rule_step`
        for (atom_index, atom) in main_atoms.iter().take(mid).enumerate() {
            let subnode = self.subtree_union(
                tree,
                manager,
                atom.predicate(),
                0..overall_step,
                &main_orders[atom_index],
            );

            join_node.add_subnode(subnode);
        }

        // For the middle atom we only take the new tables
        let midnode = self.subtree_union(
            tree,
            manager,
            main_atoms[mid].predicate(),
            rule_step..overall_step,
            &main_orders[mid],
        );

        join_node.add_subnode(midnode);

        // For every atom past the mid point we take only the old tables
        for (atom_index, atom) in main_atoms.iter().enumerate().skip(mid + 1) {
            let subnode = self.subtree_union(
                tree,
                manager,
                atom.predicate(),
                0..rule_step,
                &main_orders[atom_index],
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
        let mut tree = ExecutionTree::<TableKey>::new(
            "Body Join".to_string(),
            ExecutionResult::Temp(BODY_JOIN),
        );

        // We divide the atoms of the body into two parts:
        //    * Main: Those atom who received new elements since the last rule application
        let mut body_side = Vec::<&Atom>::new();
        let mut body_main = Vec::<&Atom>::new();

        for &atom in self.body.iter() {
            if let Some(last_step) = table_manager.predicate_last_step(atom.predicate()) {
                if last_step < rule_info.step_last_applied {
                    body_side.push(atom);
                } else {
                    body_main.push(atom);
                }
            } else {
                return tree;
            }
        }

        if body_main.is_empty() {
            return tree;
        }

        // Order of the main body atoms matters so we try to find the best one and reorder them accordingly
        let atom_permutation = self.best_atom_permutation(
            table_manager,
            &body_main,
            rule_info.step_last_applied,
            step_number,
        );
        let body_reordered: Vec<&Atom> = atom_permutation.apply_to(&body_main);

        // The variable order forces a specific [`ColumnOrder`] on each table
        // Needs to be done for the main and side atoms
        let main_orders: Vec<ColumnOrder> = body_reordered
            .iter()
            .map(|&a| order_atom(a, &variable_order))
            .collect();
        let side_orders: Vec<ColumnOrder> = body_side
            .iter()
            .map(|&a| order_atom(a, &variable_order))
            .collect();

        // Join binding needs to be computed for both types of atoms as well
        let side_binding = body_side
            .iter()
            .enumerate()
            .map(|(i, &a)| join_binding(a, &side_orders[i], &variable_order));
        let main_binding = body_reordered
            .iter()
            .enumerate()
            .map(|(i, &a)| join_binding(a, &main_orders[i], &variable_order));
        // We then combine the bindings into one
        let join_binding: JoinBinding = side_binding.chain(main_binding).collect();

        // Now we can finally calculate the execution tree
        let mut seminaive_union = tree.union_empty();
        for atom_index in 0..body_reordered.len() {
            let seminaive_node = self.subtree_join(
                &mut tree,
                table_manager,
                &body_side,
                &body_reordered,
                &side_orders,
                &main_orders,
                rule_info.step_last_applied,
                step_number,
                atom_index,
                &join_binding,
            );

            seminaive_union.add_subnode(seminaive_node);
        }

        let mut root_node = seminaive_union;

        // As a last step we apply the filters, if there are any
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
