//! Functionality which handles the execution of a program
use std::{collections::HashMap, ops::Range};

use crate::physical::tables::Trie;

use super::{
    execution_plan::{ExecutionNode, ExecutionOperation, ExecutionPlan, ExecutionResult},
    model::{Atom, Identifier, Program, Term},
    table_manager::{ColumnOrder, TableManager, TableManagerStrategy},
    ExecutionSeries,
};

mod variable_order;
use variable_order::{build_preferable_variable_orders, VariableOrder};

#[derive(Debug)]
struct RuleInfo {
    step_last_applied: usize,
    // Maps variables used in the rule to an index
    promising_orders: Vec<VariableOrder>,
}

/// Object which handles the evaluation of the program
#[derive(Debug)]
pub struct RuleExecutionEngine {
    current_step: usize,
    table_manager: TableManager,
    program: Program,

    rule_infos: Vec<RuleInfo>,
}

impl RuleExecutionEngine {
    /// Create new [`RuleExecutionEngine`]
    pub fn new(memory_strategy: TableManagerStrategy, program: Program) -> Self {
        // NOTE: indices are the ids of the rules and the rule order in variable_orders is the same as in program
        let variable_orders = build_preferable_variable_orders(&program);
        let rule_infos = variable_orders
            .into_iter()
            .map(|var_ord| RuleInfo {
                step_last_applied: 0,
                promising_orders: var_ord,
            })
            .collect();

        Self {
            current_step: 1,
            table_manager: TableManager::new(memory_strategy),
            program,
            rule_infos,
        }
    }

    /// Add trie to the table manager; useful for testing
    pub fn add_trie(
        &mut self,
        predicate: Identifier,
        absolute_step_range: Range<usize>,
        column_order: ColumnOrder,
        priority: u64,
        trie: Trie,
    ) {
        self.table_manager
            .add_trie(predicate, absolute_step_range, column_order, priority, trie);
    }

    /// Executes the program
    pub fn execute(&mut self) {
        let mut without_derivation: usize = 0;
        let mut current_rule_index: usize = 0;
        while without_derivation < self.program.rules.len() {
            let promising_orders = &self.rule_infos[current_rule_index].promising_orders;

            // Compute all possible plans from orders
            let mut plans: Vec<ExecutionSeries> = promising_orders
                .iter()
                .map(|o| self.create_execution_plan(current_rule_index, o))
                .collect();

            // Get the index of the one with the minimal costs
            let best_plan_index = plans
                .iter()
                .map(TableManager::estimate_runtime_costs)
                .enumerate()
                .min_by(|(_, a), (_, b)| (*a).cmp(b))
                .map(|(index, _)| index)
                .unwrap();

            // Hopefully, optimizer recognizes that plan is destroyed in this scope anyway and it does not need to do the shifting
            let best_plan = plans.remove(best_plan_index);
            let no_derivation = !self.table_manager.execute_series(best_plan);

            if no_derivation {
                without_derivation += 1;
            } else {
                without_derivation = 0;
                self.rule_infos[current_rule_index].step_last_applied = self.current_step;
            }

            current_rule_index = (current_rule_index + 1) % self.program.rules.len();
            self.current_step += 1;
        }
    }

    fn create_subplan_union(
        &self,
        predicate: Identifier,
        range: &Range<usize>,
        order: &ColumnOrder,
        leaves: &mut Vec<ExecutionNode>,
    ) -> ExecutionNode {
        let mut subnodes = Vec::<ExecutionNode>::new();
        for &block_number in self.table_manager.get_blocks_within_range(predicate, range) {
            let new_node = ExecutionNode {
                operation: ExecutionOperation::Fetch(
                    predicate,
                    block_number..(block_number + 1),
                    order.clone(),
                ),
            };

            leaves.push(new_node.clone());
            subnodes.push(new_node);
        }

        ExecutionNode {
            operation: ExecutionOperation::Union(subnodes),
        }
    }

    fn create_subplan_join(
        &self,
        atoms: &[&Atom],
        atom_column_orders: &[ColumnOrder],
        last_rule_step: usize,
        mid: usize,
        join_binding: &Vec<Vec<usize>>,
        leaves: &mut Vec<ExecutionNode>,
    ) -> ExecutionNode {
        let mut subplans = Vec::<ExecutionNode>::new();
        for (atom_index, atom) in atoms.iter().take(mid).enumerate() {
            subplans.push(self.create_subplan_union(
                atom.predicate(),
                &(0..self.current_step + 1),
                &atom_column_orders[atom_index],
                leaves,
            ))
        }

        subplans.push(self.create_subplan_union(
            atoms[mid].predicate(),
            &(last_rule_step..self.current_step + 1),
            &atom_column_orders[mid],
            leaves,
        ));

        for (atom_index, atom) in atoms.iter().skip(mid + 1).enumerate() {
            subplans.push(self.create_subplan_union(
                atom.predicate(),
                &(0..last_rule_step),
                &atom_column_orders[atom_index],
                leaves,
            ))
        }

        ExecutionNode {
            operation: ExecutionOperation::Join(subplans, join_binding.clone()),
        }
    }

    fn order_atom(atom: &Atom, variable_order: &VariableOrder) -> ColumnOrder {
        let mapped_variables: Vec<usize> = atom
            .terms()
            .map(|t| {
                // TODO: What about constants?
                if let Term::Variable(variable) = t {
                    *variable_order.get(variable).unwrap()
                } else {
                    panic!("Only universal variables are supported.")
                }
            })
            .collect();

        let mut indices = (0..mapped_variables.len()).collect::<Vec<_>>();
        indices.sort_by_key(|&i| mapped_variables[i]);
        indices
    }

    fn calc_binding(
        atom: &Atom,
        column_order: &ColumnOrder,
        variable_order: &VariableOrder,
    ) -> Vec<usize> {
        column_order
            .iter()
            .map(|&i| {
                // TODO: For now this should at least also cover constants
                if let Term::Variable(variable) = &atom.terms[i] {
                    *variable_order.get(variable).unwrap()
                } else {
                    panic!("Only universal variables are supported.");
                }
            })
            .collect()
    }

    fn create_execution_plan(
        &self,
        rule_id: usize,
        variable_order: &VariableOrder,
    ) -> ExecutionSeries {
        let rule = &self.program.rules[rule_id];

        // Some preliminary calculations...

        // Since we don't support negation yet,
        // we can just turn the literals into atoms
        // TODO: When adding support for negation, change this
        let body_atoms: Vec<&Atom> = rule.body().map(|l| l.atom()).collect();

        let atom_column_orders: Vec<ColumnOrder> = body_atoms
            .iter()
            .map(|a| RuleExecutionEngine::order_atom(a, variable_order))
            .collect();
        let join_binding = body_atoms
            .iter()
            .enumerate()
            .map(|(i, a)| {
                RuleExecutionEngine::calc_binding(a, &atom_column_orders[i], variable_order)
            })
            .collect();

        // Map containing, for each predicate in the head,
        // its column order and references to all atoms with that predicate
        let mut head_map = HashMap::<Identifier, (ColumnOrder, Vec<&Atom>)>::new();
        for head_atom in rule.head() {
            match head_map.get_mut(&head_atom.predicate()) {
                Some((_order, atom_vector)) => atom_vector.push(head_atom),
                None => {
                    let order = RuleExecutionEngine::order_atom(head_atom, variable_order);
                    head_map.insert(head_atom.predicate(), (order, vec![head_atom]));
                }
            }
        }

        // First, compute the big join for the body
        let mut body_leaves = Vec::<ExecutionNode>::new();

        // In the seminative evaluation, we need to perform one join per body atom
        let mut body_joins = Vec::<ExecutionNode>::new();
        for body_index in 0..rule.body.len() {
            let subplan = self.create_subplan_join(
                &body_atoms,
                &atom_column_orders,
                self.rule_infos[rule_id].step_last_applied,
                body_index,
                &join_binding,
                &mut body_leaves,
            );

            body_joins.push(subplan);
        }

        // Results of the join have to be combined
        let body_join_union = ExecutionNode {
            operation: ExecutionOperation::Union(body_joins),
        };

        // We want to materialize this table
        // since the results will be derived from it through projection/reordering
        let body_plan = ExecutionPlan {
            root: body_join_union,
            leaves: body_leaves,
            result: ExecutionResult::Temp(0),
        };

        let mut current_temp_id: usize = 1;
        let mut plans = vec![body_plan];

        for (predicate, (column_order, head_atoms)) in &head_map {
            // Calculate head tables by projecting/reordering the temporary table resulting from the body

            let predicate_node = if head_atoms.len() == 1 {
                // If the predicate only appears once then we don't need to do anything special
                let head_binding =
                    RuleExecutionEngine::calc_binding(head_atoms[0], column_order, variable_order);

                let project_node = ExecutionNode {
                    operation: ExecutionOperation::Project(0, head_binding),
                };

                // The project/reorder needs to be materialized, hence it is added to the list of plans with a temprary id
                plans.push(ExecutionPlan {
                    root: project_node,
                    leaves: vec![],
                    result: ExecutionResult::Temp(current_temp_id),
                });
                let temp_node = ExecutionNode {
                    operation: ExecutionOperation::Temp(current_temp_id),
                };
                current_temp_id += 1;

                temp_node
            } else {
                // If predicate appears multiple times then we need to get the union of the result for each atom
                let mut head_nodes = Vec::new();
                for head_atom in head_atoms {
                    let head_binding =
                        RuleExecutionEngine::calc_binding(head_atom, column_order, variable_order);

                    let project_node = ExecutionNode {
                        operation: ExecutionOperation::Project(0, head_binding),
                    };

                    // The project/reorder needs to be materialized, hence it is added to the list of plans with a temprary id
                    plans.push(ExecutionPlan {
                        root: project_node,
                        leaves: vec![],
                        result: ExecutionResult::Temp(current_temp_id),
                    });
                    head_nodes.push(ExecutionNode {
                        operation: ExecutionOperation::Temp(current_temp_id),
                    });
                    current_temp_id += 1;
                }

                ExecutionNode {
                    operation: ExecutionOperation::Union(head_nodes),
                }
            };

            // Now we need to remove duplicates
            let mut duplicate_leaves = Vec::new();
            let duplicates_node = self.create_subplan_union(
                *predicate,
                &(0..self.current_step + 1),
                column_order,
                &mut duplicate_leaves,
            );

            let final_head_node = ExecutionNode {
                operation: ExecutionOperation::Minus(
                    Box::new(predicate_node),
                    Box::new(duplicates_node),
                ),
            };

            // Finally, push the resulting plan
            plans.push(ExecutionPlan {
                root: final_head_node,
                leaves: duplicate_leaves,
                result: ExecutionResult::Save(
                    *predicate,
                    self.current_step..self.current_step + 1,
                    column_order.clone(),
                    0,
                ),
            });
        }

        ExecutionSeries { plans }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        logical::{
            model::{Atom, Identifier, Literal, Program, Rule, Term, Variable},
            table_manager::{TableManagerStrategy, TableStatus},
        },
        physical::{
            columns::{Column, IntervalColumnT},
            datatypes::DataTypeName,
            tables::{Trie, TrieSchema, TrieSchemaEntry},
            util::make_gict,
        },
    };

    use super::RuleExecutionEngine;

    #[test]
    fn test_trans_closure() {
        let trans_rule = Rule::new(
            vec![Atom::new(
                Identifier(0),
                vec![
                    Term::Variable(Variable::Universal(Identifier(0))),
                    Term::Variable(Variable::Universal(Identifier(2))),
                ],
            )],
            vec![
                Literal::Positive(Atom::new(
                    Identifier(0),
                    vec![
                        Term::Variable(Variable::Universal(Identifier(0))),
                        Term::Variable(Variable::Universal(Identifier(1))),
                    ],
                )),
                Literal::Positive(Atom::new(
                    Identifier(0),
                    vec![
                        Term::Variable(Variable::Universal(Identifier(1))),
                        Term::Variable(Variable::Universal(Identifier(2))),
                    ],
                )),
            ],
        );

        let program = Program::new(
            None,
            HashMap::new(),
            Vec::new(),
            vec![trans_rule],
            Vec::new(),
        );

        let mut engine = RuleExecutionEngine::new(TableManagerStrategy::Unlimited, program);

        let column_x = make_gict(&[1, 2, 5, 7], &[0]);
        let column_y = make_gict(&[2, 3, 5, 10, 4, 7, 10, 9, 8, 9, 10], &[0, 4, 7, 8]);

        let schema = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 10,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 11,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie = Trie::new(schema, vec![column_x, column_y]);
        engine.add_trie(Identifier(0), 0..1, vec![0, 1], 0, trie);

        engine.execute();

        let no_three = std::panic::catch_unwind(|| engine.table_manager.get_info(3));
        assert!(no_three.is_err());

        if let TableStatus::InMemory(zeroth_table) = &engine.table_manager.get_info(0).status {
            let first_col = if let IntervalColumnT::U64(col) = zeroth_table.get_column(0) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                first_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![1, 2, 5, 7]
            );
            assert_eq!(
                first_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0]
            );

            let second_col = if let IntervalColumnT::U64(col) = zeroth_table.get_column(1) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                second_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![2, 3, 5, 10, 4, 7, 10, 9, 8, 9, 10]
            );
            assert_eq!(
                second_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0, 4, 7, 8]
            );
        } else {
            unreachable!()
        }
        if let TableStatus::InMemory(first_table) = &engine.table_manager.get_info(1).status {
            let first_col = if let IntervalColumnT::U64(col) = first_table.get_column(0) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                first_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![1, 2]
            );
            assert_eq!(
                first_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0]
            );

            let second_col = if let IntervalColumnT::U64(col) = first_table.get_column(1) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                second_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![4, 7, 9, 8, 9]
            );
            assert_eq!(
                second_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0, 3]
            );
        } else {
            unreachable!()
        }
        if let TableStatus::InMemory(second_table) = &engine.table_manager.get_info(2).status {
            let first_col = if let IntervalColumnT::U64(col) = second_table.get_column(0) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                first_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![1]
            );
            assert_eq!(
                first_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0]
            );

            let second_col = if let IntervalColumnT::U64(col) = second_table.get_column(1) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                second_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![8]
            );
            assert_eq!(
                second_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0]
            );
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_trans_closure_multihead() {
        let trans_rule = Rule::new(
            vec![
                Atom::new(
                    Identifier(0),
                    vec![
                        Term::Variable(Variable::Universal(Identifier(0))),
                        Term::Variable(Variable::Universal(Identifier(2))),
                    ],
                ),
                Atom::new(
                    Identifier(0),
                    vec![
                        Term::Variable(Variable::Universal(Identifier(2))),
                        Term::Variable(Variable::Universal(Identifier(0))),
                    ],
                ),
            ],
            vec![
                Literal::Positive(Atom::new(
                    Identifier(0),
                    vec![
                        Term::Variable(Variable::Universal(Identifier(0))),
                        Term::Variable(Variable::Universal(Identifier(1))),
                    ],
                )),
                Literal::Positive(Atom::new(
                    Identifier(0),
                    vec![
                        Term::Variable(Variable::Universal(Identifier(1))),
                        Term::Variable(Variable::Universal(Identifier(2))),
                    ],
                )),
            ],
        );

        let program = Program::new(
            None,
            HashMap::new(),
            Vec::new(),
            vec![trans_rule],
            Vec::new(),
        );

        let mut engine = RuleExecutionEngine::new(TableManagerStrategy::Unlimited, program);

        let column_x = make_gict(&[1, 2, 5, 7], &[0]);
        let column_y = make_gict(&[2, 3, 5, 10, 4, 7, 10, 9, 8, 9, 10], &[0, 4, 7, 8]);

        let schema = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 10,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 11,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie = Trie::new(schema, vec![column_x, column_y]);
        engine.add_trie(Identifier(0), 0..1, vec![0, 1], 0, trie);

        engine.execute();

        let no_four = std::panic::catch_unwind(|| engine.table_manager.get_info(4));
        assert!(no_four.is_err());

        if let TableStatus::InMemory(zeroth_table) = &engine.table_manager.get_info(0).status {
            let first_col = if let IntervalColumnT::U64(col) = zeroth_table.get_column(0) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                first_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![1, 2, 5, 7]
            );
            assert_eq!(
                first_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0]
            );

            let second_col = if let IntervalColumnT::U64(col) = zeroth_table.get_column(1) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                second_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![2, 3, 5, 10, 4, 7, 10, 9, 8, 9, 10]
            );
            assert_eq!(
                second_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0, 4, 7, 8]
            );
        } else {
            unreachable!()
        }
        if let TableStatus::InMemory(first_table) = &engine.table_manager.get_info(1).status {
            let first_col = if let IntervalColumnT::U64(col) = first_table.get_column(0) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                first_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![1, 2, 4, 7, 8, 9, 10]
            );
            assert_eq!(
                first_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0]
            );

            let second_col = if let IntervalColumnT::U64(col) = first_table.get_column(1) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                second_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![4, 7, 9, 8, 9, 1, 1, 2, 1, 2, 1, 2]
            );
            assert_eq!(
                second_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0, 3, 5, 6, 7, 8, 10]
            );
        } else {
            unreachable!()
        }
        if let TableStatus::InMemory(second_table) = &engine.table_manager.get_info(2).status {
            let first_col = if let IntervalColumnT::U64(col) = second_table.get_column(0) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                first_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![1, 2, 3, 4, 5, 7, 8, 9, 10]
            );
            assert_eq!(
                first_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0]
            );

            let second_col = if let IntervalColumnT::U64(col) = second_table.get_column(1) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                second_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![
                    1, 8, 1, 2, 5, 4, 7, 9, 10, 2, 3, 4, 5, 7, 8, 9, 10, 1, 2, 4, 7, 10, 2, 3, 4,
                    5, 7, 1, 4, 7, 8, 9, 10, 3, 4, 5, 7, 8, 9, 10, 3, 4, 5, 7, 8, 9, 10
                ]
            );
            assert_eq!(
                second_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0, 2, 5, 9, 17, 22, 27, 33, 40]
            );
        } else {
            unreachable!()
        }
        if let TableStatus::InMemory(third_table) = &engine.table_manager.get_info(3).status {
            let first_col = if let IntervalColumnT::U64(col) = third_table.get_column(0) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                first_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![2, 3, 5, 8]
            );
            assert_eq!(
                first_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0]
            );

            let second_col = if let IntervalColumnT::U64(col) = third_table.get_column(1) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                second_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![3, 1, 2, 3, 5, 8, 3, 5, 8, 3, 5]
            );
            assert_eq!(
                second_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0, 1, 6, 9]
            );
        } else {
            unreachable!()
        }
    }
}
