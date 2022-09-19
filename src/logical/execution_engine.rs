//! Functionality which handles the execution of a program
use std::{ops::Range, slice};

use crate::physical::tables::Trie;

use super::{
    execution_plan::{ExecutionNode, ExecutionOperation, ExecutionPlan},
    model::{Atom, Identifier, Program, Term},
    table_manager::{ColumnOrder, TableManager, TableManagerStrategy},
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
            let current_rule = &self.program.rules[current_rule_index];
            let promising_orders = &self.rule_infos[current_rule_index].promising_orders;

            let mut no_derivation = false;
            for (head_index, head) in current_rule.head().enumerate() {
                // Compute all plans from orders
                let mut plans: Vec<ExecutionPlan> = promising_orders
                    .iter()
                    .map(|o| self.create_execution_plan(current_rule_index, head_index, o))
                    .collect();

                // Get the index of the one with the minimal costs
                let best_plan_index = plans
                    .iter()
                    .map(|p| self.table_manager.estimate_runtime_costs(p))
                    .enumerate()
                    .min_by(|(_, a), (_, b)| (*a).cmp(b))
                    .map(|(index, _)| index)
                    .unwrap();

                // Hopefully, optimizer recognizes that plan is destroyed in this scope anyway and it does not need to do the shifting
                let best_plan = plans.remove(best_plan_index);
                let best_map = &promising_orders[best_plan_index];

                let head_order = RuleExecutionEngine::order_atom(head, best_map);

                let new_table_id = self.table_manager.add_idb(
                    head.predicate(),
                    self.current_step..self.current_step + 1,
                    head_order,
                    0, // TODO: How to determine this
                    best_plan,
                );

                if new_table_id.is_none() {
                    no_derivation = true;
                }
            }

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

    fn order_atom(atom: &Atom, variable_map: &VariableOrder) -> ColumnOrder {
        let mapped_variables: Vec<usize> = atom
            .terms()
            .map(|t| {
                // TODO: For now this should at least also cover constants
                if let Term::Variable(variable) = t {
                    *variable_map.get(variable).unwrap()
                } else {
                    panic!("Only universal variables are supported.")
                }
            })
            .collect();

        let mut indices = (0..mapped_variables.len()).collect::<Vec<_>>();
        indices.sort_by_key(|&i| mapped_variables[i]);
        indices
    }

    fn calc_binding(atoms: &[&Atom], variable_map: &VariableOrder) -> Vec<Vec<usize>> {
        let mut result = Vec::new();

        for atom in atoms {
            let order = RuleExecutionEngine::order_atom(atom, variable_map);
            result.push(
                order
                    .iter()
                    .map(|&i| {
                        // TODO: For now this should at least also cover constants
                        if let Term::Variable(variable) = &atom.terms[i] {
                            *variable_map.get(variable).unwrap()
                        } else {
                            panic!("Only universal variables are supported.");
                        }
                    })
                    .collect(),
            );
        }

        result
    }

    fn create_subplan_join(
        &self,
        atoms: &[&Atom],
        last_rule_step: usize,
        mid: usize,
        variable_map: &VariableOrder,
        leaves: &mut Vec<ExecutionNode>,
    ) -> ExecutionNode {
        let mut subplans = Vec::<ExecutionNode>::new();
        for atom in atoms.iter().take(mid) {
            subplans.push(self.create_subplan_union(
                atom.predicate(),
                &(0..self.current_step + 1),
                &RuleExecutionEngine::order_atom(atom, variable_map),
                leaves,
            ))
        }

        subplans.push(self.create_subplan_union(
            atoms[mid].predicate(),
            &(last_rule_step..self.current_step + 1),
            &RuleExecutionEngine::order_atom(atoms[mid], variable_map),
            leaves,
        ));

        for atom in atoms.iter().skip(mid + 1) {
            subplans.push(self.create_subplan_union(
                atom.predicate(),
                &(0..last_rule_step),
                &RuleExecutionEngine::order_atom(atom, variable_map),
                leaves,
            ))
        }

        ExecutionNode {
            operation: ExecutionOperation::Join(
                subplans,
                RuleExecutionEngine::calc_binding(atoms, variable_map),
            ),
        }
    }

    fn create_execution_plan(
        &self,
        rule_id: usize,
        head_index: usize,
        variable_map: &VariableOrder,
    ) -> ExecutionPlan {
        let rule = &self.program.rules[rule_id];

        let head_predicate = rule.head[head_index].predicate();
        let body_atoms: Vec<&Atom> = rule.body().map(|l| l.atom()).collect();

        let mut leaves = Vec::<ExecutionNode>::new();

        let mut subjoins = Vec::<ExecutionNode>::new();
        for body_index in 0..rule.body.len() {
            subjoins.push(self.create_subplan_join(
                &body_atoms,
                self.rule_infos[rule_id].step_last_applied,
                body_index,
                variable_map,
                &mut leaves,
            ));
        }

        let temp_plan = ExecutionNode {
            operation: ExecutionOperation::Union(subjoins),
        };

        let head_binding = RuleExecutionEngine::calc_binding(
            slice::from_ref(&&rule.head[head_index]),
            variable_map,
        )
        .remove(0);

        let project_plan = ExecutionNode {
            operation: ExecutionOperation::Project(head_binding),
        };

        let head_order = RuleExecutionEngine::order_atom(&rule.head[head_index], variable_map);
        let duplicates_plan = self.create_subplan_union(
            head_predicate,
            &(0..self.current_step + 1),
            &head_order,
            &mut leaves,
        );

        let tmp_node = ExecutionNode {
            operation: ExecutionOperation::Temp(),
        };

        let minus_plan = ExecutionNode {
            operation: ExecutionOperation::Minus(vec![tmp_node, duplicates_plan]),
        };

        ExecutionPlan {
            roots: vec![temp_plan, project_plan, minus_plan],
            leaves,
        }
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
}
