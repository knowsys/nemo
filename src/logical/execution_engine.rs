use std::{collections::HashMap, ops::Range};

use super::{
    execution_plan::{ExecutionNode, ExecutionOperation, ExecutionPlan},
    model::{Atom, Identifier, Program, Rule, Term},
    table_manager::{TableManager, TableManagerStrategy, VariableOrder},
};

struct RuleInfo {
    step_last_applied: usize,
    // Maps variables used in the rule to an index
    // TODO: Maybe this type should be called VariableOrder instead of Vec<usize>?
    promising_orders: Vec<HashMap<Identifier, usize>>,
}

struct RuleExecutionEngine {
    current_step: usize,
    table_manager: TableManager,
    program: Program,

    rule_infos: Vec<RuleInfo>,
}

impl RuleExecutionEngine {
    /// Create new [`RuleExecutionEngine`]
    pub fn new(memory_strategy: TableManagerStrategy, program: Program) -> Self {
        let rule_infos = program
            .rules()
            .map(|r| RuleInfo {
                step_last_applied: 0,
                promising_orders: RuleExecutionEngine::calc_good_variable_orders(r),
            })
            .collect();

        Self {
            current_step: 0,
            table_manager: TableManager::new(memory_strategy),
            program,
            rule_infos,
        }
    }

    // TODO: Return, you know, good variable orders
    fn calc_good_variable_orders(rule: &Rule) -> Vec<HashMap<Identifier, usize>> {
        Vec::new()
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
                if self.table_manager.table_is_empty(new_table_id) {
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
        }

        self.current_step += 1;
    }

    fn create_subplan_union(
        &self,
        predicate: Identifier,
        range: &Range<usize>,
        order: &VariableOrder,
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

    fn order_atom(atom: &Atom, variable_map: &HashMap<Identifier, usize>) -> VariableOrder {
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
        indices.sort_by_key(|&i| &mapped_variables[i]);

        indices
    }

    fn calc_binding(atoms: &[&Atom], variable_map: &HashMap<Identifier, usize>) -> Vec<Vec<usize>> {
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
        head: &Atom,
        last_rule_step: usize,
        mid: usize,
        variable_map: &HashMap<Identifier, usize>,
        leaves: &mut Vec<ExecutionNode>,
    ) -> ExecutionNode {
        let mut subplans = Vec::<ExecutionNode>::new();
        for before_mid_index in 0..mid {
            subplans.push(self.create_subplan_union(
                atoms[before_mid_index].predicate(),
                &(0..self.current_step + 1),
                &RuleExecutionEngine::order_atom(&atoms[before_mid_index], variable_map),
                leaves,
            ))
        }

        subplans.push(self.create_subplan_union(
            atoms[mid].predicate(),
            &(last_rule_step..self.current_step + 1),
            &RuleExecutionEngine::order_atom(&atoms[mid], variable_map),
            leaves,
        ));

        for after_mid_index in (mid + 1)..atoms.len() {
            subplans.push(self.create_subplan_union(
                atoms[after_mid_index].predicate(),
                &(0..last_rule_step),
                &RuleExecutionEngine::order_atom(&atoms[after_mid_index], variable_map),
                leaves,
            ))
        }

        let head_order = RuleExecutionEngine::calc_binding(&[head], variable_map)
            .into_iter()
            .nth(0)
            .unwrap();

        ExecutionNode {
            operation: ExecutionOperation::Join(
                subplans,
                RuleExecutionEngine::calc_binding(atoms, variable_map),
                head_order,
            ),
        }
    }

    fn create_execution_plan(
        &self,
        rule_id: usize,
        head_index: usize,
        variable_map: &HashMap<Identifier, usize>,
    ) -> ExecutionPlan {
        let rule = &self.program.rules[rule_id];

        let head_predicate = rule.head[head_index].predicate();
        let body_atoms: Vec<&Atom> = rule.body().map(|l| l.atom()).collect();

        let mut leaves = Vec::<ExecutionNode>::new();

        let mut subjoins = Vec::<ExecutionNode>::new();
        for body_index in 0..rule.body.len() {
            subjoins.push(self.create_subplan_join(
                &body_atoms,
                &rule.head[head_index],
                self.rule_infos[rule_id].step_last_applied,
                body_index,
                variable_map,
                &mut leaves,
            ));
        }

        let temp_plan = ExecutionNode {
            operation: ExecutionOperation::Union(subjoins),
        };

        let head_order = RuleExecutionEngine::order_atom(&rule.head[head_index], variable_map);
        let duplicates_plan = self.create_subplan_union(
            head_predicate,
            &(0..self.current_step + 1),
            &head_order,
            &mut leaves,
        );

        let minus_node = ExecutionNode {
            operation: ExecutionOperation::Minus(vec![temp_plan, duplicates_plan]),
        };

        ExecutionPlan {
            root: minus_node,
            leaves,
        }
    }
}
