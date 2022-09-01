use std::ops::Range;

use super::{
    execution_plan::{ExecutionOperation, ExecutionPlan},
    model::{Identifier, Literal, Program, Rule},
    table_manager::{TableId, TableIdentifier, TableManager, TableManagerStrategy, TableStatus},
};

struct RuleInfo {
    step_last_applied: usize,
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
        Self {
            current_step: 0,
            table_manager: TableManager::new(memory_strategy),
            program,
            rule_infos: program
                .rules
                .iter()
                .map(|r| RuleInfo {
                    step_last_applied: 0,
                })
                .collect(),
        }
    }

    // Executes the program
    pub fn execute(&mut self) {
        let mut without_derivation: usize = 0;
        let mut current_rule_index: usize = 0;
        while without_derivation < self.program.rules.len() {
            let current_rule = &self.program.rules[current_rule_index];

            let mut no_derivation = false;
            for (head_index, head) in current_rule.head().enumerate() {
                let current_plan = self.create_execution_plan(current_rule_index, head_index);
                let current_identifier = TableIdentifier {
                    predicate_id: head.predicate(),
                    step_range: (self.current_step..self.current_step + 1),
                    variable_order: Vec::new(), // TODO: Set this properly
                };

                let new_table_id = self.table_manager.add_idb(current_plan, current_identifier);
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

    fn create_subplan_union(&self, predicate: Identifier, range: Range<usize>) -> ExecutionPlan {
        let id_vector = Vec::<TableId>::new();

        for (identifier, id) in self.table_manager.get_tables_by_predicate(predicate) {
            if identifier.step_range.end - identifier.step_range.start == 1
                && identifier.step_range.start >= range.start
                && identifier.step_range.start < range.end
            {
                id_vector.push(*id);
            }
        }

        ExecutionPlan {
            operation: ExecutionOperation::Union(
                id_vector
                    .iter()
                    .map(|id| ExecutionPlan {
                        operation: ExecutionOperation::Fetch(*id),
                        target_status: TableStatus::Derived, // Maybe we should have Status: Dont Change
                        target_priority: 0,                  // Same here
                    })
                    .collect(),
            ),
            target_priority: 0,
            target_status: TableStatus::Derived,
        }
    }

    fn create_subplan_join(
        &self,
        literals: &[Literal],
        last_rule_step: usize,
        mid: usize,
    ) -> ExecutionPlan {
        let subplans = Vec::<ExecutionPlan>::new();
        for before_mid_index in 0..mid {
            subplans.push(self.create_subplan_union(
                literals[before_mid_index].atom().predicate(),
                0..self.current_step + 1,
            ))
        }

        subplans.push(self.create_subplan_union(
            literals[mid].atom().predicate(),
            last_rule_step..self.current_step + 1,
        ));

        for after_mid_index in (mid + 1)..literals.len() {
            subplans.push(self.create_subplan_union(
                literals[after_mid_index].atom().predicate(),
                0..last_rule_step,
            ))
        }

        ExecutionPlan {
            operation: ExecutionOperation::Join(subplans, ()),
            target_status: TableStatus::Derived,
            target_priority: 0, //TODO: Priority
        }
    }

    fn create_execution_plan(&self, rule_id: usize, head_index: usize) -> ExecutionPlan {
        let rule = &self.program.rules[rule_id];
        let head_predicate = rule.head[head_index].predicate();

        let subjoins = Vec::<ExecutionPlan>::new();
        for body_index in 0..rule.body.len() {
            subjoins.push(self.create_subplan_join(
                &rule.body,
                self.rule_infos[rule_id].step_last_applied,
                body_index,
            ));
        }

        let temp_plan = ExecutionPlan {
            operation: ExecutionOperation::Union(subjoins),
            target_status: TableStatus::Derived,
            target_priority: 0,
        };

        let duplicates_plan = self.create_subplan_union(head_predicate, 0..self.current_step + 1);

        ExecutionPlan {
            operation: ExecutionOperation::Minus(vec![temp_plan, duplicates_plan]),
            target_status: TableStatus::InMemory,
            target_priority: 0,
        }
    }
}
