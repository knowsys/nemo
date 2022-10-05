//! Functionality which handles the execution of a program
use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use crate::{
    logical::model::{Filter, FilterOperation, Variable},
    physical::tables::{Trie, ValueAssignment},
};

use super::{
    execution_plan::{ExecutionNode, ExecutionOperation, ExecutionPlan, ExecutionResult},
    model::{Atom, Identifier, Literal, Program, Rule, Term},
    table_manager::{ColumnOrder, TableManager, TableManagerStrategy, TableStatus},
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
    pub table_manager: TableManager,
    program: Program,

    rule_infos: Vec<RuleInfo>,
}

impl RuleExecutionEngine {
    /// Create new [`RuleExecutionEngine`]
    pub fn new(memory_strategy: TableManagerStrategy, mut program: Program) -> Self {
        // First, normalize all the rules in the program
        program
            .rules
            .iter_mut()
            .for_each(|r| RuleExecutionEngine::normalize_rule(r));

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

    /// Collects all parts of a table and returns it
    pub fn get_final_table(&mut self, predicate: Identifier, order: ColumnOrder) -> Option<&Trie> {
        let mut union_leaves = Vec::<ExecutionNode>::new();
        let union_node = self.create_subplan_union(
            predicate,
            &(0..self.current_step),
            &order,
            &mut union_leaves,
        );
        let union_plan = ExecutionPlan {
            root: union_node,
            leaves: union_leaves,
            result: ExecutionResult::Save(predicate, 0..self.current_step, order.clone(), 0),
        };

        self.table_manager.execute_series(ExecutionSeries {
            plans: vec![union_plan],
        });

        let new_id = self
            .table_manager
            .get_table(predicate, &(0..self.current_step), &order)
            .ok()?;

        if let TableStatus::InMemory(trie) = &self.table_manager.get_info(new_id).status {
            return Some(trie);
        } else {
            return None;
        }
    }

    // Applies equality filters, e.g., "a(x, y), b(z), y = z" will turn into "a(x, y), b(y)"
    // Also, turns literals like "a(x, 3, x)" into "a(x, y, z), y = 3, z = x"
    fn normalize_rule(rule: &mut Rule) {
        // Apply all equality filters

        // We'll just remove everything from rules.filters and put stuff we want to keep here,
        // so we don't have to delete elements in the vector while iterating on it
        let mut new_filters = Vec::<Filter>::new();
        while !rule.filters.is_empty() {
            let filter = rule.filters.pop().expect("Vector is not empty");

            if filter.operation == FilterOperation::Equals {
                if let Term::Variable(right_variable) = filter.right {
                    for body_literal in &mut rule.body {
                        // TODO: We dont support negation yet
                        if let Literal::Positive(body_atom) = body_literal {
                            for term in &mut body_atom.terms {
                                if let Term::Variable(current_variable) = term {
                                    if *current_variable == filter.left {
                                        *current_variable = right_variable.clone();
                                    }
                                }
                            }
                        }
                    }

                    // Since we apply this filter we don't need to add it to new_filters
                    continue;
                }
            }

            new_filters.push(filter);
        }

        // Don't forget to assign the new filters
        rule.filters = new_filters;

        // Create new filters for handling constants or duplicate variables within one atom

        // TODO: This is horrible and should obviously not work that way,
        // but implementing this properly would need some discussions first
        const FRESH_ID: usize = 10000;
        let mut current_id = FRESH_ID;

        for body_literal in &mut rule.body {
            // TODO: We dont support negation yet
            if let Literal::Positive(body_atom) = body_literal {
                let mut atom_variables = HashSet::new();
                for term in &mut body_atom.terms {
                    let add_filter = if let Term::Variable(variable) = term {
                        // If term is a variable we add a filter iff it has already occured
                        !atom_variables.insert(variable.clone())
                    } else {
                        // If term is not a variable then we need to add a filter
                        true
                    };

                    if add_filter {
                        // Create fresh variable
                        let new_variable = Variable::Universal(Identifier(current_id));
                        current_id += 1;

                        // Add new filter expression
                        rule.filters.push(Filter::new(
                            FilterOperation::Equals,
                            new_variable.clone(),
                            term.clone(),
                        ));

                        // Replace current term with the new variable
                        *term = Term::Variable(new_variable);
                    }
                }
            }
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

        if subnodes.len() == 1 {
            // No need for union if its just element
            subnodes.remove(0)
        } else {
            ExecutionNode {
                operation: ExecutionOperation::Union(subnodes),
            }
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

        if subplans.len() == 1 {
            // No need for join if its just one element
            subplans.remove(0)
        } else {
            ExecutionNode {
                operation: ExecutionOperation::Join(subplans, join_binding.clone()),
            }
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
                    panic!("Only variables are supported.")
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

    fn get_variables(atoms: &[&Atom]) -> HashSet<Variable> {
        let mut result = HashSet::new();
        for atom in atoms {
            for term in atom.terms() {
                if let Term::Variable(v) = term {
                    result.insert(*v);
                }
            }
        }
        result
    }

    fn contains_existential(atom: &Atom) -> bool {
        for term in atom.terms() {
            if let Term::Variable(Variable::Existential(_)) = term {
                return true;
            }
        }

        false
    }

    fn get_frontier_projection(
        vars_initial: &HashSet<Variable>,
        vars_other: &HashSet<Variable>,
        variable_order: &VariableOrder,
    ) -> Vec<usize> {
        #[derive(Copy, Clone, PartialEq)]
        enum VariableCategory {
            Other,
            Initial,
            Both,
        }

        let mut categories = vec![VariableCategory::Other; vars_initial.len()];

        for (variable, position) in &variable_order.0 {
            let in_initial = vars_initial.contains(variable);
            let in_other = vars_other.contains(variable);

            categories[*position] = if in_initial && in_other {
                VariableCategory::Both
            } else if in_initial {
                VariableCategory::Initial
            } else {
                VariableCategory::Other
            }
        }

        let mut initial_counter = 0;
        let mut result = Vec::new();
        for category in categories {
            if category == VariableCategory::Both {
                result.push(initial_counter);
            }

            if category != VariableCategory::Other {
                initial_counter += 1;
            }
        }

        result
    }

    fn create_execution_plan(
        &self,
        rule_id: usize,
        variable_order: &VariableOrder,
    ) -> ExecutionSeries {
        const ID_BODY_JOIN: usize = 0;
        const ID_HEAD_JOIN: usize = 1;
        const ID_BODY_FRONTIER: usize = 2;
        const ID_HEAD_FRONTIER: usize = 3;
        const ID_EXISTENTIAL_DIFF: usize = 4;
        const ID_NEW_TABLES: usize = 5; // Must be one greater than the last constant

        let rule = &self.program.rules[rule_id];

        // Some preliminary calculations...

        // Since we don't support negation yet,
        // we can just turn the literals into atoms
        // TODO: When adding support for negation, change this
        let body_atoms: Vec<&Atom> = rule.body().map(|l| l.atom()).collect();
        let head_atoms: Vec<&Atom> = rule.head().map(|a| a).collect();

        let body_variables = RuleExecutionEngine::get_variables(&body_atoms);
        let head_variables = RuleExecutionEngine::get_variables(&head_atoms);

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

        // Existential rules require a lot more computation
        let is_existential = rule
            .head()
            .map(|a| {
                for t in a.terms() {
                    if let Term::Variable(Variable::Existential(_)) = t {
                        return true;
                    }
                }
                return false;
            })
            .fold(false, |acc, b| acc || b);

        // Calculate the filters
        let mut body_variables_sorted = body_variables.iter().collect::<Vec<_>>();
        body_variables_sorted.sort_by(|a, b| {
            variable_order
                .get(a)
                .unwrap()
                .cmp(variable_order.get(b).unwrap())
        });
        let mut variable_to_columnindex = HashMap::new();
        for (index, variable) in body_variables_sorted.iter().enumerate() {
            variable_to_columnindex.insert(*variable, index);
        }

        let mut filter_assignments = Vec::<ValueAssignment>::new();
        let mut filter_classes = Vec::<HashSet<&Variable>>::new();
        for filter in &rule.filters {
            match &filter.right {
                Term::Variable(right_variable) => {
                    let left_variable = &filter.left;

                    let left_index = filter_classes
                        .iter()
                        .position(|s| s.contains(left_variable));
                    let right_index = filter_classes
                        .iter()
                        .position(|s| s.contains(right_variable));

                    if left_index.is_some() && right_index.is_some() {
                        if right_index.unwrap() > left_index.unwrap() {
                            let other_set = filter_classes.remove(right_index.unwrap());
                            filter_classes[left_index.unwrap()].extend(other_set);
                        } else {
                            let other_set = filter_classes.remove(left_index.unwrap());
                            filter_classes[right_index.unwrap()].extend(other_set);
                        }
                    } else if left_index.is_some() {
                        filter_classes[left_index.unwrap()].insert(right_variable);
                    } else if right_index.is_some() {
                        filter_classes[right_index.unwrap()].insert(left_variable);
                    } else {
                        let mut new_set = HashSet::new();
                        new_set.insert(left_variable);
                        new_set.insert(right_variable);

                        filter_classes.push(new_set);
                    }
                }
                _ => {
                    if let Some(datavalue) = filter.right.to_datavalue_t() {
                        filter_assignments.push(ValueAssignment {
                            column_idx: *variable_to_columnindex.get(&filter.left).unwrap(),
                            value: datavalue,
                        });
                    } else {
                        // TODO: Not sure what to do in this case
                    }
                }
            }
        }
        let filter_classes: Vec<Vec<usize>> = filter_classes
            .iter()
            .map(|s| {
                let mut r = s
                    .iter()
                    .map(|v| *variable_to_columnindex.get(v).unwrap())
                    .collect::<Vec<usize>>();
                r.sort();
                r
            })
            .collect();

        // All the plans will be stored in this vector
        let mut plans = Vec::<ExecutionPlan>::new();

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
        let mut body_node = ExecutionNode {
            operation: ExecutionOperation::Union(body_joins),
        };

        // If there are filters apply them
        if !filter_assignments.is_empty() {
            body_node = ExecutionNode {
                operation: ExecutionOperation::SelectValue(Box::new(body_node), filter_assignments),
            };
        }
        if !filter_classes.is_empty() {
            body_node = ExecutionNode {
                operation: ExecutionOperation::SelectEqual(Box::new(body_node), filter_classes),
            };
        }

        // We want to materialize this table
        // since the results will be derived from it through projection/reordering
        let body_plan = ExecutionPlan {
            root: body_node,
            leaves: body_leaves,
            result: ExecutionResult::Temp(0),
        };

        plans.push(body_plan);

        if is_existential {
            // 1. Compute the join over the head expression
            let mut head_bindings = Vec::new();
            let mut head_join_leaves = Vec::new();
            let mut head_join_subtables = Vec::new();

            for (predicate, (column_order, head_atoms)) in &head_map {
                for head_atom in head_atoms {
                    head_bindings.push(RuleExecutionEngine::calc_binding(
                        head_atom,
                        column_order,
                        variable_order,
                    ));
                    head_join_subtables.push(self.create_subplan_union(
                        *predicate,
                        &(0..self.current_step),
                        column_order,
                        &mut head_join_leaves,
                    ));
                }
            }

            let head_join_node = ExecutionNode {
                operation: ExecutionOperation::Join(head_join_subtables, head_bindings),
            };

            plans.push(ExecutionPlan {
                root: head_join_node,
                leaves: head_join_leaves,
                result: ExecutionResult::Temp(ID_HEAD_JOIN),
            });

            // 2. Project Body join to frontier variables
            let no_nonfroniter = body_variables
                .difference(&head_variables)
                .collect::<HashSet<&Variable>>()
                .is_empty();

            // If every body variable is a frontier variable then no projection is needed
            if !no_nonfroniter {
                let body_projected_columns = RuleExecutionEngine::get_frontier_projection(
                    &body_variables,
                    &head_variables,
                    &variable_order,
                );
                let body_project_node = ExecutionNode {
                    operation: ExecutionOperation::Project(ID_BODY_JOIN, body_projected_columns),
                };

                plans.push(ExecutionPlan {
                    root: body_project_node,
                    leaves: vec![],
                    result: ExecutionResult::Temp(ID_BODY_FRONTIER),
                });
            }

            // 3. Project head join to frontier variables
            let head_projected_columns = RuleExecutionEngine::get_frontier_projection(
                &head_variables,
                &body_variables,
                &variable_order,
            );
            let head_project_node = ExecutionNode {
                operation: ExecutionOperation::Project(ID_BODY_JOIN, head_projected_columns),
            };

            plans.push(ExecutionPlan {
                root: head_project_node,
                leaves: vec![],
                result: ExecutionResult::Temp(ID_HEAD_FRONTIER),
            });

            // 4. Calculate the difference

            // Either take the original body join table or the projected one
            let left_id = if no_nonfroniter {
                ID_BODY_JOIN
            } else {
                ID_BODY_FRONTIER
            };
            let right_id = ID_HEAD_FRONTIER;

            let left_node = ExecutionNode {
                operation: ExecutionOperation::Temp(left_id),
            };
            let right_node = ExecutionNode {
                operation: ExecutionOperation::Temp(right_id),
            };
            let difference_node = ExecutionNode {
                operation: ExecutionOperation::Minus(Box::new(left_node), Box::new(right_node)),
            };

            plans.push(ExecutionPlan {
                root: difference_node,
                leaves: vec![],
                result: ExecutionResult::Temp(ID_EXISTENTIAL_DIFF),
            });

            // 5. Add the new nulls if restricted and I don't know what for skolem chase
            // TODO: ...
        }

        let mut current_temp_id = ID_NEW_TABLES;

        for (predicate, (column_order, head_atoms)) in &head_map {
            // Calculate head tables by projecting/reordering the temporary table resulting from the body

            // For datalog rules we can simply start with the body join table
            // For existential rules we need to find matches for the head
            let projection_base_id = if is_existential {
                ID_EXISTENTIAL_DIFF
            } else {
                ID_BODY_JOIN
            };

            // Execution result for the saved table
            let execution_result = ExecutionResult::Save(
                *predicate,
                self.current_step..self.current_step + 1,
                column_order.clone(),
                0,
            );

            let mut head_nodes = Vec::new();
            let mut head_leaves = Vec::new();
            for head_atom in head_atoms {
                let atom_existential = RuleExecutionEngine::contains_existential(head_atom);

                let head_binding =
                    RuleExecutionEngine::calc_binding(head_atom, column_order, variable_order);
                let project_node = ExecutionNode {
                    operation: ExecutionOperation::Project(projection_base_id, head_binding),
                };

                // If atom is existential then there are no duplicates
                // If, also, we have only one atom with that predicate we need no union
                // Hence, the table can be saved permanently
                if atom_existential && head_atoms.len() == 1 {
                    plans.push(ExecutionPlan {
                        root: project_node,
                        leaves: vec![],
                        // TODO: Without clone, we would get an error stating that value is moved in previous loop iterations
                        // Hoever, this is false since we know that there is only one iteration
                        // How do I tell Rust this?
                        result: execution_result.clone(),
                    });
                }
                // If atom is not existential, we need to remove duplicates
                else if !atom_existential {
                    // Create a temporary materialized version of the table
                    plans.push(ExecutionPlan {
                        root: project_node,
                        leaves: vec![],
                        result: ExecutionResult::Temp(current_temp_id),
                    });
                    let projected_temp = ExecutionNode {
                        operation: ExecutionOperation::Temp(current_temp_id),
                    };
                    current_temp_id += 1;

                    // Collect the results of previous iterations and...
                    let mut previous_leaves = Vec::new();
                    let previous_node = self.create_subplan_union(
                        *predicate,
                        &(0..self.current_step + 1),
                        column_order,
                        &mut previous_leaves,
                    );

                    // ... remove them and ...
                    let final_head_node = ExecutionNode {
                        operation: ExecutionOperation::Minus(
                            Box::new(projected_temp),
                            Box::new(previous_node),
                        ),
                    };

                    // ... materialize the result
                    if head_atoms.len() == 1 {
                        // No union needed and we can save it permanently
                        plans.push(ExecutionPlan {
                            root: final_head_node,
                            leaves: previous_leaves,
                            // TODO: Similar problem as above
                            result: execution_result.clone(),
                        })
                    } else {
                        head_leaves.append(&mut previous_leaves);
                        head_nodes.push(final_head_node);
                    }
                }
            }

            // Still need to do the union
            if head_atoms.len() > 1 {
                let final_head_union = ExecutionNode {
                    operation: ExecutionOperation::Union(head_nodes),
                };

                plans.push(ExecutionPlan {
                    root: final_head_union,
                    leaves: vec![],
                    result: execution_result,
                })
            }
        }

        ExecutionSeries { plans }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        logical::{
            model::{Atom, Identifier, Literal, NumericLiteral, Program, Rule, Term, Variable},
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
            vec![],
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
            vec![],
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

    #[test]
    fn body_constants_repeated_vars() {
        let rule = Rule::new(
            vec![Atom::new(
                Identifier(1),
                vec![Term::Variable(Variable::Universal(Identifier(0)))],
            )],
            vec![
                Literal::Positive(Atom::new(
                    Identifier(2),
                    vec![
                        Term::Variable(Variable::Universal(Identifier(0))),
                        Term::Variable(Variable::Universal(Identifier(0))),
                    ],
                )),
                Literal::Positive(Atom::new(
                    Identifier(3),
                    vec![
                        Term::Variable(Variable::Universal(Identifier(0))),
                        Term::NumericLiteral(NumericLiteral::Integer(3)),
                    ],
                )),
            ],
            vec![],
        );

        let program = Program::new(None, HashMap::new(), Vec::new(), vec![rule], Vec::new());

        let mut engine = RuleExecutionEngine::new(TableManagerStrategy::Unlimited, program);

        let a_column_x = make_gict(&[1, 2, 3, 4, 5], &[0]);
        let a_column_y = make_gict(&[2, 3, 1, 2, 4, 2, 3, 2, 4, 4], &[0, 2, 5, 7, 9]);

        let b_column_x = make_gict(&[1, 2, 3, 4], &[0]);
        let b_column_y = make_gict(&[3, 2, 3, 2, 2, 3], &[0, 1, 3, 4]);

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

        let trie_a = Trie::new(schema.clone(), vec![a_column_x, a_column_y]);
        let trie_b = Trie::new(schema.clone(), vec![b_column_x, b_column_y]);
        engine.add_trie(Identifier(2), 0..1, vec![0, 1], 0, trie_a);
        engine.add_trie(Identifier(3), 0..1, vec![0, 1], 0, trie_b);

        engine.execute();

        let no_five = std::panic::catch_unwind(|| engine.table_manager.get_info(5));
        assert!(no_five.is_err());

        if let TableStatus::InMemory(result_table) = &engine.table_manager.get_info(3).status {
            let first_col = if let IntervalColumnT::U64(col) = result_table.get_column(0) {
                col
            } else {
                unreachable!()
            };

            assert_eq!(
                first_col.get_data_column().iter().collect::<Vec<u64>>(),
                vec![2, 4]
            );
            assert_eq!(
                first_col.get_int_column().iter().collect::<Vec<usize>>(),
                vec![0]
            );
        } else {
            unreachable!()
        }
    }
}
