// NOTE: Generation of variable orders and the filter functions are taken from
// https://github.com/phil-hanisch/rulewerk/blob/lftj/rulewerk-lftj/src/main/java/org/semanticweb/rulewerk/lftj/implementation/Heuristic.java
// NOTE: some functions are slightly modified but the overall idea is reflected

use crate::logical::Permutator;
use num::rational::Ratio;
use std::collections::{HashMap, HashSet};

use super::super::{
    model::{Identifier, Literal, Program, Rule, Variable},
    table_manager::ColumnOrder,
};

#[repr(transparent)]
#[derive(Debug, PartialEq)]
pub(super) struct VariableOrder(pub HashMap<Variable, usize>);

impl VariableOrder {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn push(&mut self, variable: Variable) {
        let max_index = self.0.values().max();
        self.0.insert(variable, max_index.map_or(0, |i| i + 1));
    }

    pub(super) fn get(&self, variable: &Variable) -> Option<&usize> {
        self.0.get(variable)
    }

    fn contains(&self, variable: &Variable) -> bool {
        self.0.contains_key(variable)
    }

    fn iter(&self) -> impl Iterator<Item = &Variable> {
        let mut vars: Vec<&Variable> = self.0.keys().collect();
        vars.sort_by_key(|var| {
            self.0
                .get(var)
                .expect("we are iterating over existing keys")
        });
        vars.into_iter()
    }

    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

enum IterationOrder {
    Forward,
    Backward,
    InsideOut,
}

impl IterationOrder {
    fn get_permutator(&self, len: usize) -> Permutator {
        match self {
            Self::Forward => Permutator::sort_from_vec(&(0..len).collect::<Vec<_>>()),
            Self::Backward => Permutator::sort_from_vec(&(0..len).rev().collect::<Vec<_>>()),
            Self::InsideOut => Permutator::sort_from_vec(
                &(0..len / 2)
                    .rev()
                    .zip((len / 2)..len)
                    .flat_map(|(l, r)| vec![l, r])
                    .collect::<Vec<_>>(),
            ),
        }
    }
}

fn column_order_for(lit: &Literal, var_order: &VariableOrder) -> ColumnOrder {
    var_order
        .iter()
        .flat_map(|var| {
            lit.variables()
                .enumerate()
                .filter(move |(_, lit_var)| lit_var == var)
                .map(|(i, _)| i)
        })
        .collect()
}

trait RuleVariableList {
    fn filter_cartesian_product(
        self,
        partial_var_order: &VariableOrder,
        rule: &Rule,
    ) -> Vec<Variable>;

    fn filter_tries<P: FnMut(&Identifier) -> bool>(
        self,
        partial_var_order: &VariableOrder,
        rule: &Rule,
        required_trie_column_orders: &HashMap<Identifier, HashSet<ColumnOrder>>,
        predicate_filter: P,
    ) -> Vec<Variable>;
}

impl RuleVariableList for Vec<Variable> {
    fn filter_cartesian_product(
        self,
        partial_var_order: &VariableOrder,
        rule: &Rule,
    ) -> Vec<Variable> {
        let result: Vec<Variable> = self
            .iter()
            .filter(|var| {
                rule.body().any(|lit| {
                    let predicate_vars: Vec<Variable> = lit.atom().variables().collect();

                    predicate_vars.iter().any(|pred_var| pred_var == *var)
                        && predicate_vars
                            .iter()
                            .any(|pred_var| partial_var_order.contains(pred_var))
                })
            })
            .copied()
            .collect();

        if result.is_empty() {
            self
        } else {
            result
        }
    }

    fn filter_tries<P: FnMut(&Identifier) -> bool>(
        self,
        partial_var_order: &VariableOrder,
        rule: &Rule,
        required_trie_column_orders: &HashMap<Identifier, HashSet<ColumnOrder>>,
        mut predicate_filter: P,
    ) -> Vec<Variable> {
        let ratios: Vec<Ratio<usize>> = self
            .iter()
            .map(|var| {
                let mut extended_var_order: VariableOrder = partial_var_order.clone();
                extended_var_order.push(*var);

                let literals = rule
                    .body()
                    .filter(|lit| predicate_filter(&lit.predicate()))
                    .filter(|lit| lit.variables().any(|lit_var| lit_var == *var));

                let (total_literals, literals_requiring_new_orders) =
                    literals.fold((0, 0), |acc, lit| {
                        let new_col_order_required: bool = required_trie_column_orders
                            .get(&lit.predicate())
                            .map(|set| set.contains(&column_order_for(lit, &extended_var_order)))
                            .unwrap_or(false);

                        (acc.0 + 1, acc.1 + usize::from(new_col_order_required))
                        // bool is coverted to 1 for true and 0 for false
                    });

                if total_literals == 0 {
                    Ratio::new(0, 1) // literals_requiring_new_orders == 0 here since literals_requiring_new_orders <= total_literals
                } else {
                    Ratio::new(literals_requiring_new_orders, total_literals)
                }
            })
            .collect();

        let min_ratio: Option<Ratio<usize>> = ratios.iter().min().copied();
        self.into_iter()
            .zip(ratios.into_iter())
            .filter(move |(_, ratio)| {
                *ratio == min_ratio.expect("the vars and therefore the ratios are non-empty")
            })
            .map(|(var, _)| var)
            .collect()
    }
}

struct VariableOrderBuilder<'a> {
    program: &'a Program,
    iteration_order_within_rule: IterationOrder,
    required_trie_column_orders: HashMap<Identifier, HashSet<ColumnOrder>>, // maps predicates to sets of column orders
    idb_preds: HashSet<Identifier>,
}

impl<'a> VariableOrderBuilder<'a> {
    fn build_for(
        program: &Program,
        iteration_order_within_rule: IterationOrder,
    ) -> Vec<VariableOrder> {
        let mut builder = VariableOrderBuilder {
            program,
            iteration_order_within_rule,
            required_trie_column_orders: HashMap::new(),
            idb_preds: program.idb_predicates(),
        };

        builder.generate_variable_orders()
    }

    fn get_already_present_idb_edb_count_for_rule_in_tries(&self, rule: &Rule) -> (usize, usize) {
        let preds_with_tries = rule.body().filter_map(|lit| {
            let pred = lit.predicate();
            self.required_trie_column_orders
                .get(&pred)
                .map_or(false, |orders| !orders.is_empty())
                .then_some(pred)
        });

        let (idb_count, edb_count) = preds_with_tries.fold((0, 0), |(idb, edb), pred| {
            if self.idb_preds.contains(&pred) {
                (idb + 1, edb)
            } else {
                (idb, edb + 1)
            }
        });

        (idb_count, edb_count)
    }

    fn generate_variable_orders(&mut self) -> Vec<VariableOrder> {
        let mut remaining_rules: HashMap<usize, &Rule> =
            self.program.rules.iter().enumerate().collect();

        let mut result: Vec<(usize, VariableOrder)> = Vec::with_capacity(self.program.rules.len());

        while !remaining_rules.is_empty() {
            let (next_index, next_rule) = remaining_rules
                .iter()
                .max_by(|(_, rule_a), (_, rule_b)| {
                    let (idb_count_a, edb_count_a) =
                        self.get_already_present_idb_edb_count_for_rule_in_tries(rule_a);
                    let (idb_count_b, edb_count_b) =
                        self.get_already_present_idb_edb_count_for_rule_in_tries(rule_b);

                    (idb_count_a != idb_count_b)
                        .then(|| idb_count_a.cmp(&idb_count_b))
                        .unwrap_or_else(|| edb_count_a.cmp(&edb_count_b))
                })
                .expect("the remaining rules are never empty here");

            let next_index = *next_index;
            let var_order = self.generate_variable_order_for_rule(next_rule);
            remaining_rules.remove(&next_index);
            result.push((next_index, var_order));
        }

        result.sort_by_key(|(i, _)| *i);
        result.into_iter().map(|(_, ord)| ord).collect()
    }

    fn generate_variable_order_for_rule(&mut self, rule: &Rule) -> VariableOrder {
        let mut variable_order: VariableOrder = VariableOrder::new();
        let mut remaining_vars = {
            let remaining_vars_unpermutated: Vec<Variable> = rule
                .body()
                .flat_map(|lit| lit.variables())
                .fold(vec![], |mut acc, var| {
                    if !acc.contains(&var) {
                        acc.push(var);
                    }

                    acc
                });

            self.iteration_order_within_rule
                .get_permutator(remaining_vars_unpermutated.len())
                .permutate(&remaining_vars_unpermutated)
                .expect("we are checking the length so everything should work out")
        };

        while !remaining_vars.is_empty() {
            let next_var = {
                remaining_vars
                    .clone()
                    .filter_cartesian_product(&variable_order, rule)
                    .filter_tries(
                        &variable_order,
                        rule,
                        &self.required_trie_column_orders,
                        |pred| self.idb_preds.contains(pred),
                    )
                    .filter_tries(
                        &variable_order,
                        rule,
                        &self.required_trie_column_orders,
                        |pred| !self.idb_preds.contains(pred),
                    )
                    .first()
                    .copied()
                    .expect("the filter results are guaranteed to be non-empty")
            };

            variable_order.push(next_var);
            remaining_vars.retain(|var| var != &next_var);
            self.update_trie_column_orders(&variable_order, HashSet::from([next_var]), rule);
        }

        variable_order
    }

    fn update_trie_column_orders(
        &mut self,
        variable_order: &VariableOrder,
        must_contain: HashSet<Variable>,
        rule: &Rule,
    ) {
        let literals = rule.body().filter(|lit| {
            let vars: Vec<Variable> = lit.variables().collect();
            must_contain.iter().all(|var| vars.contains(var))
                && vars.iter().all(|var| variable_order.contains(var))
        });

        for lit in literals {
            let column_ord: ColumnOrder = column_order_for(lit, variable_order);

            let set = self
                .required_trie_column_orders
                .entry(lit.predicate())
                .or_insert_with(HashSet::new);

            set.insert(column_ord);
        }
    }
}

pub(super) fn build_preferable_variable_orders(program: &Program) -> Vec<Vec<VariableOrder>> {
    let orders = [
        IterationOrder::Forward,
        IterationOrder::Backward,
        IterationOrder::InsideOut,
    ];

    orders
        .into_iter()
        .map(|iter_ord| {
            VariableOrderBuilder::build_for(program, iter_ord)
                .into_iter()
                .map(|var_ord| vec![var_ord])
                .collect()
        })
        .reduce(|mut vec_a: Vec<Vec<VariableOrder>>, vec_b| {
            vec_a
                .iter_mut()
                .zip(vec_b)
                .for_each(|(var_ords_a, mut var_ords_b)| {
                    let new_element = var_ords_b
                        .pop()
                        .expect("we knwo that var_ords_b contains exactly one element");
                    if !var_ords_a.contains(&new_element) {
                        var_ords_a.push(new_element);
                    }
                });
            vec_a
        })
        .expect("orders are defined above and is non-empty")
}

// TODO: write tests
