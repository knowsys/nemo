// NOTE: Generation of variable orders and the filter functions are taken fron
// https://github.com/phil-hanisch/rulewerk/blob/lftj/rulewerk-lftj/src/main/java/org/semanticweb/rulewerk/lftj/implementation/Heuristic.java

use num::rational::Ratio;
use std::collections::{HashMap, HashSet};

use super::super::{
    model::{Identifier, Literal, Program, Rule, Variable},
    table_manager::ColumnOrder,
};

#[repr(transparent)]
#[derive(Debug)]
pub(super) struct VariableOrder(HashMap<Variable, usize>);

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

struct VariableOrderBuilder<'a> {
    program: &'a Program,
    required_trie_column_orders: HashMap<Identifier, HashSet<ColumnOrder>>, // maps predicates to sets of column orders
}

// TODO: many function stake self just to be able to call them as methods; we should chanse this and possibly move the functions to a broader scope(?)
impl<'a> VariableOrderBuilder<'a> {
    fn build_for(program: &Program) -> Vec<Vec<VariableOrder>> {
        let mut builder = VariableOrderBuilder {
            program,
            required_trie_column_orders: HashMap::new(),
        };

        builder.generate_variable_orders()
    }

    fn generate_variable_orders(&mut self) -> Vec<Vec<VariableOrder>> {
        // TODO: pick good order of rules; take care of not messing up indices
        self.program
            .rules
            .iter()
            .map(|rule| self.generate_variable_order_for_rule(rule))
            .collect()
    }

    fn generate_variable_order_for_rule(&mut self, rule: &Rule) -> Vec<VariableOrder> {
        let mut variable_order: VariableOrder = VariableOrder::new();
        let mut remaining_vars: HashSet<Variable> =
            rule.body().flat_map(|lit| lit.variables()).collect();

        while !remaining_vars.is_empty() {
            let next_var = {
                let cartesian_product_filter =
                    self.filter_cartesian_product(remaining_vars.iter(), &variable_order, rule);
                let mut trie_filter =
                    self.filter_tries(cartesian_product_filter, &variable_order, rule);

                *trie_filter.next().unwrap_or_else(|| {
                    remaining_vars
                        .iter()
                        .next()
                        .expect("there is at least one var left")
                })
            };

            variable_order.push(next_var);
            remaining_vars.remove(&next_var);
            self.update_trie_column_orders(&variable_order, HashSet::from([next_var]), rule);
        }

        // TODO: return more than one candidate
        vec![variable_order]
    }

    fn column_order_for(&self, lit: &Literal, var_order: &VariableOrder) -> ColumnOrder {
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
            let column_ord: ColumnOrder = self.column_order_for(lit, variable_order);

            let set = self
                .required_trie_column_orders
                .entry(lit.predicate())
                .or_insert_with(HashSet::new);

            set.insert(column_ord);
        }
    }

    fn filter_cartesian_product<T: Iterator<Item = &'a Variable>>(
        &'a self,
        candidate_vars: T,
        partial_var_order: &'a VariableOrder,
        rule: &'a Rule,
    ) -> impl Iterator<Item = &'a Variable> {
        candidate_vars.filter(|var| {
            rule.body().any(|lit| {
                let predicate_vars: Vec<Variable> = lit.atom().variables().collect();

                predicate_vars.iter().any(|pred_var| pred_var == *var)
                    && predicate_vars
                        .iter()
                        .any(|pred_var| partial_var_order.contains(pred_var))
            })
        })
    }

    fn filter_tries<T: Iterator<Item = &'a Variable>>(
        &'a self,
        candidate_vars: T,
        partial_var_order: &'a VariableOrder,
        rule: &'a Rule,
    ) -> impl Iterator<Item = &'a Variable> {
        let (vars, ratios): (Vec<_>, Vec<_>) = candidate_vars
            .map(|var| {
                let mut extended_var_order: VariableOrder = partial_var_order.clone();
                extended_var_order.push(*var);

                let literals = rule
                    .body()
                    .filter(|lit| lit.variables().any(|lit_var| lit_var == *var));

                let (total_literals, literals_requiring_new_orders) =
                    literals.fold((0, 0), |acc, lit| {
                        let new_col_order_required: bool = !self
                            .required_trie_column_orders
                            .get(&lit.predicate())
                            .map(|set| {
                                set.contains(&self.column_order_for(lit, &extended_var_order))
                            })
                            .unwrap_or(false);

                        (acc.0 + 1, acc.1 + usize::from(new_col_order_required))
                        // bool is coverted to 1 for true and 0 for false
                    });

                // we only consider variables from the rule so there is at least one literals in the rule that features the variable
                debug_assert!(total_literals > 0);

                (
                    var,
                    Ratio::new(literals_requiring_new_orders, total_literals),
                )
            })
            .unzip();
        //.fold((iter::empty(), Ratio::new(usize::MAX, 1)), |(current_iter, current_min), (var, ratio)| {
        //    if ratio < current_min {
        //        (iter::once(var), ratio)
        //    } else if ratio == current_min {
        //        (current_iter.chain(iter::once(var)), current_min)
        //    } else {
        //        (current_iter, current_min)
        //    }
        //})
        //.0

        let min_ratio: Ratio<usize> = *ratios
            .iter()
            .min()
            .expect("the cadidate_vars and therefore the ratios are non-empty");
        vars.into_iter()
            .zip(ratios.into_iter())
            .filter(move |(_, ratio)| *ratio == min_ratio)
            .map(|(var, _)| var)
    }
}

pub(super) fn build_preferable_variable_orders(program: &Program) -> Vec<Vec<VariableOrder>> {
    VariableOrderBuilder::build_for(program)
}
