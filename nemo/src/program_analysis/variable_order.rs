// NOTE: Generation of variable orders and the filter functions are taken from
// https://github.com/phil-hanisch/rulewerk/blob/lftj/rulewerk-lftj/src/main/java/org/semanticweb/rulewerk/lftj/implementation/Heuristic.java
// NOTE: some functions are slightly modified but the overall idea is reflected

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::model::{
    chase_model::{ChaseAtom, ChaseProgram, ChaseRule, VariableAtom},
    Identifier, Variable,
};
use nemo_physical::{
    management::execution_plan::ColumnOrder, permutator::Permutator,
    util::mapping::permutation::Permutation,
};

/// Represents an ordering of variables as [HashMap].
#[repr(transparent)]
#[derive(Clone, Default, PartialEq, Eq)]
pub struct VariableOrder(HashMap<Variable, usize>);

impl VariableOrder {
    /// Create new [VariableOrder].
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Insert new variable in the last position.
    pub fn push(&mut self, variable: Variable) {
        let max_index = self.0.values().max();
        self.0.insert(variable, max_index.map_or(0, |i| i + 1));
    }

    /// Insert a new variable at a certain position.
    pub fn push_position(&mut self, variable: Variable, position: usize) {
        for current_position in &mut self.0.values_mut() {
            if *current_position >= position {
                *current_position += 1;
            }
        }

        let insert_result = self.0.insert(variable, position);
        debug_assert!(insert_result.is_none());
    }

    /// Get position of a variable.
    pub fn get(&self, variable: &Variable) -> Option<&usize> {
        self.0.get(variable)
    }

    /// Check if variable is part of the [VariableOrder].
    pub fn contains(&self, variable: &Variable) -> bool {
        self.0.contains_key(variable)
    }

    /// Returns a [VariableOrder] which is restricted to the given variables (but preserve their order)
    pub fn restrict_to(&self, variables: &HashSet<Variable>) -> Self {
        let mut variable_vector = Vec::<Variable>::with_capacity(variables.len());
        for variable in variables {
            if self.0.contains_key(variable) {
                variable_vector.push(variable.clone());
            }
        }

        variable_vector.sort_by(|a, b| {
            self.get(a)
                .unwrap()
                .partial_cmp(self.get(b).unwrap())
                .unwrap()
        });

        let mut result = HashMap::<Variable, usize>::new();

        for (index, variable) in variable_vector.into_iter().enumerate() {
            result.insert(variable, index);
        }

        Self(result)
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether it contains any entry.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return an iterator over all mapped variables.
    pub fn iter(&self) -> impl Iterator<Item = &Variable> {
        let mut vars: Vec<&Variable> = self.0.keys().collect();
        vars.sort_by_key(|var| {
            self.0
                .get(var)
                .expect("we are iterating over existing keys")
        });
        vars.into_iter()
    }

    /// Return a vector which puts the variables in the order prescribed by `self`.
    pub fn as_ordered_list(&self) -> Vec<Variable> {
        let mut variables: Vec<Variable> = self.iter().cloned().collect();
        variables.sort_by(|a, b| self.get(a).unwrap().cmp(self.get(b).unwrap()));

        variables
    }

    /// Return [String] with the contents of this object for debugging.
    pub(crate) fn debug(&self) -> String {
        let mut variable_vector = Vec::<Variable>::new();
        variable_vector.resize_with(self.0.len(), || {
            Variable::Universal("PLACEHOLDER".to_string())
        });

        for (variable, index) in &self.0 {
            if *index >= variable_vector.len() {
                return String::from("TODO: Fix this function");
            }

            variable_vector[*index] = variable.clone();
        }

        let mut result = String::new();

        result += "[";
        for (index, variable) in variable_vector.iter().enumerate() {
            let identifier = match variable {
                Variable::Universal(id) => id.to_owned(),
                Variable::Existential(id) => id.to_owned(),
                Variable::UnnamedUniversal(id) => format!("_{}", id),
            };

            result += &identifier;

            if index < variable_vector.len() - 1 {
                result += ", ";
            }
        }
        result += "]";

        result
    }
}

impl std::fmt::Debug for VariableOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("VariableOrder")
            .field(
                &self
                    .as_ordered_list()
                    .iter()
                    .map(|var| match var {
                        Variable::Universal(v) => "?".to_string() + v.as_str(),
                        Variable::Existential(v) => "!".to_string() + v.as_str(),
                        Variable::UnnamedUniversal(v) => format!("_{}", v).to_string(),
                    })
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Debug)]
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
                &(0..(len / 2))
                    .map(|i| i * 2 + 1)
                    .rev()
                    .chain((0..(len + 1) / 2).map(|i| i * 2))
                    .collect::<Vec<_>>(),
            ),
        }
    }
}

fn column_order_for(atom: &VariableAtom, var_order: &VariableOrder) -> ColumnOrder {
    let mut partial_col_order: Vec<usize> = var_order
        .iter()
        .flat_map(|var| {
            atom.terms()
                .iter()
                .enumerate()
                .filter(move |(_, lit_var)| *lit_var == var)
                .map(|(i, _)| i)
        })
        .collect();

    let mut remaining_vars: Vec<usize> = atom
        .terms()
        .iter()
        .enumerate()
        .map(|(i, _)| i)
        .filter(|i| !partial_col_order.contains(i))
        .collect();

    partial_col_order.append(&mut remaining_vars);

    ColumnOrder::from_vector(partial_col_order)
}

trait RuleVariableList {
    fn filter_cartesian_product(
        self,
        partial_var_order: &VariableOrder,
        rule: &ChaseRule,
    ) -> Vec<Variable>;

    fn filter_tries<P: FnMut(&Identifier) -> bool>(
        self,
        partial_var_order: &VariableOrder,
        rule: &ChaseRule,
        required_trie_column_orders: &HashMap<Identifier, HashSet<ColumnOrder>>,
        predicate_filter: P,
    ) -> Vec<Variable>;
}

impl RuleVariableList for Vec<Variable> {
    fn filter_cartesian_product(
        self,
        partial_var_order: &VariableOrder,
        rule: &ChaseRule,
    ) -> Vec<Variable> {
        let result: Vec<Variable> = self
            .iter()
            .filter(|var| {
                rule.positive_body().iter().any(|atom| {
                    let predicate_vars: Vec<Variable> = atom.terms().clone();

                    predicate_vars.iter().any(|pred_var| pred_var == *var)
                        && predicate_vars
                            .iter()
                            .any(|pred_var| partial_var_order.contains(pred_var))
                })
            })
            .cloned()
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
        rule: &ChaseRule,
        required_trie_column_orders: &HashMap<Identifier, HashSet<ColumnOrder>>,
        mut predicate_filter: P,
    ) -> Vec<Variable> {
        let ratios: Vec<(usize, usize)> = self
            .iter()
            .map(|var| {
                let mut extended_var_order: VariableOrder = partial_var_order.clone();
                extended_var_order.push(var.clone());

                let atoms = rule
                    .positive_body()
                    .iter()
                    .filter(|atom| predicate_filter(&atom.predicate()))
                    .filter(|atom| atom.terms().iter().any(|atom_var| atom_var == var));

                let (atoms_requiring_new_orders, total_atoms) = atoms.fold((0, 0), |acc, atom| {
                    let fitting_column_order_exists: bool = required_trie_column_orders
                        .get(&atom.predicate())
                        .map(|set| set.contains(&column_order_for(atom, &extended_var_order)))
                        .unwrap_or(false);

                    let new_col_order_required = !fitting_column_order_exists;

                    (acc.0 + usize::from(new_col_order_required), acc.1 + 1)
                    // bool is coverted to 1 for true and 0 for false
                });

                (atoms_requiring_new_orders, total_atoms)
            })
            .collect();

        // prefer variables that occur in the most literals
        // if the number of occurrences is the same, prefer fewer reorders
        let min_ratio: Option<(usize, usize)> = ratios
            .iter()
            .min_by(|a, b| {
                if a.1 != b.1 {
                    b.1.cmp(&a.1)
                } else {
                    a.0.cmp(&b.0)
                }
            })
            .copied();

        self.into_iter()
            .zip(ratios)
            .filter(move |(_, ratio)| {
                *ratio == min_ratio.expect("the vars and therefore the ratios are non-empty")
            })
            .map(|(var, _)| var)
            .collect()
    }
}

struct VariableOrderBuilder<'a> {
    program: &'a ChaseProgram,
    iteration_order_within_rule: IterationOrder,
    required_trie_column_orders: HashMap<Identifier, HashSet<ColumnOrder>>, // maps predicates to sets of column orders
    idb_preds: HashSet<Identifier>,
}

struct BuilderResult {
    /// A [Vec] where the ith entry contains a good variable order for rule i.
    variable_orders: Vec<VariableOrder>,
    /// A [HashMap] mapping each predicate to the set of [ColumnOrder]s that are supposed to be available.
    column_orders: HashMap<Identifier, HashSet<ColumnOrder>>,
}

impl VariableOrderBuilder<'_> {
    fn build_for(
        program: &ChaseProgram,
        iteration_order_within_rule: IterationOrder,
        initial_column_orders: HashMap<Identifier, HashSet<ColumnOrder>>,
    ) -> BuilderResult {
        let mut builder = VariableOrderBuilder {
            program,
            iteration_order_within_rule,
            required_trie_column_orders: initial_column_orders,
            idb_preds: program.idb_predicates(),
        };

        let variable_orders = builder.generate_variable_orders();
        let column_orders = builder.get_column_orders();

        BuilderResult {
            variable_orders,
            column_orders,
        }
    }

    fn get_already_present_idb_edb_count_for_rule_in_tries(
        &self,
        rule: &ChaseRule,
    ) -> (usize, usize) {
        let preds_with_tries = rule.positive_body().iter().filter_map(|atom| {
            let pred = atom.predicate();
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
        // NOTE: We use a BTreeMap to determinise the iteration order for easier debugging; this should not be performance critical
        let mut remaining_rules: BTreeMap<usize, &ChaseRule> =
            self.program.rules().iter().enumerate().collect();

        let mut result: Vec<(usize, VariableOrder)> =
            Vec::with_capacity(self.program.rules().len());

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

    fn generate_variable_order_for_rule(&mut self, rule: &ChaseRule) -> VariableOrder {
        let mut variable_order: VariableOrder = VariableOrder::new();
        let mut remaining_vars = {
            let remaining_vars_unpermutated: Vec<Variable> = rule
                .positive_body()
                .iter()
                .flat_map(|lit| lit.terms())
                .fold(vec![], |mut acc, var| {
                    if !acc.contains(var) {
                        acc.push(var.clone());
                    }

                    acc
                });

            self.iteration_order_within_rule
                .get_permutator(remaining_vars_unpermutated.len())
                .permute(&remaining_vars_unpermutated)
                .expect("we are checking the length so everything should work out")
                .collect::<Vec<_>>()
        };

        while !remaining_vars.is_empty() {
            let next_var = {
                let after_cart = remaining_vars
                    .clone()
                    .filter_cartesian_product(&variable_order, rule);

                let after_trie_idb = after_cart.filter_tries(
                    &variable_order,
                    rule,
                    &self.required_trie_column_orders,
                    |pred| self.idb_preds.contains(pred),
                );

                after_trie_idb
                    .filter_tries(
                        &variable_order,
                        rule,
                        &self.required_trie_column_orders,
                        |pred| !self.idb_preds.contains(pred),
                    )
                    .first()
                    .cloned()
                    .expect("the filter results are guaranteed to be non-empty")
            };

            variable_order.push(next_var.clone());
            remaining_vars.retain(|var| var != &next_var);
            self.update_trie_column_orders(&variable_order, HashSet::from([next_var]), rule);
        }

        variable_order
    }

    fn update_trie_column_orders(
        &mut self,
        variable_order: &VariableOrder,
        must_contain: HashSet<Variable>,
        rule: &ChaseRule,
    ) {
        let atoms = rule.positive_body().iter().filter(|atom| {
            let vars: Vec<Variable> = atom.terms().clone();
            must_contain.iter().all(|var| vars.contains(var))
                && vars.iter().all(|var| variable_order.contains(var))
        });

        for atom in atoms {
            let column_ord: ColumnOrder = column_order_for(atom, variable_order);

            let set = self
                .required_trie_column_orders
                .entry(atom.predicate())
                .or_default();

            set.insert(column_ord);
        }
    }

    fn get_column_orders(self) -> HashMap<Identifier, HashSet<ColumnOrder>> {
        self.required_trie_column_orders
    }
}

/// Contains the result of the function `build_preferable_variable_orders`.
pub(super) struct BuilderResultVariants {
    /// [Vec] where the ith entry contains a [Vec] of with good variable orders for the ith rule
    pub(super) all_variable_orders: Vec<Vec<VariableOrder>>,
    /// For each variant of the variable order computation
    /// contains one [HashSet] mapping each predicate to its available [ColumnOrder]s.
    pub(super) all_column_orders: Vec<HashMap<Identifier, HashSet<ColumnOrder>>>,
}

pub(super) fn build_preferable_variable_orders(
    program: &ChaseProgram,
    initial_column_orders: Option<HashMap<Identifier, HashSet<ColumnOrder>>>,
) -> BuilderResultVariants {
    let iteration_orders = [
        IterationOrder::Forward,
        IterationOrder::Backward,
        IterationOrder::InsideOut,
    ];

    let initial_column_orders = initial_column_orders.unwrap_or_else(|| {
        let mut result: HashMap<Identifier, HashSet<Permutation>> = Default::default();
        for fact in program.facts().iter() {
            result.insert(fact.predicate(), HashSet::from([ColumnOrder::default()]));
        }
        for (pred, _handler) in program.imports() {
            result.insert(pred.clone(), HashSet::from([ColumnOrder::default()]));
        }
        result
    });

    let mut all_variable_orders = vec![Vec::<VariableOrder>::new(); program.rules().len()];
    let mut all_column_orders = Vec::new();

    for iteration_order in iteration_orders {
        let BuilderResult {
            variable_orders,
            column_orders,
        } = VariableOrderBuilder::build_for(
            program,
            iteration_order,
            initial_column_orders.clone(),
        );

        for (rule_index, variable_order) in variable_orders.into_iter().enumerate() {
            if !all_variable_orders[rule_index].contains(&variable_order) {
                all_variable_orders[rule_index].push(variable_order);
            }
        }

        all_column_orders.push(column_orders);
    }

    BuilderResultVariants {
        all_variable_orders,
        all_column_orders,
    }
}

#[cfg(test)]
mod test {
    use super::{IterationOrder, RuleVariableList, VariableOrder};

    use crate::model::chase_model::{ChaseProgram, ChaseRule, PrimitiveAtom, VariableAtom};
    use crate::model::{
        FileFormat, Identifier, ImportDirective, ImportExportDirective, PrimitiveTerm, Variable,
        PARAMETER_NAME_FORMAT, PARAMETER_NAME_RESOURCE, VALUE_FORMAT_ANY,
    };
    use nemo_physical::datavalues::{AnyDataValue, MapDataValue, TupleDataValue};
    use nemo_physical::management::execution_plan::ColumnOrder;

    use std::collections::{HashMap, HashSet};

    type TestRuleSetWithAdditionalInfo =
        (Vec<ChaseRule>, Vec<Vec<Variable>>, Vec<(Identifier, usize)>);

    impl VariableOrder {
        fn from_vec(vec: Vec<Variable>) -> Self {
            Self(vec.into_iter().enumerate().map(|(i, v)| (v, i)).collect())
        }
    }

    #[test]
    fn get_iteration_order_permutator() {
        let orders = [
            IterationOrder::Forward,
            IterationOrder::Backward,
            IterationOrder::InsideOut,
        ];

        let test_vec_5: Vec<usize> = (0..5).collect();
        let test_vec_6: Vec<usize> = (0..6).collect();

        let expected_5: Vec<Vec<usize>> = vec![
            vec![0, 1, 2, 3, 4],
            vec![4, 3, 2, 1, 0],
            vec![2, 1, 3, 0, 4],
        ];
        let expected_6: Vec<Vec<usize>> = vec![
            vec![0, 1, 2, 3, 4, 5],
            vec![5, 4, 3, 2, 1, 0],
            vec![3, 2, 4, 1, 5, 0],
        ];

        let results_5: Vec<Vec<usize>> = orders
            .iter()
            .map(|ord| {
                ord.get_permutator(5)
                    .permute(&test_vec_5)
                    .expect("the length is correct")
                    .collect()
            })
            .collect();
        let results_6: Vec<Vec<usize>> = orders
            .iter()
            .map(|ord| {
                ord.get_permutator(6)
                    .permute(&test_vec_6)
                    .expect("the length is correct")
                    .collect()
            })
            .collect();

        assert_eq!(expected_5, results_5);
        assert_eq!(expected_6, results_6);
    }

    fn get_test_rule_with_vars_where_predicates_are_different() -> (ChaseRule, Vec<Variable>) {
        let a = Identifier("a".to_string());
        let b = Identifier("b".to_string());
        let c = Identifier("c".to_string());

        let x = Variable::Universal("x".to_string());
        let y = Variable::Universal("y".to_string());
        let z = Variable::Universal("z".to_string());

        let tx = PrimitiveTerm::Variable(x.clone());
        let _ty = PrimitiveTerm::Variable(y.clone());
        let tz = PrimitiveTerm::Variable(z.clone());

        (
            ChaseRule::positive_rule(
                vec![PrimitiveAtom::new(c, vec![tx.clone(), tz.clone()])],
                vec![
                    VariableAtom::new(a, vec![x.clone(), y.clone()]),
                    VariableAtom::new(b, vec![y.clone(), z.clone()]),
                ],
                vec![],
            ),
            vec![x, y, z],
        )
    }

    fn get_test_rule_with_vars_where_predicates_are_the_same() -> (ChaseRule, Vec<Variable>) {
        let a = Identifier("a".to_string());

        let x = Variable::Universal("x".to_string());
        let y = Variable::Universal("y".to_string());
        let z = Variable::Universal("z".to_string());

        let tx = PrimitiveTerm::Variable(x.clone());
        let _ty = PrimitiveTerm::Variable(y.clone());
        let tz = PrimitiveTerm::Variable(z.clone());

        (
            ChaseRule::positive_rule(
                vec![PrimitiveAtom::new(a.clone(), vec![tx.clone(), tz.clone()])],
                vec![
                    VariableAtom::new(a.clone(), vec![x.clone(), y.clone()]),
                    VariableAtom::new(a, vec![y.clone(), z.clone()]),
                ],
                vec![],
            ),
            vec![x, y, z],
        )
    }

    enum RulePredicateVariant {
        Same,
        Different,
    }

    fn filter_cartesian_product_with_empty_var_order(pred_variant: RulePredicateVariant) {
        let (rule, vars) = match pred_variant {
            RulePredicateVariant::Same => get_test_rule_with_vars_where_predicates_are_the_same(),
            RulePredicateVariant::Different => {
                get_test_rule_with_vars_where_predicates_are_different()
            }
        };
        let empty_ord = VariableOrder::new();

        let expected = vars.clone();

        let filtered_vars = vars.filter_cartesian_product(&empty_ord, &rule);

        assert_eq!(expected, filtered_vars);
    }

    fn filter_cartesian_product_with_only_x_in_var_order(pred_variant: RulePredicateVariant) {
        let (rule, vars) = match pred_variant {
            RulePredicateVariant::Same => get_test_rule_with_vars_where_predicates_are_the_same(),
            RulePredicateVariant::Different => {
                get_test_rule_with_vars_where_predicates_are_different()
            }
        };
        let x = vars[0].clone();
        let y = vars[1].clone();
        let mut ord_with_x = VariableOrder::new();
        ord_with_x.push(x.clone());

        let remaining_vars: Vec<Variable> = vars.into_iter().filter(|v| v != &x).collect();

        let expected = vec![y];

        let filtered_vars = remaining_vars.filter_cartesian_product(&ord_with_x, &rule);

        assert_eq!(expected, filtered_vars);
    }

    #[test]
    fn filter_cartesian_product_where_all_rules_predicates_are_different_with_empty_var_order() {
        filter_cartesian_product_with_empty_var_order(RulePredicateVariant::Different);
    }

    #[test]
    fn filter_cartesian_product_where_all_rules_predicates_are_the_same_with_empty_var_order() {
        filter_cartesian_product_with_empty_var_order(RulePredicateVariant::Same);
    }

    #[test]
    fn filter_cartesian_product_where_all_rules_predicates_are_different_with_only_x_in_var_order()
    {
        filter_cartesian_product_with_only_x_in_var_order(RulePredicateVariant::Different);
    }

    #[test]
    fn filter_cartesian_product_where_all_rules_predicates_are_the_same_with_only_x_in_var_order() {
        filter_cartesian_product_with_only_x_in_var_order(RulePredicateVariant::Same);
    }

    #[test]
    fn filter_tries_where_rule_predicates_are_different_with_empty_var_order_and_empty_trie_cache()
    {
        let (rule, vars) = get_test_rule_with_vars_where_predicates_are_different();
        let y = vars[1].clone();
        let empty_ord = VariableOrder::new();
        let empty_trie_cache: HashMap<Identifier, HashSet<ColumnOrder>> = HashMap::new();

        let expected = vec![y];

        let filtered_vars = vars.filter_tries(&empty_ord, &rule, &empty_trie_cache, |_| true);

        assert_eq!(expected, filtered_vars);
    }

    #[test]
    fn filter_tries_where_rule_predicates_are_the_same_with_empty_var_order_and_empty_trie_cache() {
        let (rule, vars) = get_test_rule_with_vars_where_predicates_are_the_same();
        let y = vars[1].clone();
        let empty_ord = VariableOrder::new();
        let empty_trie_cache: HashMap<Identifier, HashSet<ColumnOrder>> = HashMap::new();

        let expected = vec![y];

        let filtered_vars = vars.filter_tries(&empty_ord, &rule, &empty_trie_cache, |_| true);

        assert_eq!(expected, filtered_vars);
    }

    #[test]
    fn build_preferable_variable_orders_with_different_predicate_rule() {
        let (rules, var_lists): (Vec<ChaseRule>, Vec<Vec<Variable>>) =
            vec![get_test_rule_with_vars_where_predicates_are_different()]
                .into_iter()
                .unzip();

        let program = ChaseProgram::builder().rules(rules).build();

        let rule_vars = &var_lists[0];
        let rule_var_orders: Vec<VariableOrder> = vec![
            VariableOrder::from_vec(vec![
                rule_vars[1].clone(),
                rule_vars[0].clone(),
                rule_vars[2].clone(),
            ]),
            VariableOrder::from_vec(vec![
                rule_vars[1].clone(),
                rule_vars[2].clone(),
                rule_vars[0].clone(),
            ]),
        ];

        assert_eq!(
            vec![rule_var_orders],
            super::build_preferable_variable_orders(&program, None).all_variable_orders,
        );
    }

    #[test]
    fn build_preferable_variable_orders_with_same_predicate_rule() {
        let (rules, var_lists): (Vec<ChaseRule>, Vec<Vec<Variable>>) =
            vec![get_test_rule_with_vars_where_predicates_are_the_same()]
                .into_iter()
                .unzip();

        let program = ChaseProgram::builder().rules(rules).build();

        let rule_vars = &var_lists[0];
        let rule_var_orders: Vec<VariableOrder> = vec![
            VariableOrder::from_vec(vec![
                rule_vars[1].clone(),
                rule_vars[0].clone(),
                rule_vars[2].clone(),
            ]),
            VariableOrder::from_vec(vec![
                rule_vars[1].clone(),
                rule_vars[2].clone(),
                rule_vars[0].clone(),
            ]),
        ];

        assert_eq!(
            vec![rule_var_orders],
            super::build_preferable_variable_orders(&program, None).all_variable_orders,
        );
    }

    #[test]
    fn build_preferable_variable_orders_with_both_rules() {
        let (rules, var_lists): (Vec<ChaseRule>, Vec<Vec<Variable>>) = vec![
            get_test_rule_with_vars_where_predicates_are_different(),
            get_test_rule_with_vars_where_predicates_are_the_same(),
        ]
        .into_iter()
        .unzip();

        let program = ChaseProgram::builder().rules(rules).build();

        let rule_1_vars = &var_lists[0];
        let rule_1_var_orders: Vec<VariableOrder> = vec![VariableOrder::from_vec(vec![
            rule_1_vars[1].clone(),
            rule_1_vars[0].clone(),
            rule_1_vars[2].clone(),
        ])];
        let rule_2_vars = &var_lists[1];
        let rule_2_var_orders: Vec<VariableOrder> = vec![
            VariableOrder::from_vec(vec![
                rule_2_vars[1].clone(),
                rule_2_vars[0].clone(),
                rule_2_vars[2].clone(),
            ]),
            VariableOrder::from_vec(vec![
                rule_2_vars[1].clone(),
                rule_2_vars[2].clone(),
                rule_2_vars[0].clone(),
            ]),
        ];

        assert_eq!(
            vec![rule_1_var_orders, rule_2_var_orders],
            super::build_preferable_variable_orders(&program, None).all_variable_orders,
        );
    }

    fn get_part_of_galen_test_ruleset_ie_first_5_rules_without_constant(
    ) -> TestRuleSetWithAdditionalInfo {
        let init = Identifier("init".to_string());
        let sub_class_of = Identifier("sub_class_of".to_string());
        let is_main_class = Identifier("is_main_class".to_string());
        let conj = Identifier("conj".to_string());
        let is_sub_class = Identifier("is_sub_class".to_string());
        let xe = Identifier("xe".to_string());
        let exists = Identifier("exists".to_string());

        let predicates = vec![
            (init.clone(), 1),
            (sub_class_of.clone(), 2),
            (is_main_class.clone(), 1),
            (conj.clone(), 3),
            (is_sub_class.clone(), 1),
            (xe.clone(), 3),
            (exists.clone(), 3),
        ];

        let c = Variable::Universal("c".to_string());
        let d1 = Variable::Universal("d1".to_string());
        let d2 = Variable::Universal("d2".to_string());
        let y = Variable::Universal("y".to_string());
        let r = Variable::Universal("r".to_string());
        let e = Variable::Universal("e".to_string());

        let tc = PrimitiveTerm::Variable(c.clone());
        let td1 = PrimitiveTerm::Variable(d1.clone());
        let td2 = PrimitiveTerm::Variable(d2.clone());
        let ty = PrimitiveTerm::Variable(y.clone());
        let tr = PrimitiveTerm::Variable(r.clone());
        let te = PrimitiveTerm::Variable(e.clone());

        let (rules, variables) = [
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(init.clone(), vec![tc.clone()])],
                    vec![VariableAtom::new(is_main_class, vec![c.clone()])],
                    vec![],
                ),
                vec![c.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        sub_class_of.clone(),
                        vec![tc.clone(), tc.clone()],
                    )],
                    vec![VariableAtom::new(init, vec![c.clone()])],
                    vec![],
                ),
                vec![c.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![
                        PrimitiveAtom::new(sub_class_of.clone(), vec![tc.clone(), td1.clone()]),
                        PrimitiveAtom::new(sub_class_of.clone(), vec![tc.clone(), td2.clone()]),
                    ],
                    vec![
                        VariableAtom::new(sub_class_of.clone(), vec![c.clone(), y.clone()]),
                        VariableAtom::new(conj.clone(), vec![y.clone(), d1.clone(), d2.clone()]),
                    ],
                    vec![],
                ),
                vec![c.clone(), y.clone(), d1.clone(), d2.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        sub_class_of.clone(),
                        vec![tc.clone(), ty.clone()],
                    )],
                    vec![
                        VariableAtom::new(sub_class_of.clone(), vec![c.clone(), d1.clone()]),
                        VariableAtom::new(sub_class_of.clone(), vec![c.clone(), d2.clone()]),
                        VariableAtom::new(conj, vec![y.clone(), d1.clone(), d2.clone()]),
                        VariableAtom::new(is_sub_class, vec![y.clone()]),
                    ],
                    vec![],
                ),
                vec![c.clone(), d1, d2, y.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        xe,
                        vec![tc.clone(), tr.clone(), te.clone()],
                    )],
                    vec![
                        VariableAtom::new(sub_class_of, vec![e.clone(), y.clone()]),
                        VariableAtom::new(exists, vec![y.clone(), r.clone(), c.clone()]),
                    ],
                    vec![],
                ),
                vec![e, y, r, c],
            ),
        ]
        .into_iter()
        .unzip();

        (rules, variables, predicates)
    }

    /// Helper function to create source-like imports
    fn csv_import(predicate: Identifier, arity: usize) -> ImportDirective {
        let attributes = MapDataValue::from_iter([
            (
                AnyDataValue::new_iri(PARAMETER_NAME_RESOURCE.to_string()),
                AnyDataValue::new_plain_string("".to_string()),
            ),
            (
                AnyDataValue::new_iri(PARAMETER_NAME_FORMAT.to_string()),
                TupleDataValue::from_iter(
                    vec![VALUE_FORMAT_ANY; arity]
                        .iter()
                        .map(|format| AnyDataValue::new_plain_string((*format).to_string()))
                        .collect::<Vec<AnyDataValue>>(),
                )
                .into(),
            ),
        ]);
        ImportDirective::from(ImportExportDirective {
            predicate,
            format: FileFormat::CSV,
            attributes,
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn build_preferable_variable_orders_with_galen_el_part_ie_5_rules_without_constant() {
        let (rules, var_lists, predicates) =
            get_part_of_galen_test_ruleset_ie_first_5_rules_without_constant();

        let program = ChaseProgram::builder()
            .imports(
                [1usize, 2, 3, 4, 6]
                    .map(|idx| csv_import(predicates[idx].0.clone(), predicates[idx].1)),
            )
            .expect("these imports should not lead to errors")
            .rules(rules)
            .build();

        let rule_1_vars = &var_lists[0];
        let rule_1_var_orders: Vec<VariableOrder> = vec![
            VariableOrder::from_vec(vec![rule_1_vars[0].clone()]), // z is always first here since it occurs only in edb predicate; x and y occur in A(...) which is idb by rule 2
        ];
        let rule_2_vars = &var_lists[1];
        let rule_2_var_orders: Vec<VariableOrder> =
            vec![VariableOrder::from_vec(vec![rule_2_vars[0].clone()])];
        let rule_3_vars = &var_lists[2];
        let rule_3_var_orders: Vec<VariableOrder> = vec![VariableOrder::from_vec(vec![
            rule_3_vars[0].clone(),
            rule_3_vars[1].clone(),
            rule_3_vars[2].clone(),
            rule_3_vars[3].clone(),
        ])];
        let rule_4_vars = &var_lists[3];
        let rule_4_var_orders: Vec<VariableOrder> = vec![
            VariableOrder::from_vec(vec![
                rule_4_vars[0].clone(),
                rule_4_vars[1].clone(),
                rule_4_vars[2].clone(),
                rule_4_vars[3].clone(),
            ]),
            VariableOrder::from_vec(vec![
                rule_4_vars[0].clone(),
                rule_4_vars[2].clone(),
                rule_4_vars[1].clone(),
                rule_4_vars[3].clone(),
            ]),
        ];
        let rule_5_vars = &var_lists[4];
        let rule_5_var_orders: Vec<VariableOrder> = vec![VariableOrder::from_vec(vec![
            rule_5_vars[0].clone(),
            rule_5_vars[1].clone(),
            rule_5_vars[2].clone(),
            rule_5_vars[3].clone(),
        ])];

        assert_eq!(
            vec![
                rule_1_var_orders,
                rule_2_var_orders,
                rule_3_var_orders,
                rule_4_var_orders,
                rule_5_var_orders
            ],
            super::build_preferable_variable_orders(&program, None).all_variable_orders,
        );
    }

    fn get_el_test_ruleset_without_constants() -> TestRuleSetWithAdditionalInfo {
        let init = Identifier("init".to_string());
        let sub_class_of = Identifier("sub_class_of".to_string());
        let is_main_class = Identifier("is_main_class".to_string());
        let conj = Identifier("conj".to_string());
        let is_sub_class = Identifier("is_sub_class".to_string());
        let xe = Identifier("xe".to_string());
        let exists = Identifier("exists".to_string());
        let aux_subsub_ext = Identifier("aux_subsub_ext".to_string());
        let sub_prop = Identifier("sub_prop".to_string());
        let aux = Identifier("aux".to_string());
        let sub_prop_chain = Identifier("sub_prop_chain".to_string());
        let main_sub_class_of = Identifier("main_sub_class_of".to_string());

        let predicates = vec![
            (init.clone(), 1),
            (sub_class_of.clone(), 2),
            (is_main_class.clone(), 1),
            (conj.clone(), 3),
            (is_sub_class.clone(), 1),
            (xe.clone(), 3),
            (exists.clone(), 3),
            (aux_subsub_ext.clone(), 3),
            (sub_prop.clone(), 2),
            (aux.clone(), 3),
            (sub_prop_chain.clone(), 3),
            (main_sub_class_of.clone(), 2),
        ];

        let c = Variable::Universal("c".to_string());
        let d1 = Variable::Universal("d1".to_string());
        let d2 = Variable::Universal("d2".to_string());
        let y = Variable::Universal("y".to_string());
        let r = Variable::Universal("r".to_string());
        let e = Variable::Universal("e".to_string());
        let s = Variable::Universal("s".to_string());
        let r1 = Variable::Universal("r1".to_string());
        let r2 = Variable::Universal("r2".to_string());
        let s1 = Variable::Universal("s1".to_string());
        let s2 = Variable::Universal("s2".to_string());
        let d = Variable::Universal("d".to_string());
        let a = Variable::Universal("a".to_string());
        let b = Variable::Universal("b".to_string());

        let tc = PrimitiveTerm::Variable(c.clone());
        let td1 = PrimitiveTerm::Variable(d1.clone());
        let td2 = PrimitiveTerm::Variable(d2.clone());
        let ty = PrimitiveTerm::Variable(y.clone());
        let tr = PrimitiveTerm::Variable(r.clone());
        let te = PrimitiveTerm::Variable(e.clone());
        let ts = PrimitiveTerm::Variable(s.clone());
        let _tr1 = PrimitiveTerm::Variable(r1.clone());
        let _tr2 = PrimitiveTerm::Variable(r2.clone());
        let _ts1 = PrimitiveTerm::Variable(s1.clone());
        let _ts2 = PrimitiveTerm::Variable(s2.clone());
        let td = PrimitiveTerm::Variable(d.clone());
        let ta = PrimitiveTerm::Variable(a.clone());
        let tb = PrimitiveTerm::Variable(b.clone());

        let (rules, variables) = [
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(init.clone(), vec![tc.clone()])],
                    vec![VariableAtom::new(is_main_class.clone(), vec![c.clone()])],
                    vec![],
                ),
                vec![c.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        sub_class_of.clone(),
                        vec![tc.clone(), tc.clone()],
                    )],
                    vec![VariableAtom::new(init.clone(), vec![c.clone()])],
                    vec![],
                ),
                vec![c.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![
                        PrimitiveAtom::new(sub_class_of.clone(), vec![tc.clone(), td1.clone()]),
                        PrimitiveAtom::new(sub_class_of.clone(), vec![tc.clone(), td2.clone()]),
                    ],
                    vec![
                        VariableAtom::new(sub_class_of.clone(), vec![c.clone(), y.clone()]),
                        VariableAtom::new(conj.clone(), vec![y.clone(), d1.clone(), d2.clone()]),
                    ],
                    vec![],
                ),
                vec![c.clone(), y.clone(), d1.clone(), d2.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        sub_class_of.clone(),
                        vec![tc.clone(), ty.clone()],
                    )],
                    vec![
                        VariableAtom::new(sub_class_of.clone(), vec![c.clone(), d1.clone()]),
                        VariableAtom::new(sub_class_of.clone(), vec![c.clone(), d2.clone()]),
                        VariableAtom::new(conj, vec![y.clone(), d1.clone(), d2.clone()]),
                        VariableAtom::new(is_sub_class.clone(), vec![y.clone()]),
                    ],
                    vec![],
                ),
                vec![c.clone(), d1, d2, y.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        xe.clone(),
                        vec![tc.clone(), tr.clone(), te.clone()],
                    )],
                    vec![
                        VariableAtom::new(sub_class_of.clone(), vec![e.clone(), y.clone()]),
                        VariableAtom::new(exists.clone(), vec![y.clone(), r.clone(), c.clone()]),
                    ],
                    vec![],
                ),
                vec![e.clone(), y.clone(), r.clone(), c.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        aux_subsub_ext.clone(),
                        vec![td.clone(), tr.clone(), ty.clone()],
                    )],
                    vec![
                        VariableAtom::new(sub_prop.clone(), vec![r.clone(), s.clone()]),
                        VariableAtom::new(exists, vec![y.clone(), s.clone(), d.clone()]),
                        VariableAtom::new(is_sub_class, vec![y.clone()]),
                    ],
                    vec![],
                ),
                vec![r.clone(), s.clone(), y.clone(), d.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        aux.clone(),
                        vec![tc.clone(), tr.clone(), ty.clone()],
                    )],
                    vec![
                        VariableAtom::new(sub_class_of.clone(), vec![c.clone(), d.clone()]),
                        VariableAtom::new(aux_subsub_ext, vec![d.clone(), r.clone(), y.clone()]),
                    ],
                    vec![],
                ),
                vec![c.clone(), d.clone(), r.clone(), y.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        sub_class_of.clone(),
                        vec![te.clone(), ty.clone()],
                    )],
                    vec![
                        VariableAtom::new(xe.clone(), vec![c.clone(), r.clone(), e.clone()]),
                        VariableAtom::new(aux, vec![c.clone(), r.clone(), y.clone()]),
                    ],
                    vec![],
                ),
                vec![c.clone(), r.clone(), e.clone(), y],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        sub_class_of.clone(),
                        vec![tc.clone(), te.clone()],
                    )],
                    vec![
                        VariableAtom::new(sub_class_of.clone(), vec![c.clone(), d.clone()]),
                        VariableAtom::new(sub_class_of.clone(), vec![d.clone(), e.clone()]),
                    ],
                    vec![],
                ),
                vec![c.clone(), d.clone(), e.clone()],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        xe.clone(),
                        vec![td.clone(), ts.clone(), te.clone()],
                    )],
                    vec![
                        VariableAtom::new(xe.clone(), vec![c.clone(), r1.clone(), e.clone()]),
                        VariableAtom::new(xe.clone(), vec![d.clone(), r2.clone(), c.clone()]),
                        VariableAtom::new(sub_prop.clone(), vec![r1.clone(), s1.clone()]),
                        VariableAtom::new(sub_prop, vec![r2.clone(), s2.clone()]),
                        VariableAtom::new(sub_prop_chain, vec![s1.clone(), s2.clone(), s.clone()]),
                    ],
                    vec![],
                ),
                vec![c.clone(), r1, e.clone(), d, r2, s1, s2, s],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(init, vec![tc.clone()])],
                    vec![VariableAtom::new(xe, vec![c.clone(), r.clone(), e.clone()])],
                    vec![],
                ),
                vec![c, r, e],
            ),
            (
                ChaseRule::positive_rule(
                    vec![PrimitiveAtom::new(
                        main_sub_class_of,
                        vec![ta.clone(), tb.clone()],
                    )],
                    vec![
                        VariableAtom::new(sub_class_of, vec![a.clone(), b.clone()]),
                        VariableAtom::new(is_main_class.clone(), vec![a.clone()]),
                        VariableAtom::new(is_main_class, vec![b.clone()]),
                    ],
                    vec![],
                ),
                vec![a, b],
            ),
        ]
        .into_iter()
        .unzip();

        (rules, variables, predicates)
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn build_preferable_variable_orders_with_el_without_constant() {
        let (rules, var_lists, predicates) = get_el_test_ruleset_without_constants();

        let program = ChaseProgram::builder()
            .imports(
                [1usize, 2, 3, 4, 6, 8, 10]
                    .map(|idx| csv_import(predicates[idx].0.clone(), predicates[idx].1)),
            )
            .expect("these imports should not lead to errors")
            .rules(rules)
            .build();

        let rule_1_vars = &var_lists[0];
        let rule_1_var_orders: Vec<VariableOrder> = vec![
            VariableOrder::from_vec(vec![rule_1_vars[0].clone()]), // z is always first here since it occurs only in edb predicate; x and y occur in A(...) which is idb by rule 2
        ];
        let rule_2_vars = &var_lists[1];
        let rule_2_var_orders: Vec<VariableOrder> =
            vec![VariableOrder::from_vec(vec![rule_2_vars[0].clone()])];
        let rule_3_vars = &var_lists[2];
        let rule_3_var_orders: Vec<VariableOrder> = vec![VariableOrder::from_vec(vec![
            rule_3_vars[1].clone(),
            rule_3_vars[0].clone(),
            rule_3_vars[2].clone(),
            rule_3_vars[3].clone(),
        ])];
        let rule_4_vars = &var_lists[3];
        let rule_4_var_orders: Vec<VariableOrder> = vec![
            VariableOrder::from_vec(vec![
                rule_4_vars[0].clone(),
                rule_4_vars[1].clone(),
                rule_4_vars[2].clone(),
                rule_4_vars[3].clone(),
            ]),
            VariableOrder::from_vec(vec![
                rule_4_vars[0].clone(),
                rule_4_vars[2].clone(),
                rule_4_vars[1].clone(),
                rule_4_vars[3].clone(),
            ]),
        ];
        let rule_5_vars = &var_lists[4];
        let rule_5_var_orders: Vec<VariableOrder> = vec![VariableOrder::from_vec(vec![
            rule_5_vars[1].clone(),
            rule_5_vars[0].clone(),
            rule_5_vars[2].clone(),
            rule_5_vars[3].clone(),
        ])];
        let rule_6_vars = &var_lists[5];
        let rule_6_var_orders: Vec<VariableOrder> = vec![VariableOrder::from_vec(vec![
            rule_6_vars[2].clone(),
            rule_6_vars[1].clone(),
            rule_6_vars[3].clone(),
            rule_6_vars[0].clone(),
        ])];
        let rule_7_vars = &var_lists[6];
        let rule_7_var_orders: Vec<VariableOrder> = vec![
            VariableOrder::from_vec(vec![
                rule_7_vars[1].clone(),
                rule_7_vars[0].clone(),
                rule_7_vars[2].clone(),
                rule_7_vars[3].clone(),
            ]),
            VariableOrder::from_vec(vec![
                rule_7_vars[1].clone(),
                rule_7_vars[0].clone(),
                rule_7_vars[3].clone(),
                rule_7_vars[2].clone(),
            ]),
        ];
        let rule_8_vars = &var_lists[7];
        let rule_8_var_orders: Vec<VariableOrder> = vec![VariableOrder::from_vec(vec![
            rule_8_vars[0].clone(),
            rule_8_vars[1].clone(),
            rule_8_vars[2].clone(),
            rule_8_vars[3].clone(),
        ])];
        let rule_9_vars = &var_lists[8];
        let rule_9_var_orders: Vec<VariableOrder> = vec![VariableOrder::from_vec(vec![
            rule_9_vars[1].clone(),
            rule_9_vars[2].clone(),
            rule_9_vars[0].clone(),
        ])];
        let rule_10_vars = &var_lists[9];
        let rule_10_var_orders: Vec<VariableOrder> = vec![
            VariableOrder::from_vec(vec![
                rule_10_vars[0].clone(),
                rule_10_vars[1].clone(),
                rule_10_vars[4].clone(),
                rule_10_vars[2].clone(),
                rule_10_vars[3].clone(),
                rule_10_vars[5].clone(),
                rule_10_vars[6].clone(),
                rule_10_vars[7].clone(),
            ]),
            VariableOrder::from_vec(vec![
                rule_10_vars[0].clone(),
                rule_10_vars[4].clone(),
                rule_10_vars[1].clone(),
                rule_10_vars[3].clone(),
                rule_10_vars[2].clone(),
                rule_10_vars[5].clone(),
                rule_10_vars[6].clone(),
                rule_10_vars[7].clone(),
            ]),
        ];
        let rule_11_vars = &var_lists[10];
        let rule_11_var_orders: Vec<VariableOrder> = vec![VariableOrder::from_vec(vec![
            rule_11_vars[0].clone(),
            rule_11_vars[1].clone(),
            rule_11_vars[2].clone(),
        ])];
        let rule_12_vars = &var_lists[11];
        let rule_12_var_orders: Vec<VariableOrder> = vec![
            VariableOrder::from_vec(vec![rule_12_vars[0].clone(), rule_12_vars[1].clone()]),
            VariableOrder::from_vec(vec![rule_12_vars[1].clone(), rule_12_vars[0].clone()]),
        ];

        assert_eq!(
            vec![
                rule_1_var_orders,
                rule_2_var_orders,
                rule_3_var_orders,
                rule_4_var_orders,
                rule_5_var_orders,
                rule_6_var_orders,
                rule_7_var_orders,
                rule_8_var_orders,
                rule_9_var_orders,
                rule_10_var_orders,
                rule_11_var_orders,
                rule_12_var_orders,
            ],
            super::build_preferable_variable_orders(&program, None).all_variable_orders,
        );
    }
}
