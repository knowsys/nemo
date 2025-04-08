use std::collections::{hash_map::Entry, HashMap, HashSet};

use nemo_physical::management::execution_plan::ColumnOrder;

use crate::{
    chase_model::components::{
        atom::{primitive_atom::PrimitiveAtom, variable_atom::VariableAtom, ChaseAtom},
        filter::ChaseFilter,
        program::ChaseProgram,
        rule::ChaseRule,
        term::operation_term::{Operation, OperationTerm},
    },
    rule_model::components::{
        tag::Tag,
        term::{
            operation::operation_kind::OperationKind,
            primitive::{variable::Variable, Primitive},
        },
    },
};

use super::variable_order::{
    build_preferable_variable_orders, BuilderResultVariants, VariableOrder,
};

/// Contains useful information for a (existential) rule
#[derive(Debug, Clone)]
pub struct RuleAnalysis {
    /// Whether it uses an existential variable in its head.
    pub is_existential: bool,
    /// Whether an atom in the head also occurs in the body.
    pub is_recursive: bool,
    /// Whether the rule has positive constraints that need to be applied.
    pub _has_positive_constraints: bool,
    /// Whether the rule has at least one aggregate term in the head.
    pub has_aggregates: bool,

    /// Predicates appearing in the positive part of the body.
    pub positive_body_predicates: HashSet<Tag>,
    /// Predicates appearing in the negative part of the body.
    pub negative_body_predicates: HashSet<Tag>,
    /// Predicates appearing in the head.
    pub head_predicates: HashSet<Tag>,

    /// Variables occurring in the positive part of the body.
    pub _positive_body_variables: HashSet<Variable>,
    /// Variables occurring in the positive part of the body.
    pub _negative_body_variables: HashSet<Variable>,
    /// Variables occurring in the head.
    pub head_variables: HashSet<Variable>,
    /// Number of existential variables.
    pub _num_existential: usize,

    /// Rule that represents the calculation of the satisfied matches for an existential rule.
    existential_aux_rule: ChaseRule,
    /// The associated variable order for the join of the head atoms
    pub existential_aux_order: VariableOrder,

    /// Variable orders that are worth considering.
    pub promising_variable_orders: Vec<VariableOrder>,
}

impl RuleAnalysis {
    /// Return the existential auxillary rule.
    pub(crate) fn existential_aux_rule(&self) -> &ChaseRule {
        &self.existential_aux_rule
    }
}

/// Return true if there is a predicate in the positive part of the rule that also appears in the head of the rule.
fn is_recursive(rule: &ChaseRule) -> bool {
    rule.head().iter().any(|h| {
        rule.positive_body()
            .iter()
            .any(|b| h.predicate() == b.predicate())
    })
}

fn count_distinct_existential_variables(rule: &ChaseRule) -> usize {
    let mut existentials = HashSet::<Variable>::new();

    for head_atom in rule.head() {
        for term in head_atom.terms() {
            if let Primitive::Variable(Variable::Existential(id)) = term {
                existentials.insert(Variable::Existential(id.clone()));
            }
        }
    }

    existentials.len()
}

fn get_variables<Atom: ChaseAtom>(atoms: &[Atom]) -> HashSet<Variable> {
    let mut result = HashSet::new();
    for atom in atoms {
        for variable in atom.variables().cloned() {
            result.insert(variable);
        }
    }
    result
}

fn get_predicates<Atom: ChaseAtom>(atoms: &[Atom]) -> HashSet<Tag> {
    atoms.iter().map(|a| a.predicate()).collect()
}

pub(super) fn get_fresh_rule_predicate(rule_index: usize) -> Tag {
    Tag::new(format!("FRESH_HEAD_MATCHES_Tag_FOR_RULE_{rule_index}"))
}

fn construct_existential_aux_rule(
    rule_index: usize,
    head_atoms: Vec<PrimitiveAtom>,
    column_orders: &HashMap<Tag, HashSet<ColumnOrder>>,
) -> (ChaseRule, VariableOrder) {
    let mut result = ChaseRule::default();

    let mut variable_index = 0;
    let mut generate_variable = move || {
        variable_index += 1;
        let name = format!("__GENERATED_HEAD_AUX_VARIABLE_{variable_index}");
        Variable::universal(&name)
    };

    let mut aux_predicate_terms = Vec::new();
    for atom in &head_atoms {
        let mut used_variables = HashSet::new();
        let mut new_terms = Vec::new();

        for term in atom.terms() {
            match term {
                Primitive::Variable(variable) => {
                    if !used_variables.insert(variable.clone()) {
                        let generated_variable = generate_variable();
                        new_terms.push(generated_variable.clone());

                        let new_constraint = Operation::new(
                            OperationKind::Equal,
                            vec![
                                OperationTerm::Primitive(Primitive::Variable(generated_variable)),
                                OperationTerm::Primitive(term.clone()),
                            ],
                        );

                        result.add_positive_filter(ChaseFilter::new(OperationTerm::Operation(
                            new_constraint,
                        )));
                    } else {
                        if variable.is_universal() {
                            aux_predicate_terms.push(Primitive::Variable(variable.clone()));
                        }

                        new_terms.push(variable.clone());
                    }

                    if variable.is_universal() && used_variables.insert(variable.clone()) {}
                }
                Primitive::Ground(_) => {
                    let generated_variable = generate_variable();
                    new_terms.push(generated_variable.clone());

                    let new_constraint = Operation::new(
                        OperationKind::Equal,
                        vec![
                            OperationTerm::Primitive(Primitive::Variable(generated_variable)),
                            OperationTerm::Primitive(term.clone()),
                        ],
                    );

                    result.add_positive_filter(ChaseFilter::new(OperationTerm::Operation(
                        new_constraint,
                    )));
                }
            }
        }

        result.add_positive_atom(VariableAtom::new(atom.predicate(), new_terms));
    }

    let temp_head_tag = get_fresh_rule_predicate(rule_index);
    let temp_head_atom = PrimitiveAtom::new(temp_head_tag, aux_predicate_terms);
    result.add_head_atom(temp_head_atom);

    let mut rule_program = ChaseProgram::default();
    rule_program.add_rule(result.clone());

    let variable_order =
        build_preferable_variable_orders(&rule_program, Some(column_orders.clone()))
            .all_variable_orders
            .pop()
            .and_then(|mut v| v.pop())
            .expect("This functions provides at least one variable order");

    (result, variable_order)
}

fn analyze_rule(
    rule: &ChaseRule,
    promising_variable_orders: Vec<VariableOrder>,
    promising_column_orders: &[HashMap<Tag, HashSet<ColumnOrder>>],
    rule_index: usize,
) -> RuleAnalysis {
    let num_existential = count_distinct_existential_variables(rule);

    let (existential_aux_rule, existential_aux_order) = if num_existential > 0 {
        // TODO: We only consider the first variable order
        construct_existential_aux_rule(rule_index, rule.head().clone(), &promising_column_orders[0])
    } else {
        (ChaseRule::default(), VariableOrder::new())
    };

    RuleAnalysis {
        is_existential: num_existential > 0,
        is_recursive: is_recursive(rule),
        _has_positive_constraints: !rule.positive_filters().is_empty(),
        has_aggregates: rule.aggregate().is_some(),
        positive_body_predicates: get_predicates(rule.positive_body()),
        negative_body_predicates: get_predicates(rule.negative_body()),
        head_predicates: get_predicates(rule.head()),
        _positive_body_variables: get_variables(rule.positive_body()),
        _negative_body_variables: get_variables(rule.negative_body()),
        head_variables: get_variables(rule.head()),
        _num_existential: num_existential,
        existential_aux_rule,
        existential_aux_order,
        promising_variable_orders,
    }
}

/// Contains useful information about the
#[derive(Debug)]
pub struct ProgramAnalysis {
    /// Analysis result for each rule.
    pub rule_analysis: Vec<RuleAnalysis>,
    /// Set of all the predicates that are derived in the chase.
    pub derived_predicates: HashSet<Tag>,
    /// Set of all predicates and their arity.
    pub all_predicates: HashMap<Tag, usize>,

    /// Map from a predicate to all the rules where that predicate appears in its body
    pub predicate_to_rule_body: HashMap<Tag, HashSet<usize>>,
    /// Map from a predicate to all the rules where that predicate appears in its head
    pub predicate_to_rule_head: HashMap<Tag, HashSet<usize>>,
}

impl ChaseProgram {
    /// Collect all predicates that appear in a head atom into a [HashSet]
    fn get_head_predicates(&self) -> HashSet<Tag> {
        let mut result = HashSet::<Tag>::new();

        for rule in self.rules() {
            for head_atom in rule.head() {
                result.insert(head_atom.predicate());
            }
        }

        result
    }

    /// Collect all predicates in the program, and determine their arity.
    /// An error is returned if arities required for a predicate are not unique.
    pub(super) fn get_all_predicates(&self) -> HashMap<Tag, usize> {
        let mut result = HashMap::<Tag, usize>::new();

        fn add_arity(predicate: Tag, arity: usize, arities: &mut HashMap<Tag, usize>) {
            if let Some(current) = arities.get(&predicate) {
                if *current != arity {
                    unreachable!("invalid program: same predicate used with different arities");
                }
            } else {
                arities.insert(predicate, arity);
            }
        }

        // Predicates in import statements
        for import in self.imports() {
            add_arity(import.predicate().clone(), import.arity(), &mut result);
        }

        // Predicates in export statements
        for export in self.exports() {
            add_arity(export.predicate().clone(), export.arity(), &mut result);
        }

        // Predicates in rules
        for rule in self.rules() {
            for atom in rule.head() {
                add_arity(atom.predicate(), atom.arity(), &mut result);
            }
            for atom in rule
                .positive_body()
                .iter()
                .chain(rule.negative_body().iter())
            {
                add_arity(atom.predicate(), atom.arity(), &mut result);
            }
        }

        // Predicates in facts
        for fact in self.facts() {
            add_arity(fact.predicate(), fact.arity(), &mut result);
        }

        // Additional predicates for existential rules
        for (rule_index, rule) in self.rules().iter().enumerate() {
            let is_existential = count_distinct_existential_variables(rule) > 0;
            if !is_existential {
                continue;
            }

            let body_variables = get_variables(rule.positive_body());
            let head_variables = get_variables(rule.head());

            let predicate = get_fresh_rule_predicate(rule_index);
            let arity = head_variables.difference(&body_variables).count();

            add_arity(predicate, arity, &mut result);
        }

        result
    }

    /// Analyze the program and return a struct containing the results.
    /// This method also checks for structural problems that are not detected
    /// in parsing.
    pub fn analyze(&self) -> ProgramAnalysis {
        let BuilderResultVariants {
            all_variable_orders,
            all_column_orders,
        } = build_preferable_variable_orders(self, None);

        let all_predicates = self.get_all_predicates();
        let derived_predicates = self.get_head_predicates();

        let rule_analysis: Vec<RuleAnalysis> = self
            .rules()
            .iter()
            .enumerate()
            .map(|(idx, rule)| {
                analyze_rule(
                    rule,
                    all_variable_orders[idx].clone(),
                    &all_column_orders,
                    idx,
                )
            })
            .collect();

        let mut predicate_to_rule_body = HashMap::<Tag, HashSet<usize>>::new();
        let mut predicate_to_rule_head = HashMap::<Tag, HashSet<usize>>::new();

        for (rule_index, rule) in self.rules().iter().enumerate() {
            for body_atom in rule.positive_body() {
                match predicate_to_rule_body.entry(body_atom.predicate()) {
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().insert(rule_index);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(HashSet::from([rule_index]));
                    }
                }
            }

            for head_atom in rule.head() {
                match predicate_to_rule_head.entry(head_atom.predicate()) {
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().insert(rule_index);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(HashSet::from([rule_index]));
                    }
                }
            }
        }

        ProgramAnalysis {
            rule_analysis,
            derived_predicates,
            all_predicates,
            predicate_to_rule_body,
            predicate_to_rule_head,
        }
    }
}
