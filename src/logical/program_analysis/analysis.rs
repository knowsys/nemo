use std::collections::{HashMap, HashSet};

use crate::{
    logical::model::{Atom, Identifier, Literal, Program, Rule, Term, Variable},
    physical::util::Reordering,
};

use super::variable_order::{build_preferable_variable_orders, VariableOrder};

/// Contains useful information for a normal (existential) rule
#[derive(Debug)]
pub struct NormalRuleAnalysis {
    /// Whether it uses an existential variable in its head.
    pub is_existential: bool,
    /// Whether an atom in the head also occurs in the body.
    pub is_recursive: bool,
    /// Whether the rule has filter that need to be applied.
    pub has_filters: bool,

    /// Variables occuring in the body.
    pub body_variables: HashSet<Variable>,
    /// Variables occuring in the head.
    pub head_variables: HashSet<Variable>,

    /// Variable orders that are worth considering.
    pub promising_variable_orders: Vec<VariableOrder>,
}

/// Contains useful information for a "copy"-rule
/// Those are rules which only copy/reorder their input tables
/// For example: p(x, y), q(z, w) -> s(w, z), r(x, y)
#[derive(Debug)]
pub struct CopyRuleAnalysis {
    /// Map each head atom to the body atom its supposed to be copied from
    /// For the example above:
    /// head_to_body = {0 -> (1, [1, 0]), 1 -> (0, [0, 1])}
    pub head_to_body: HashMap<usize, (usize, Reordering)>,
}

/// Result of the static analysis of a rule.
#[derive(Debug)]
pub enum RuleAnalysis {
    /// Analysis for a "copy" rule.
    Copy(CopyRuleAnalysis),
    /// Analysis for any other kind of rule.
    Normal(NormalRuleAnalysis),
}

fn is_recursive(rule: &Rule) -> bool {
    rule.head().iter().any(|h| {
        rule.body()
            .iter()
            .filter(|b| b.is_positive())
            .any(|b| h.predicate() == b.predicate())
    })
}

fn is_existential(rule: &Rule) -> bool {
    rule.head().iter().any(|a| {
        a.terms()
            .iter()
            .any(|t| matches!(t, Term::Variable(Variable::Existential(_))))
    })
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

/// Check if the given rule is a "copy" rule.
/// Such a rule must satisfy the following constraints:
/// * The variables of each body atom have to be disjoint
/// * No head predicate must appear twice
/// * For every head atom there is a body atom which uses the same variables (possibly in a different order)
/// * May not repeat variables in any atom (head nor body)
/// * Must not use any filters
/// Returns None if it isn't or the analysis result.
fn check_copy_rule(rule: &Rule) -> Option<CopyRuleAnalysis> {
    // First check if it has filters
    if !rule.filters().is_empty() {
        return None;
    }

    // Then, check if all variables of each body atom are disjoint
    let mut all_body_variables = HashSet::<Variable>::new();

    for body_literal in rule.body() {
        if let Literal::Positive(body_atom) = body_literal {
            let body_atom_variables = get_variables(&vec![body_atom]);

            if body_atom_variables.len() != body_atom.terms().len() {
                // Repeats at least one variable in body aotm
                return None;
            }

            if all_body_variables.is_disjoint(&body_atom_variables) {
                all_body_variables.extend(body_atom_variables.iter());
            } else {
                // Variables are not disjoint
                return None;
            }
        } else {
            // TODO: Negation
        }
    }

    let mut head_to_body = HashMap::<usize, (usize, Reordering)>::new();

    for (head_index, head_atom) in rule.head().iter().enumerate() {
        let head_atom_variables = get_variables(&vec![head_atom]);

        if head_atom_variables.len() != head_atom.terms().len() {
            // Repeats at least one variable in head aotm
            return None;
        }

        let mut covered_by_body = false;
        for (body_index, body_literal) in rule.body().iter().enumerate() {
            if let Literal::Positive(body_atom) = body_literal {
                let body_atom_variables = get_variables(&vec![body_atom]);
                if head_atom_variables == body_atom_variables {
                    covered_by_body = true;

                    if head_to_body
                        .insert(
                            head_index,
                            (
                                body_index,
                                Reordering::from_transformation(
                                    body_atom.terms(),
                                    &head_atom.terms(),
                                ),
                            ),
                        )
                        .is_some()
                    {
                        // This means the head predicate is present at least twice
                        return None;
                    }

                    break;
                }
            } else {
                // TODO: Negation
            }
        }

        if !covered_by_body {
            return None;
        }
    }

    Some(CopyRuleAnalysis { head_to_body })
}

fn analyze_rule(rule: &Rule, promising_variable_orders: Vec<VariableOrder>) -> RuleAnalysis {
    // First determine the type of the rule
    if let Some(copy_info) = check_copy_rule(rule) {
        return RuleAnalysis::Copy(copy_info);
    }

    // At this point we assume type "normal"
    let body_atoms: Vec<&Atom> = rule.body().iter().map(|l| l.atom()).collect();
    let head_atoms: Vec<&Atom> = rule.head().iter().collect();

    let normal_analysis = NormalRuleAnalysis {
        is_existential: is_existential(rule),
        is_recursive: is_recursive(rule),
        has_filters: !rule.filters().is_empty(),
        body_variables: get_variables(&body_atoms),
        head_variables: get_variables(&head_atoms),
        promising_variable_orders,
    };

    RuleAnalysis::Normal(normal_analysis)
}

/// Contains useful information about the
#[derive(Debug)]
pub struct ProgramAnalysis {
    /// Analysis result for each rule.
    pub rule_analysis: Vec<RuleAnalysis>,
    /// Set of all the predicates which potentially contain new information
    pub derived_predicates: HashSet<Identifier>,
}

impl Program {
    /// Collect all predicates that appear in a head atom into a [`HashSet`]
    fn get_head_predicates(&self) -> HashSet<Identifier> {
        let mut result = HashSet::<Identifier>::new();

        for rule in self.rules() {
            for head_atom in rule.head() {
                result.insert(head_atom.predicate());
            }
        }

        result
    }

    /// Analyze itself and return a struct containing the results.
    pub fn analyze(&self) -> ProgramAnalysis {
        let variable_orders = build_preferable_variable_orders(self, None);

        ProgramAnalysis {
            rule_analysis: self
                .rules()
                .iter()
                .enumerate()
                .map(|(i, r)| analyze_rule(r, variable_orders[i].clone()))
                .collect(),
            derived_predicates: self.get_head_predicates(),
        }
    }
}
