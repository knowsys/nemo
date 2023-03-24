use std::collections::HashSet;

use crate::logical::model::{Atom, Identifier, Program, Rule, Term, Variable};

use super::variable_order::{build_preferable_variable_orders, VariableOrder};

/// Contains useful information for a (existential) rule
#[derive(Debug, Clone)]
pub struct RuleAnalysis {
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
    /// Number of existential variables.
    pub num_existential: usize,

    /// Table identifier for storing head matches for the restricted chase
    pub head_matches_identifier: Identifier,

    /// Variable orders that are worth considering.
    pub promising_variable_orders: Vec<VariableOrder>,
}

fn is_recursive(rule: &Rule) -> bool {
    rule.head().iter().any(|h| {
        rule.body()
            .iter()
            .filter(|b| b.is_positive())
            .any(|b| h.predicate() == b.predicate())
    })
}

fn count_distinct_existential_variables(rule: &Rule) -> usize {
    let mut existentials = HashSet::<Variable>::new();

    for head_atom in rule.head() {
        for term in head_atom.terms() {
            if let Term::Variable(Variable::Existential(id)) = term {
                existentials.insert(Variable::Existential(id.clone()));
            }
        }
    }

    existentials.len()
}

fn get_variables(atoms: &[&Atom]) -> HashSet<Variable> {
    let mut result = HashSet::new();
    for atom in atoms {
        for term in atom.terms() {
            if let Term::Variable(v) = term {
                result.insert(v.clone());
            }
        }
    }
    result
}

fn analyze_rule(
    rule: &Rule,
    promising_variable_orders: Vec<VariableOrder>,
    rule_index: usize,
) -> RuleAnalysis {
    let body_atoms: Vec<&Atom> = rule.body().iter().map(|l| l.atom()).collect();
    let head_atoms: Vec<&Atom> = rule.head().iter().collect();

    let num_existential = count_distinct_existential_variables(rule);

    RuleAnalysis {
        is_existential: num_existential > 0,
        is_recursive: is_recursive(rule),
        has_filters: !rule.filters().is_empty(),
        body_variables: get_variables(&body_atoms),
        head_variables: get_variables(&head_atoms),
        num_existential,
        // TODO: not sure if this is a good way to introduce a fresh head identifier; I'm not quite sute why it is even required
        head_matches_identifier: Identifier(format!(
            "FRESH_HEAD_MATCHES_IDENTIFIER_FOR_RULE_{rule_index}"
        )),
        promising_variable_orders,
    }
}

/// Contains useful information about the
#[derive(Debug)]
pub struct ProgramAnalysis {
    /// Analysis result for each rule.
    pub rule_analysis: Vec<RuleAnalysis>,
    /// Set of all the predicates that are derived in the chase along with their arity.
    pub derived_predicates: HashSet<Identifier>,
    /// Set of all predicates and their arity.
    pub all_predicates: HashSet<(Identifier, usize)>,
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

    /// Collect all predicates occurring in the program.
    fn get_all_predicates(&self, rule_analysis: &[RuleAnalysis]) -> HashSet<(Identifier, usize)> {
        let mut result = HashSet::<(Identifier, usize)>::new();

        // Predicates in source statments
        for ((predicate, arity), _) in self.sources() {
            result.insert((predicate.clone(), arity));
        }

        // Predicates in rules
        for rule in self.rules() {
            for body_atom in rule.body() {
                result.insert((body_atom.predicate(), body_atom.terms().len()));
            }

            for head_atom in rule.head() {
                result.insert((head_atom.predicate(), head_atom.terms().len()));
            }
        }

        // Predicates in facts
        for fact in self.facts() {
            result.insert((fact.0.predicate(), fact.0.terms().len()));
        }

        // Additional predicates for existential rules
        for analysis in rule_analysis {
            if !analysis.is_existential {
                continue;
            }

            let predicate = analysis.head_matches_identifier.clone();
            let arity = analysis
                .head_variables
                .difference(&analysis.body_variables)
                .count();

            result.insert((predicate, arity));
        }

        result
    }

    /// Analyze itself and return a struct containing the results.
    pub fn analyze(&self) -> ProgramAnalysis {
        let variable_orders = build_preferable_variable_orders(self, None);

        let rule_analysis: Vec<RuleAnalysis> = self
            .rules()
            .iter()
            .enumerate()
            .map(|(i, r)| analyze_rule(r, variable_orders[i].clone(), i))
            .collect();

        let derived_predicates = self.get_head_predicates();
        let all_predicates = self.get_all_predicates(&rule_analysis);

        ProgramAnalysis {
            rule_analysis,
            derived_predicates,
            all_predicates,
        }
    }
}
