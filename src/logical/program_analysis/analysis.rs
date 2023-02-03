use std::collections::HashSet;

use crate::{
    logical::model::{Atom, Identifier, Program, Rule, Term, Variable},
    logical::types::LogicalTypeCollection,
    physical::dictionary::Dictionary,
};

use super::variable_order::{build_preferable_variable_orders, VariableOrder};

/// Contains useful information for a (existential) rule
#[derive(Debug)]
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

    /// Variable orders that are worth considering.
    pub promising_variable_orders: Vec<VariableOrder>,
}

fn is_recursive<LogicalTypes: LogicalTypeCollection>(rule: &Rule<LogicalTypes>) -> bool {
    rule.head().iter().any(|h| {
        rule.body()
            .iter()
            .filter(|b| b.is_positive())
            .any(|b| h.predicate() == b.predicate())
    })
}

fn is_existential<LogicalTypes: LogicalTypeCollection>(rule: &Rule<LogicalTypes>) -> bool {
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

fn analyze_rule<LogicalTypes: LogicalTypeCollection>(
    rule: &Rule<LogicalTypes>,
    promising_variable_orders: Vec<VariableOrder>,
) -> RuleAnalysis {
    let body_atoms: Vec<&Atom> = rule.body().iter().map(|l| l.atom()).collect();
    let head_atoms: Vec<&Atom> = rule.head().iter().collect();

    RuleAnalysis {
        is_existential: is_existential(rule),
        is_recursive: is_recursive(rule),
        has_filters: !rule.filters().is_empty(),
        body_variables: get_variables(&body_atoms),
        head_variables: get_variables(&head_atoms),
        promising_variable_orders,
    }
}

/// Contains useful information about the
#[derive(Debug)]
pub struct ProgramAnalysis {
    /// Analysis result for each rule.
    pub rule_analysis: Vec<RuleAnalysis>,
    /// Set of all the predicates which potentially contain new information
    pub derived_predicates: HashSet<Identifier>,
}

impl<Dict: Dictionary, LogicalTypes: LogicalTypeCollection> Program<Dict, LogicalTypes> {
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
