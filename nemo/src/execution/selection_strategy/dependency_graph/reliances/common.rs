//! Module for common functionality for the computation of different reliances.

use std::collections::{HashMap, HashSet};

use petgraph::Directed;

use crate::util::{class_assignment::ClassAssignment, labeled_graph::LabeledGraph};

use super::{
    positive::PositiveReliance,
    restraint::RestraintReliance,
    rules::{Atom, Fact, Formula, GroundTerm, PredicateId, Program, Rule, Term, Variable},
    self_restraint::SelfRestraintReliance,
};

#[derive(Debug, Clone)]
pub(super) struct Interpretation {
    facts: Vec<Fact>,
}

impl Interpretation {
    pub fn new(facts: Vec<Fact>) -> Self {
        Self { facts }
    }

    pub fn extend(mut self, other: Interpretation) -> Self {
        self.facts.extend(other.facts);

        self
    }

    pub fn facts(&self) -> &Vec<Fact> {
        &self.facts
    }
}

pub(super) type VariableAssignment = ClassAssignment<Variable, GroundTerm>;

pub(super) enum RelianceCheckResult {
    Abort,
    Success,
    Extend,
}

pub(super) trait RelianceImplementation {
    fn reliance_type() -> RelianceType;

    ///
    fn formula_source(rule: &Rule) -> &Formula;
    fn formula_target(rule: &Rule) -> &Formula;

    fn valid_assignment(
        term_source: &Term,
        term_target: &Term,
        assignment: &VariableAssignment,
    ) -> bool;

    fn check_conditions(
        mapped_atoms: &Vec<usize>,
        rule_source: &Rule,
        rule_target: &Rule,
        assignment: &VariableAssignment,
    ) -> RelianceCheckResult;
}

/// Possible types of reliances.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RelianceType {
    /// Positive reliance,
    /// i.e. the application of one rule may directly lead to the application of the other.
    Positive,
    /// Restraint reliance,
    /// i.e. the application of the second rule may make an earlier application of the first rule redundant.
    Restraint,
}

/// Directed labeled graph that marks reliances between rules.
///
/// The nodes are labeled with the rule indices.
/// The edges are labeled with the type of reliance.
pub type RelianceGraph = LabeledGraph<usize, RelianceType, Directed>;

/// Object from which a [`RelianceGraph`] can be created.
#[derive(Debug)]
pub struct RelianceGraphConstructor {
    program: Program,
}

impl RelianceGraphConstructor {
    /// Create a new [`RelianceGraphConstructor`].
    pub fn new(program: &crate::model::Program) -> Self {
        Self {
            program: Program::from_parsed_rules(program.rules()),
        }
    }

    /// Attempts to unify the given terms and updates the `VariableAssignment` accordingly.
    ///
    /// Returns `true` if the unifcation is successful and `false` otherwise.
    fn unify_terms(
        assignment: &mut VariableAssignment,
        term_source: &Term,
        term_target: &Term,
    ) -> bool {
        match term_source {
            Term::Variable(variable_source) => match term_target {
                Term::Variable(variable_target) => {
                    assignment.merge_classes(variable_source, variable_target)
                }
                Term::Ground(ground_target) => {
                    assignment.assign_value(variable_source, *ground_target)
                }
            },
            Term::Ground(ground_source) => match term_target {
                Term::Variable(variable_target) => {
                    assignment.assign_value(variable_target, *ground_source)
                }
                Term::Ground(ground_target) => ground_source == ground_target,
            },
        }
    }

    /// Attempts to extend the atom mapping by unifying `atom_source` with `atom_target`.
    /// If successful, returns the new `VariableAssignment`.
    fn extend_assignment<Implementation: RelianceImplementation>(
        atom_source: &Atom,
        atom_target: &Atom,
        assignment: &VariableAssignment,
    ) -> Option<VariableAssignment> {
        if !atom_source.compatible(atom_target) {
            return None;
        }

        let mut result = assignment.clone();

        for (term_source, term_target) in atom_source.terms.iter().zip(&atom_target.terms) {
            if !Self::unify_terms(&mut result, term_source, term_target) {
                return None;
            }

            if !Implementation::valid_assignment(term_source, term_target, assignment) {
                return None;
            }
        }

        Some(result)
    }

    /// Performs a depth-first search over all possible atom mappings,
    /// i.e. partial assignments of atoms from the target rule to atoms of the source rule,
    /// until every condition of the respective reliance is fulfilled.
    ///
    /// Depending on which condition failed,
    /// certain paths in the search tree may be aborted early.
    fn extend<Implementation: RelianceImplementation>(
        mapping_domain: &mut Vec<usize>,
        assignment: &VariableAssignment,
        rule_source: &Rule,
        rule_target: &Rule,
    ) -> bool {
        let body_target_start = mapping_domain.last().map_or_else(|| 0, |&s| s + 1);

        for (index_target, atom_target) in Implementation::formula_target(rule_target)
            .atoms()
            .iter()
            .enumerate()
            .skip(body_target_start)
        {
            mapping_domain.push(index_target);

            for atom_source in Implementation::formula_source(rule_source)
                .apply_restricted()
                .atoms()
            {
                if let Some(extended_assignment) =
                    Self::extend_assignment::<Implementation>(atom_source, atom_target, assignment)
                {
                    match Implementation::check_conditions(
                        mapping_domain,
                        rule_source,
                        rule_target,
                        &extended_assignment,
                    ) {
                        RelianceCheckResult::Abort => {
                            continue;
                        }
                        RelianceCheckResult::Success => {
                            return true;
                        }
                        RelianceCheckResult::Extend => {
                            return Self::extend::<Implementation>(
                                mapping_domain,
                                &extended_assignment,
                                rule_source,
                                rule_target,
                            )
                        }
                    }
                }
            }

            mapping_domain.pop();
        }

        false
    }

    /// Traverses each "promising" pair of rules and checks whether there is a reliance between them.
    /// If that is the case, the repspective edge is added to the graph.
    fn compute_reliances<Implementation: RelianceImplementation>(
        &self,
        predicate_map: &HashMap<PredicateId, Vec<usize>>,
        reliance_graph: &mut RelianceGraph,
    ) {
        for (index_source, rule_source) in self.program.rules().iter().enumerate() {
            // Indices of target rules for which the reliance relationship has already been computed
            let mut finish_set = HashSet::<usize>::new();

            for atom_head_source in rule_source.head().atoms() {
                if let Some(rule_target_indices) = predicate_map.get(&atom_head_source.predicate) {
                    for &index_target in rule_target_indices {
                        if finish_set.contains(&index_target) {
                            continue;
                        }
                        finish_set.insert(index_target);

                        let rule_target = &self.program.rules()[index_target];

                        let assignment = VariableAssignment::default();
                        let mut mapping_domain = Vec::<usize>::default();

                        if Self::extend::<PositiveReliance>(
                            &mut mapping_domain,
                            &assignment,
                            rule_source,
                            rule_target,
                        ) {
                            reliance_graph.add_edge(
                                index_source,
                                index_target,
                                RelianceType::Positive,
                            );
                        }
                    }
                }
            }
        }
    }

    /// Build a [`RelianceGraph`].
    pub fn build_reliance_graph(&self) -> RelianceGraph {
        // For each predicate, contains a list of rule indices that contain that predicate in its body
        let mut predicate_to_body = HashMap::<PredicateId, Vec<usize>>::new();
        // For each predicate, contains a list of rule indices that contain that predicate in its head
        // Only contains rule indices of existential rules
        let mut predicate_to_head_ex = HashMap::<PredicateId, Vec<usize>>::new();

        // Populate the above maps
        for (rule_index, rule) in self.program.rules().iter().enumerate() {
            for atom_body in rule.body().atoms() {
                let rules = predicate_to_body
                    .entry(atom_body.predicate.clone())
                    .or_default();
                rules.push(rule_index);
            }

            if !rule.is_existential() {
                continue;
            }

            for atom_head in rule.head().atoms() {
                let rules = predicate_to_head_ex
                    .entry(atom_head.predicate.clone())
                    .or_default();
                rules.push(rule_index);
            }
        }

        let mut reliance_graph = RelianceGraph::default();

        self.compute_reliances::<PositiveReliance>(&predicate_to_body, &mut reliance_graph);
        self.compute_reliances::<RestraintReliance>(&predicate_to_head_ex, &mut reliance_graph);

        reliance_graph
    }
}

#[cfg(test)]
mod test {
    use crate::{
        execution::selection_strategy::dependency_graph::reliances::common::RelianceType,
        io::parser::parse_program,
    };

    use super::RelianceGraphConstructor;

    fn test_graph(rule_a: &str, rule_b: &str, expected: Vec<(usize, usize, RelianceType)>) {
        let program_string = rule_a.to_string() + "\n" + rule_b;
        let program = parse_program(program_string).unwrap();

        let constructor = RelianceGraphConstructor::new(&program);
        let graph = constructor.build_reliance_graph();

        for (from, to, label) in &expected {
            assert!(graph.contains_labeled_edge(from, to, label));
        }

        for (from, to, label) in graph.edges() {
            let edge = (*from, *to, *label);
            assert!(expected.contains(&edge))
        }
    }

    #[test]
    fn positive_constants() {
        let rule_a = "a(c1, c2) :- b(c2, c1).";
        let rule_b = "c(c2, c1) :- a(c1, c2).";
        let expected = vec![(0, 1, RelianceType::Positive)];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_simple() {
        let rule_a = "a(?x, !v) :- b(?x, ?y).";
        let rule_b = "c(?x, ?y) :- a(?x, ?y).";
        let expected = vec![(0, 1, RelianceType::Positive)];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_simple_2() {
        let rule_a = "a(?x, ?x), c(?x, ?x) :- b(?x, ?y).";
        let rule_b = "d(?x, ?y) :- a(?x, ?y), c(?y, ?x).";
        let expected = vec![(0, 1, RelianceType::Positive)];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_null_1() {
        // Unifcation forces a null to be present before applying rule_a

        let rule_a = "h(?x, !v) :- b(?x).";
        let rule_b = "c(?y) :- h(?y, ?y).";
        let expected = vec![];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_null_2() {
        // Unifcation forces a null to be present before applying rule_a

        let rule_a = "h(!v, !x) :- b(?x).";
        let rule_b = "c(?y) :- h(?y, ?y).";
        let expected = vec![];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_null_3() {
        // There is no way to obtain c(null) from applying rule_a

        let rule_a = "h(?x, !v) :- b(?x).";
        let rule_b = "d(?x, ?y) :- h(?x, ?y), c(?y).";
        let expected = vec![];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_phi2ia() {
        // Every time rule_a is applicable, so is rule_b

        let rule_a = "a(?x, ?y), r(?x) :- a(?x, ?y).";
        let rule_b = "b(?x, ?y) :- a(?x, ?y).";
        let expected = vec![];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_phi2ia_ext() {
        // Mapping has to be extended to the whole body of rule_b.
        // Otherwise rule_b would have been applicable regardless of the application of rule_a

        let rule_a = "a(?x, ?x), r(?x) :- b(?x).";
        let rule_b = "c(?x, ?y) :- a(?x, ?y), a(?y, ?x).";
        let expected = vec![(0, 1, RelianceType::Positive)];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_psi1ia_ext() {
        // To prove the reliance, it is neccessary to extend the mapping to include the h(x, x) atom
        // Otherwise rule_a will be satisfied

        let rule_a = "h(?x, ?x), h(?x, !v), c(!v, ?x) :- c(?x, ?x), b(?x).";
        let rule_b = "d(?x, ?y) :- h(?x, ?y), c(?y, ?x), h(?x, ?x).";
        let expected = vec![(0, 1, RelianceType::Positive)];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_psi1ia_phi1() {
        // rule_a is always satisfied if there is a body match

        let rule_a = "a(!v, !v) :- a(?x, ?x).";
        let rule_b = "c(?y) :- a(?y, ?y).";
        let expected = vec![];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_psi1ia_phi1_phi22() {
        // Precondition of rule_a together with the precondition of rule_b
        // satisfy rule_a (there has to be an a(x, x) and h(x, x) present before applying rule_a)

        let rule_a = "h(?x, !v), a(!v, !v) :- a(?x, ?x), b(?x).";
        let rule_b = "c(?y) :- h(?x, ?x), h(?x, ?y).";
        let expected = vec![];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_psi1ia_phi22() {
        // Precondition of rule_b satisfies rule_a
        // (there has to be h(x, x) present before before applying rule_a)

        let rule_a = "h(?x, !v):- b(?x).";
        let rule_b = "c(?x) :- h(?x, ?x), h(?x, ?y).";
        let expected = vec![];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_psi2ib_phi1() {
        // Precondition of rule_a satisfies rule_b
        // Hence there is no reliance from rule_a to rule_b
        // However, there is still one from rule_b to rule_a

        let rule_a = "b(?x, ?y):- a(?x, ?y).";
        let rule_b = "a(?x, !v) :- b(?x, ?y).";
        let expected = vec![(1, 0, RelianceType::Positive)];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_psi2ib_phi22() {
        // Unification of the h-atoms forces a b(x, x) into the precondition hence satisfying rule_b

        let rule_a = "h(?x, ?x):- a(?x).";
        let rule_b = "b(?x, ?x) :- b(?x, ?y), h(?x, ?y).";
        let expected = vec![];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_psi2ib_psi1() {
        // rule_b will be satisfied with the derived facts from rule_a

        let rule_a = "h(?x, !v), t(?x, !v):- a(?x, ?y).";
        let rule_b = "t(!w, ?y) :- h(?x, ?y).";
        let expected = vec![];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_thesis() {
        let rule_a = "p(?x1, !v), q(!v, ?y1) :- a(?x1, ?y1).";
        let rule_b = "b(!w, !w) :- p(?x2, ?x2), p(?x2, ?y2), q(?y2, ?x2).";
        let expected = vec![(0, 1, RelianceType::Positive)];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_unification_1() {
        let rule_a = "h(?x, ?x, const) :- b(?x).";
        let rule_b = "c(!w) :- h(?y, other, ?y).";
        let expected = vec![];

        test_graph(rule_a, rule_b, expected);
    }

    #[test]
    fn positive_unification_2() {
        // Constant cannot be a null

        let rule_a = "h(?x, !v, const) :- b(?x).";
        let rule_b = "c(?y, ?z) :- h(?y, ?z, ?z).";
        let expected = vec![];

        test_graph(rule_a, rule_b, expected);
    }

    // #[test]
    // fn restraint_simple_1() {
    //     let rule_a = "h(x, v) :- b(x)";
    //     let rule_b = "h(x, y) :- a(x, y)";
    //     let expected = vec![(0, 1, RelianceType::Restraint)];

    //     test_graph(rule_a, rule_b, expected);
    // }

    // #[test]
    // fn restraint_simple_2() {
    //     let rule_a = "h(x, v, w), b(y, w, const) :- a(x, y)";
    //     let rule_b = "h(x, y, w), b(x, w, const) :- c(x, y)";
    //     let expected = vec![(0, 1, RelianceType::Restraint)];

    //     test_graph(rule_a, rule_b, expected);
    // }

    // #[test]
    // fn restraint_almost_blocked() {
    //     // Even though a(x, x), c(x, x) and h(x, x, n, n) will be present after applying rule_a,
    //     // rule_b is still applicable because there is no h(x, x, x, x)

    //     let rule_a = "h(x, y, v, v), h(y, x, v, v) :- a(x, y)";
    //     let rule_b = "h(x, y, x, y), c(x, x), a(x, x) :- c(x, y)";
    //     let expected = vec![(0, 1, RelianceType::Restraint)];

    //     test_graph(rule_a, rule_b, expected);
    // }
}