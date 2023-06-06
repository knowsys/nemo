//! Bundles functionality for checking the satisfiablity of a formula.

use std::collections::{hash_map::Entry, HashMap};

use super::{
    common::Interpretation,
    rules::{Atom, Formula, GroundTerm, Term, Variable, VariableId},
};

/// Encodes a mapping from existential variables to ground terms.
#[derive(Debug, Default, Clone)]
struct ExistentialMapping {
    map: HashMap<VariableId, GroundTerm>,
}

impl ExistentialMapping {
    /// Assign a ground term to the given variable.
    ///
    /// Returns `true` if the variable was previously unassigned or assigned to the same term .
    /// Returns `false` otherwise.
    pub fn assign(&mut self, variable: VariableId, term: &GroundTerm) -> bool {
        match self.map.entry(variable) {
            Entry::Occupied(entry) => entry.get() == term,
            Entry::Vacant(entry) => {
                entry.insert(*term);
                true
            }
        }
    }

    /// Extend `self` by all the assignments in `other`.
    ///
    /// Returns `true` if there is no conflict,
    /// i.e. if there are no variables that are assigned to different ground terms by the other mapping.
    /// Returns `false` otherwise.
    pub fn extend(&mut self, other: &Self) -> bool {
        for (&variable, term) in &other.map {
            if !self.assign(variable, term) {
                return false;
            }
        }

        true
    }
}

/// Represents the search space that needs to be traversed
/// in order to determine whether a formula is satisfied by some interpretation.
#[derive(Debug)]
struct SatisfySearchSpace {
    /// For each atom in the formula,
    /// contains a list of [`ExistentialMapping`]s,
    /// which represents all the potential assignments that would satisfy the respective atom.
    mapping_options: Vec<Vec<ExistentialMapping>>,
}

impl SatisfySearchSpace {
    /// Create a new [`SatisfySearchSpace`].
    ///
    /// Returns `None` if it can be determined without expesive search that the formula is unsatisfiable.
    fn create(formula: &Formula, interpretation: &Interpretation) -> Option<Self> {
        let mut mapping_options = Vec::<Vec<ExistentialMapping>>::new();

        for atom in formula.atoms() {
            let options = Self::atom_options(atom, interpretation);

            if options.is_empty() {
                return None;
            }

            mapping_options.push(options);
        }

        Some(Self { mapping_options })
    }

    /// Return all the possible assignments that would satisfy the given atom in the formula.
    ///
    /// # Panics
    /// Panics if the atom contains any universal variables.
    fn atom_options(atom: &Atom, interpretation: &Interpretation) -> Vec<ExistentialMapping> {
        let mut options = Vec::<ExistentialMapping>::new();

        for fact in interpretation.facts() {
            if !fact.compatible(atom) {
                continue;
            }

            let mut mapping = ExistentialMapping::default();
            let mut is_consistent = true;

            for (term_index, atom_term) in atom.terms.iter().enumerate() {
                let ground_term = &fact.terms[term_index];

                match atom_term {
                    Term::Variable(Variable::Existential(variable)) => {
                        is_consistent &= mapping.assign(*variable, ground_term);
                    }
                    Term::Variable(Variable::Universal(_)) => {
                        panic!("Function assumes that there are no universal variables in teh formula.");
                    }
                    Term::Ground(ground) => is_consistent &= ground == ground_term,
                }
            }

            if is_consistent {
                options.push(mapping);
            }
        }

        options
    }

    /// Recursive part of the function `search`.
    fn search_deep(&self, mapping: ExistentialMapping, layer: usize) -> bool {
        if layer >= self.mapping_options.len() {
            return true;
        }

        for partial_mapping in &self.mapping_options[layer] {
            let mut current_mapping = mapping.clone();
            if !current_mapping.extend(partial_mapping) {
                continue;
            }

            if self.search_deep(current_mapping.clone(), layer + 1) {
                return true;
            }
        }

        false
    }

    /// Traverses the search space in order to find a satisfying assignment.
    ///
    /// Returns `true` if the search is successfull, i.e. the formula is satisfiable.
    /// Returns `false` otherwise.
    fn search(&self) -> bool {
        self.search_deep(ExistentialMapping::default(), 0)
    }
}

/// Checks whether the given formula is satisfiable in the given interpretation.
///
/// A formula is satisfiable if it is possible to uniformly replace every existential variable in the formula
/// with a ground term such that (after the replacement) every atom occurs in the interpretation.
///
/// # Panics
/// Panics if the formula contains any universal variables.
pub(super) fn is_satisfiable(formula: &Formula, interpretation: &Interpretation) -> bool {
    if let Some(search_space) = SatisfySearchSpace::create(formula, interpretation) {
        return search_space.search();
    }

    false
}

#[cfg(test)]
mod test {
    use crate::execution::selection_strategy::dependency_graph::reliances::{
        common::Interpretation,
        rules::{Atom, Fact, Formula, GroundTerm, Term, Variable},
        satisfy::is_satisfiable,
    };

    #[test]
    fn satisfy_single_atom() {
        let c_0 = GroundTerm::Constant(0);
        let c_1 = GroundTerm::Constant(1);

        let interpretation = Interpretation::new(vec![
            Fact::new("p", vec![c_1, c_1]),
            Fact::new("q", vec![c_0, c_1, c_0]),
        ]);

        let c_0 = Term::Ground(GroundTerm::Constant(0));
        let c_1 = Term::Ground(GroundTerm::Constant(1));
        let v_0 = Term::Variable(Variable::Existential(0));
        let v_1 = Term::Variable(Variable::Existential(1));

        let formula = Formula::new(vec![Atom::new("p", vec![c_1, v_0])]);
        assert!(is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![Atom::new("p", vec![v_0, c_1])]);
        assert!(is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![Atom::new("p", vec![c_1, c_1])]);
        assert!(is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![Atom::new("p", vec![v_0, v_0])]);
        assert!(is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![Atom::new("p", vec![v_0, v_1])]);
        assert!(is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![Atom::new("p", vec![v_0, c_0])]);
        assert!(!is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![Atom::new("p", vec![c_0, v_0])]);
        assert!(!is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![Atom::new("q", vec![v_0, c_1, v_0])]);
        assert!(is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![Atom::new("q", vec![v_0, c_0, v_0])]);
        assert!(!is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![Atom::new("q", vec![v_0, v_1, c_0])]);
        assert!(is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![Atom::new("q", vec![v_0, v_1, c_1])]);
        assert!(!is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![Atom::new("q", vec![v_0, v_1, c_0])]);
        assert!(is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![
            Atom::new("p", vec![v_1, v_1]),
            Atom::new("q", vec![v_0, v_1, c_0]),
        ]);
        assert!(is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![
            Atom::new("p", vec![v_1, v_1]),
            Atom::new("q", vec![v_1, v_0, c_0]),
        ]);
        assert!(!is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![
            Atom::new("p", vec![v_1, v_1]),
            Atom::new("p", vec![c_1, c_1]),
            Atom::new("p", vec![c_1, v_0]),
            Atom::new("p", vec![v_0, c_1]),
        ]);
        assert!(is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![
            Atom::new("p", vec![v_1, v_1]),
            Atom::new("p", vec![c_1, c_1]),
            Atom::new("p", vec![c_0, v_0]),
            Atom::new("p", vec![v_0, c_1]),
        ]);
        assert!(!is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![
            Atom::new("p", vec![v_1, v_1]),
            Atom::new("p", vec![c_1, c_0]),
            Atom::new("p", vec![c_0, v_0]),
            Atom::new("p", vec![v_0, c_1]),
        ]);
        assert!(!is_satisfiable(&formula, &interpretation));
    }

    #[test]
    fn satisfy_multiple_atoms() {
        let c_0 = GroundTerm::Constant(0);
        let c_1 = GroundTerm::Constant(1);

        let interpretation = Interpretation::new(vec![
            Fact::new("p", vec![c_1, c_1]),
            Fact::new("q", vec![c_0, c_1, c_0]),
            Fact::new("q", vec![c_0, c_0, c_1]),
        ]);

        let c_0 = Term::Ground(GroundTerm::Constant(0));
        let v_0 = Term::Variable(Variable::Existential(0));
        let v_1 = Term::Variable(Variable::Existential(1));
        let v_2 = Term::Variable(Variable::Existential(2));

        let formula = Formula::new(vec![
            Atom::new("p", vec![v_1, v_1]),
            Atom::new("q", vec![c_0, v_0, v_1]),
        ]);
        assert!(is_satisfiable(&formula, &interpretation));

        let formula = Formula::new(vec![
            Atom::new("p", vec![v_1, v_1]),
            Atom::new("q", vec![v_1, v_0, v_2]),
        ]);
        assert!(!is_satisfiable(&formula, &interpretation));
    }
}
