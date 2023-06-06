//! Bundles functionaly for checking the satisfiablity of a formula.

use std::collections::{hash_map::Entry, HashMap};

use crate::util::equivalence_class::EquivalenceRelation;

use super::{
    common::{Interpretation, VariableAssignment},
    rules::{Atom, Formula, GroundTerm, Term, Variable, VariableId},
};

#[derive(Debug, Default, Clone)]
struct ExistentialMapping {
    map: HashMap<VariableId, GroundTerm>,
}

impl ExistentialMapping {
    pub fn new(map: HashMap<VariableId, GroundTerm>) -> Self {
        Self { map }
    }

    pub fn assign(&mut self, variable: VariableId, term: &GroundTerm) -> bool {
        match self.map.entry(variable) {
            Entry::Occupied(entry) => entry.get() == term,
            Entry::Vacant(entry) => {
                entry.insert(*term);
                false
            }
        }
    }

    pub fn extend(&mut self, other: &Self) -> bool {
        for (&variable, term) in &other.map {
            if !self.assign(variable, term) {
                return false;
            }
        }

        true
    }
}

struct SatisfySearchSpace {
    mapping_options: Vec<Vec<ExistentialMapping>>,
}

impl SatisfySearchSpace {
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

    fn atom_options(atom: &Atom, interpretation: &Interpretation) -> Vec<ExistentialMapping> {
        let mut options = Vec::<ExistentialMapping>::new();

        for ground_atom in interpretation.atoms() {
            if !ground_atom.compatible(atom) {
                continue;
            }

            let mut mapping = ExistentialMapping::default();
            let mut is_consistent = true;

            for (term_index, atom_term) in atom.terms.iter().enumerate() {
                let ground_term = &ground_atom.terms[term_index];

                if let &Term::Variable(Variable::Existential(variable)) = atom_term {
                    is_consistent &= mapping.assign(variable, ground_term);
                }
            }

            if is_consistent {
                options.push(mapping);
            }
        }

        options
    }

    fn search_deep(&self, mapping: ExistentialMapping, layer: usize) -> bool {
        if self.mapping_options.len() >= layer {
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

    fn search(&self) -> bool {
        self.search_deep(ExistentialMapping::default(), 0)
    }
}

pub(super) fn is_satisfiable(formula: &Formula, interpretation: &Interpretation) -> bool {
    if let Some(search_space) = SatisfySearchSpace::create(formula, interpretation) {
        return search_space.search();
    }

    false
}
