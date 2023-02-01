use std::collections::HashSet;

use crate::{
    logical::model::{Atom, Filter, FilterOperation, Identifier, Literal, Program, Term, Variable},
    physical::dictionary::Dictionary,
};

/// Represents the result of normalizing a list of atoms.
/// Normalized atoms do not contain constants or repeat variables in one atom.
#[derive(Debug)]
pub struct NormalizationResult {
    /// Normalized atoms.
    /// Don't contain constants or duplicate variables.
    pub atoms: Vec<Atom>,
    /// Filters.
    /// Do not contain Equality expressions.
    pub filters: Vec<Filter>,
}

/// Transforms a given atom vectors with filters into a "normalized" form.
/// Applies equality filters, e.g., "a(x, y), b(z), y = z" will turn into "a(x, y), b(y)".
/// Also, turns literals like "a(x, 3, x)" into "a(x, y, z), y = 3, z = x".
pub fn normalize_atom_vector(atoms: &[&Atom], filters: &[Filter]) -> NormalizationResult {
    let mut new_atoms: Vec<Atom> = atoms.iter().cloned().cloned().collect();
    let mut new_filters = Vec::<Filter>::new();

    // Apply all equality filters
    for filter in filters {
        if filter.operation == FilterOperation::Equals {
            if let Term::Variable(right_variable) = filter.right {
                for atom in new_atoms.iter_mut() {
                    for term in atom.terms_mut() {
                        if let Term::Variable(current_variable) = term {
                            if *current_variable == filter.left {
                                *current_variable = right_variable;
                            }
                        }
                    }
                }

                // Since we apply this filter we don't need to add it to new_filters
                continue;
            }
        }

        new_filters.push(filter.clone());
    }

    // Create new filters for handling constants or duplicate variables within one atom

    // TODO: This is horrible and should obviously not work that way,
    // but implementing this properly would need some discussions first
    const FRESH_ID: usize = usize::MAX - 10000;
    let mut current_id = FRESH_ID;

    for atom in new_atoms.iter_mut() {
        let mut atom_variables = HashSet::new();

        for term in atom.terms_mut() {
            if let Term::Variable(variable) = term {
                // If term is a variable we add a filter iff it has already occured
                if !atom_variables.insert(*variable) {
                    // Create fresh variable
                    let new_variable = match variable {
                        Variable::Universal(_) => Variable::Universal(Identifier(current_id)),
                        Variable::Existential(_) => Variable::Existential(Identifier(current_id)),
                    };

                    current_id += 1;

                    // Add new filter expression
                    new_filters.push(Filter::new(FilterOperation::Equals, new_variable, *term));

                    // Replace current term with the new variable
                    *term = Term::Variable(new_variable);
                }
            }
        }
    }

    NormalizationResult {
        atoms: new_atoms,
        filters: new_filters,
    }
}

impl<Dict: Dictionary> Program<Dict> {
    /// Transforms the rules into a "normalized" form.
    pub fn normalize(&mut self) {
        for rule in self.rules_mut() {
            let body_atoms: Vec<&Atom> = rule
                .body()
                .iter()
                .map(|l| {
                    if let Literal::Positive(a) = l {
                        a
                    } else {
                        // TODO:
                        panic!("We do not support negation yet")
                    }
                })
                .collect();

            let normalized_body = normalize_atom_vector(&body_atoms, &rule.filters());
            // TODO: Consider negation
            *rule.body_mut() = normalized_body
                .atoms
                .iter()
                .map(|a| Literal::Positive(a.clone()))
                .collect();
            *rule.filters_mut() = normalized_body.filters;
        }
    }
}
