use std::collections::HashSet;

use crate::model::chase_model::{ChaseAtom, ChaseProgram};
use crate::model::{Filter, FilterOperation, Identifier, Term, Variable};

/// Represents the result of normalizing a list of atoms.
/// Normalized atoms do not contain constants or repeat variables in one atom.
#[derive(Debug)]
pub struct NormalizationResult {
    /// Normalized atoms.
    /// Don't contain constants or duplicate variables.
    pub atoms: Vec<ChaseAtom>,
    /// Filters.
    /// Do not contain Equality expressions.
    pub filters: Vec<Filter>,
}

/// Transforms a given atom vectors with filters into a "normalized" form.
/// Applies equality filters, e.g., "a(x, y), b(z), y = z" will turn into "a(x, y), b(y)".
/// Also, turns literals like "a(x, 3, x)" into "a(x, y, z), y = 3, z = x".
pub fn normalize_atom_vector(
    atoms: &[ChaseAtom],
    filters: &[Filter],
    term_counter: &mut usize,
) -> NormalizationResult {
    let mut new_atoms: Vec<ChaseAtom> = atoms.to_vec();
    let mut new_filters = Vec::<Filter>::new();

    // Apply all equality filters
    for filter in filters {
        if filter.operation == FilterOperation::Equals {
            if let Term::Variable(right_variable) = &filter.rhs {
                for atom in new_atoms.iter_mut() {
                    for term in atom.terms_mut() {
                        if let Term::Variable(current_variable) = term {
                            if *current_variable == filter.lhs {
                                *current_variable = right_variable.clone();
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
    for atom in new_atoms.iter_mut() {
        let mut atom_variables = HashSet::new();

        for term in atom.terms_mut() {
            *term_counter += 1;

            let add_filter = if let Term::Variable(variable) = term {
                // If term is a variable we add a filter iff it has already occured
                !atom_variables.insert(variable.clone())
            } else {
                // If term is not a variable then we need to add a filter
                true
            };

            if add_filter {
                // Create fresh variable
                let new_variable = Variable::Universal(Identifier(format!(
                    "_FRESH_VARIABLE_IN_NORMALIZATION_FOR_TERM_{term_counter}"
                )));

                // Add new filter expression
                new_filters.push(Filter::new(
                    FilterOperation::Equals,
                    new_variable.clone(),
                    term.clone(),
                ));

                // Replace current term with the new variable
                *term = Term::Variable(new_variable);
            }
        }
    }

    NormalizationResult {
        atoms: new_atoms,
        filters: new_filters,
    }
}

impl ChaseProgram {
    /// Transforms the rules into a "normalized" form.
    pub fn normalize(&mut self) {
        for rule in self.rules_mut() {
            // Used to create unique names for new variables
            let mut term_counter = 0;

            let normalized_positive = normalize_atom_vector(
                rule.positive_body(),
                rule.positive_filters(),
                &mut term_counter,
            );
            let normalized_negative = normalize_atom_vector(
                rule.negative_body(),
                rule.negative_filters(),
                &mut term_counter,
            );

            *rule.positive_body_mut() = normalized_positive.atoms;
            *rule.positive_filters_mut() = normalized_positive.filters;
            *rule.negative_body_mut() = normalized_negative.atoms;
            *rule.negative_filters_mut() = normalized_negative.filters;
        }
    }
}
