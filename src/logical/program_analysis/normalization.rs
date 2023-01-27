use std::collections::HashSet;

use crate::{
    logical::model::{Filter, FilterOperation, Identifier, Literal, Program, Rule, Term, Variable},
    physical::dictionary::Dictionary,
};

// Transforms a given rule into a "normalized" form.
// Applies equality filters, e.g., "a(x, y), b(z), y = z" will turn into "a(x, y), b(y)".
// Also, turns literals like "a(x, 3, x)" into "a(x, y, z), y = 3, z = x".
fn normalize_rule(rule: &mut Rule) {
    // Apply all equality filters

    // We'll just remove everything from rules.filters and put stuff we want to keep here,
    // so we don't have to delete elements in the vector while iterating on it
    let mut new_filters = Vec::<Filter>::new();
    while !rule.filters().is_empty() {
        let filter = rule.filters_mut().pop().expect("Vector is not empty");

        if filter.operation == FilterOperation::Equals {
            if let Term::Variable(right_variable) = filter.right {
                for body_literal in rule.body_mut() {
                    // TODO: We dont support negation yet
                    if let Literal::Positive(body_atom) = body_literal {
                        for term in body_atom.terms_mut() {
                            if let Term::Variable(current_variable) = term {
                                if *current_variable == filter.left {
                                    *current_variable = right_variable;
                                }
                            }
                        }
                    }
                }

                // Since we apply this filter we don't need to add it to new_filters
                continue;
            }
        }

        new_filters.push(filter);
    }

    // Create new filters for handling constants or duplicate variables within one atom

    // TODO: This is horrible and should obviously not work that way,
    // but implementing this properly would need some discussions first
    const FRESH_ID: usize = usize::MAX - 10000;
    let mut current_id = FRESH_ID;

    for body_literal in rule.body_mut() {
        // TODO: We dont support negation yet
        if let Literal::Positive(body_atom) = body_literal {
            let mut atom_variables = HashSet::new();
            for term in body_atom.terms_mut() {
                let add_filter = if let Term::Variable(variable) = term {
                    // If term is a variable we add a filter iff it has already occured
                    !atom_variables.insert(*variable)
                } else {
                    // If term is not a variable then we need to add a filter
                    true
                };

                if add_filter {
                    // Create fresh variable
                    let new_variable = Variable::Universal(Identifier(current_id));
                    current_id += 1;

                    // Add new filter expression
                    new_filters.push(Filter::new(FilterOperation::Equals, new_variable, *term));

                    // Replace current term with the new variable
                    *term = Term::Variable(new_variable);
                }
            }
        }
    }

    // Don't forget to assign the new filters
    *rule.filters_mut() = new_filters;
}

impl<Dict: Dictionary> Program<Dict> {
    /// Transforms the rules into a "normalized" form.
    pub fn normalize(&mut self) {
        self.rules_mut().iter_mut().for_each(normalize_rule);
    }
}
