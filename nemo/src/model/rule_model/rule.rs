use std::collections::HashSet;

use crate::io::parser::ParseError;

use super::{Atom, Filter, Literal, Term, Variable};

/// A rule.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Rule {
    /// Head atoms of the rule
    head: Vec<Atom>,
    /// Body literals of the rule
    body: Vec<Literal>,
    /// Filters applied to the body
    filters: Vec<Filter>,
}

impl Rule {
    /// Construct a new rule.
    pub fn new(head: Vec<Atom>, body: Vec<Literal>, filters: Vec<Filter>) -> Self {
        Self {
            head,
            body,
            filters,
        }
    }

    /// Construct a new rule, validating constraints on variable usage.
    pub(crate) fn new_validated(
        head: Vec<Atom>,
        body: Vec<Literal>,
        filters: Vec<Filter>,
    ) -> Result<Self, ParseError> {
        // Check if existential variables occur in the body.
        let existential_variables = body
            .iter()
            .flat_map(|literal| literal.existential_variables())
            .collect::<Vec<_>>();

        if !existential_variables.is_empty() {
            return Err(ParseError::BodyExistential(
                existential_variables
                    .first()
                    .expect("is not empty here")
                    .name(),
            ));
        }

        // Check if some variable in the body occurs only in negative literals.
        let (positive, negative): (Vec<_>, Vec<_>) = body
            .iter()
            .cloned()
            .partition(|literal| literal.is_positive());
        let negative_variables = negative
            .iter()
            .flat_map(|literal| literal.universal_variables())
            .collect::<HashSet<_>>();
        let positive_varibales = positive
            .iter()
            .flat_map(|literal| literal.universal_variables())
            .collect();
        let negative_variables = negative_variables
            .difference(&positive_varibales)
            .collect::<Vec<_>>();

        if !negative_variables.is_empty() {
            return Err(ParseError::UnsafeNegatedVariable(
                negative_variables
                    .first()
                    .expect("is not empty here")
                    .name(),
            ));
        }

        // Check if a variable occurs with both existential and universal quantification.
        let universal_variables = body
            .iter()
            .flat_map(|literal| literal.universal_variables())
            .collect::<HashSet<_>>();

        let existential_variables = head
            .iter()
            .flat_map(|atom| atom.existential_variables())
            .collect();

        let common_variables = universal_variables
            .intersection(&existential_variables)
            .take(1)
            .collect::<Vec<_>>();

        if !common_variables.is_empty() {
            return Err(ParseError::BothQuantifiers(
                common_variables.first().expect("is not empty here").name(),
            ));
        }

        // Check if there are universal variables in the head which do not occur in a positive body literal
        let head_universal_variables = head
            .iter()
            .flat_map(|atom| atom.universal_variables())
            .collect::<HashSet<_>>();

        for head_variable in head_universal_variables {
            if !universal_variables.contains(head_variable) {
                return Err(ParseError::UnsafeHeadVariable(head_variable.name()));
            }
        }

        // Check if filters are correctly formed
        for filter in &filters {
            let mut filter_variables = vec![&filter.lhs];

            if let Term::Variable(right_variable) = &filter.rhs {
                filter_variables.push(right_variable);
            }

            for variable in filter_variables {
                match variable {
                    Variable::Universal(universal_variable) => {
                        if !positive_varibales.contains(variable) {
                            return Err(ParseError::UnsafeFilterVariable(
                                universal_variable.name(),
                            ));
                        }
                    }
                    Variable::Existential(existential_variable) => {
                        return Err(ParseError::BodyExistential(existential_variable.name()))
                    }
                }
            }
        }

        Ok(Rule {
            head,
            body,
            filters,
        })
    }

    /// Return the head atoms of the rule - immutable.
    #[must_use]
    pub fn head(&self) -> &Vec<Atom> {
        &self.head
    }

    /// Return the head atoms of the rule - mutable.
    #[must_use]
    pub fn head_mut(&mut self) -> &mut Vec<Atom> {
        &mut self.head
    }

    /// Return the body literals of the rule - immutable.
    #[must_use]
    pub fn body(&self) -> &Vec<Literal> {
        &self.body
    }

    /// Return the body literals of the rule - mutable.
    #[must_use]
    pub fn body_mut(&mut self) -> &mut Vec<Literal> {
        &mut self.body
    }

    /// Return the filters of the rule - immutable.
    #[must_use]
    pub fn filters(&self) -> &Vec<Filter> {
        &self.filters
    }

    /// Return the filters of the rule - mutable.
    #[must_use]
    pub fn filters_mut(&mut self) -> &mut Vec<Filter> {
        &mut self.filters
    }
}
