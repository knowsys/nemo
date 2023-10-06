use std::collections::HashSet;

use crate::{io::parser::ParseError, model::VariableAssignment};

use super::{Atom, Condition, Literal, PrimitiveTerm, Variable};

/// A rule.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Rule {
    /// Head atoms of the rule
    head: Vec<Atom>,
    /// Body literals of the rule
    body: Vec<Literal>,
    /// Conditions expressed within the left side of the rule
    conditions: Vec<Condition>,
}

impl Rule {
    /// Construct a new rule.
    pub fn new(head: Vec<Atom>, body: Vec<Literal>, conditions: Vec<Condition>) -> Self {
        Self {
            head,
            body,
            conditions,
        }
    }

    /// Construct a new rule, validating constraints on variable usage.
    pub(crate) fn new_validated(
        head: Vec<Atom>,
        body: Vec<Literal>,
        conditions: Vec<Condition>,
    ) -> Result<Self, ParseError> {
        // All the existential variables used in the rule
        let existential_variable_names = head
            .iter()
            .flat_map(|a| a.existential_variables().map(|v| v.name()))
            .collect::<HashSet<_>>();

        for variable in body
            .iter()
            .flat_map(|l| l.variables())
            .chain(conditions.iter().flat_map(|c| c.variables()))
        {
            // Existential variables may only occur in the head
            if variable.is_existential() {
                return Err(ParseError::BodyExistential(variable.name()));
            }

            // There may not be a universal variable whose name is the same that of an existential
            if existential_variable_names.contains(&variable.name()) {
                return Err(ParseError::BothQuantifiers(variable.name()));
            }
        }

        // Divide the literals into a positive and a negative part
        let (positive, negative): (Vec<_>, Vec<_>) = body
            .iter()
            .cloned()
            .partition(|literal| literal.is_positive());

        // Bound variables are considered to be all variables occuring as primitive terms in a positive body literal
        let bound_variables = positive
            .iter()
            .flat_map(|l| l.primitive_terms())
            .filter_map(|t| {
                if let PrimitiveTerm::Variable(variable) = t {
                    Some(variable)
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();

        // A derived variable is a variable which is defined in terms of bound variables
        let mut derived_variables = HashSet::<&Variable>::new();
        for condition in &conditions {
            if let Condition::Assignment(variable, term) = condition {
                if bound_variables.contains(variable) || derived_variables.contains(variable) {
                    return Err(ParseError::MultipleDefinitions(variable.name()));
                }

                for term_variable in term.variables() {
                    if !bound_variables.contains(term_variable) {
                        return Err(ParseError::UnsafeDefinition(variable.name()));
                    }
                }

                derived_variables.insert(variable);
            }
        }

        // Each complex term in the body must only use derived or bound variables
        for term in body
            .iter()
            .flat_map(|l| l.terms())
            .chain(conditions.iter().flat_map(|c| c.terms()))
            .chain(head.iter().flat_map(|a| a.terms()))
        {
            if term.is_primitive() {
                continue;
            }

            for variable in term.variables() {
                if !bound_variables.contains(variable) && !derived_variables.contains(variable) {
                    return Err(ParseError::UnsafeComplexTerm(
                        term.to_string(),
                        variable.name(),
                    ));
                }
            }
        }

        // Head atoms may only use variables that are either bound or derived
        for variable in head.iter().flat_map(|a| a.variables()) {
            if !bound_variables.contains(variable) && !derived_variables.contains(variable) {
                return Err(ParseError::UnsafeHeadVariable(variable.name()));
            }
        }

        // Unsafe variables in negative literals may only be used within one atom
        let mut unsafe_negative_variables = HashSet::<Variable>::new();
        for negative_literal in negative {
            let mut current_unsafe = HashSet::<Variable>::new();

            for primitive_term in negative_literal.primitive_terms() {
                if let PrimitiveTerm::Variable(variable) = primitive_term {
                    if derived_variables.contains(&variable) || bound_variables.contains(variable) {
                        continue;
                    }

                    if unsafe_negative_variables.contains(&variable) {
                        return Err(ParseError::UnsafeVariableInMultipleNegativeLiterals(
                            variable.name(),
                        ));
                    }

                    current_unsafe.insert(variable.clone());
                }
            }

            unsafe_negative_variables.extend(current_unsafe)
        }

        // Check for aggregates in the body of a rule
        for literal in &body {
            #[allow(clippy::never_loop)]
            for aggregate in literal.aggregates() {
                return Err(ParseError::AggregateInBody(aggregate.clone()));
            }
        }

        // Check if aggregate is used within another complex term
        for term in head.iter().flat_map(|a| a.terms()) {
            if term.aggregate_subterm() {
                return Err(ParseError::AggregateSubterm(term.to_string()));
            }
        }

        Ok(Rule {
            head,
            body,
            conditions,
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

    /// Return the conditions of the rule - immutable.
    #[must_use]
    pub fn conditions(&self) -> &Vec<Condition> {
        &self.conditions
    }

    /// Return the filters of the rule - mutable.
    #[must_use]
    pub fn conditions_mut(&mut self) -> &mut Vec<Condition> {
        &mut self.conditions
    }

    /// Replaces [`Variable`]s with [`Term`]s according to the provided assignment.
    pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
        self.body
            .iter_mut()
            .for_each(|l| l.apply_assignment(assignment));
        self.head
            .iter_mut()
            .for_each(|a| a.apply_assignment(assignment));
        self.conditions
            .iter_mut()
            .for_each(|f| f.apply_assignment(assignment));
    }
}

impl std::fmt::Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, atom) in self.head.iter().enumerate() {
            atom.fmt(f)?;

            if index < self.head.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(" :- ")?;

        for (index, literal) in self.body.iter().enumerate() {
            literal.fmt(f)?;

            if index < self.body.len() - 1 {
                f.write_str(", ")?;
            }
        }

        if !self.conditions.is_empty() {
            f.write_str(", ")?;
        }

        for (index, filter) in self.conditions.iter().enumerate() {
            filter.fmt(f)?;

            if index < self.conditions.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(" .")
    }
}
