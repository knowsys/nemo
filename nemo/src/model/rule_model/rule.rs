use std::collections::HashSet;

use crate::{io::parser::ParseError, model::VariableAssignment};

use super::{Atom, Condition, Literal, Variable};

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
        // TODO: Rework this to use the concept of
        // 1. Bound variable = Primitive occuring in positve literal
        // 2. Derived variable = Condition Assignment only using bound variables
        // 3. Head and all literals may only use variables of type 1 and 2 (head may also use existentials)
        // 4. Exception: Negative literals may use an unsafe variable as before

        // Check if existential variables occur in the body.
        let existential_variables_body = body
            .iter()
            .flat_map(|literal| literal.existential_variables())
            .collect::<Vec<_>>();

        if let Some(variable) = existential_variables_body.first() {
            return Err(ParseError::BodyExistential(variable.name()));
        }

        // Check if some variable in the body occurs only in negative literals.
        let (positive, negative): (Vec<_>, Vec<_>) = body
            .iter()
            .cloned()
            .partition(|literal| literal.is_positive());
        let positive_variables = positive
            .iter()
            .flat_map(|literal| literal.variables())
            .collect::<HashSet<Variable>>();

        let mut unsafe_negative_variables = HashSet::<Variable>::new();
        for negative_literal in negative {
            let mut current_unsafe = HashSet::<Variable>::new();

            for variable in negative_literal.variables() {
                if positive_variables.contains(&variable) {
                    continue;
                }

                if unsafe_negative_variables.contains(&variable) {
                    return Err(ParseError::UnsafeVariableInMulltipleNegativeLiterals(
                        variable.name(),
                    ));
                }

                current_unsafe.insert(variable.clone());
            }

            unsafe_negative_variables.extend(current_unsafe)
        }

        // Check if a variable occurs with both existential and universal quantification.
        let universal_variables_names = body
            .iter()
            .flat_map(|literal| {
                literal
                    .universal_variables()
                    .into_iter()
                    .map(|v| v.name())
                    .collect::<HashSet<String>>()
            })
            .collect::<HashSet<String>>();

        let existential_variables_names = head
            .iter()
            .flat_map(|atom| {
                atom.existential_variables()
                    .into_iter()
                    .map(|v| v.name())
                    .collect::<HashSet<String>>()
            })
            .collect();

        let common_variable_names = universal_variables_names
            .intersection(&existential_variables_names)
            .take(1)
            .collect::<Vec<_>>();

        if let Some(&&common_variable) = common_variable_names.first() {
            return Err(ParseError::BothQuantifiers(common_variable));
        }

        //
        let mut defined_condition_variables = HashSet::<Variable>::new();

        // Check if there are universal variables in the head which do not occur in a positive body literal
        let head_universal_variables = head
            .iter()
            .flat_map(|atom| atom.universal_variables())
            .collect::<HashSet<_>>();

        for head_variable in &head_universal_variables {
            if !positive_variables.contains(head_variable) {
                return Err(ParseError::UnsafeHeadVariable(head_variable.name()));
            }
        }

        // Check if conditions are correctly formed
        for condition in &conditions {
            for variable in &condition.variables() {
                match variable {
                    Variable::Universal(universal_variable) => {
                        if !positive_variables.contains(variable) {
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

        // Aggregates
        {
            // Check for aggregates in the body of a rule
            for literal in &body {
                #[allow(clippy::never_loop)]
                for aggregate in literal.aggregates() {
                    return Err(ParseError::AggregateInBody(aggregate.clone()));
                }
            }

            // Check that variables used in aggregates also occur positively in the rule body
            for literal in &head {
                for aggregate in literal.aggregates() {
                    for variable_identifier in &aggregate.variable_identifiers {
                        if !positive_variables
                            .contains(&Variable::Universal(variable_identifier.clone()))
                        {
                            return Err(ParseError::UnsafeHeadVariable(variable_identifier.name()));
                        }
                    }
                }
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
