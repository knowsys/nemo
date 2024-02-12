use std::collections::HashSet;

use crate::{io::parser::ParseError, model::VariableAssignment};

use super::{Atom, Constraint, Literal, PrimitiveTerm, Term, Variable};

/// A rule.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Rule {
    /// Head atoms of the rule
    head: Vec<Atom>,
    /// Body literals of the rule
    body: Vec<Literal>,
    /// Constraints on the body of the rule
    constraints: Vec<Constraint>,
}

impl Rule {
    /// Construct a new rule.
    pub fn new(head: Vec<Atom>, body: Vec<Literal>, constraints: Vec<Constraint>) -> Self {
        Self {
            head,
            body,
            constraints,
        }
    }

    /// Construct a new rule, validating constraints on variable usage.
    pub(crate) fn new_validated(
        head: Vec<Atom>,
        body: Vec<Literal>,
        constraints: Vec<Constraint>,
    ) -> Result<Self, ParseError> {
        // All the existential variables used in the rule
        let existential_variable_names = head
            .iter()
            .flat_map(|a| a.existential_variables().flat_map(|v| v.name()))
            .collect::<HashSet<_>>();

        for variable in body
            .iter()
            .flat_map(|l| l.variables())
            .chain(constraints.iter().flat_map(|c| c.variables()))
        {
            // Existential variables may only occur in the head
            if variable.is_existential() {
                return Err(ParseError::BodyExistential(variable.clone()));
            }

            // There may not be a universal variable whose name is the same that of an existential
            if let Some(name) = variable.name() {
                if existential_variable_names.contains(&name) {
                    return Err(ParseError::BothQuantifiers(name));
                }
            }
        }

        // Divide the literals into a positive and a negative part
        let (positive, negative): (Vec<_>, Vec<_>) = body
            .iter()
            .cloned()
            .partition(|literal| literal.is_positive());

        // Safe variables are considered to be all variables occuring as primitive terms in a positive body literal
        let safe_variables = Self::safe_variables_literals(&positive);

        // A derived variable is a variable which is defined in terms of bound variables
        // using a simple equality operation
        let mut derived_variables = HashSet::<&Variable>::new();
        for constraint in &constraints {
            if let Some((variable, term)) = constraint.has_form_assignment() {
                if safe_variables.contains(variable) {
                    continue;
                }

                for term_variable in term.variables() {
                    if !safe_variables.contains(term_variable) {
                        return Err(ParseError::UnsafeDefinition(variable.clone()));
                    }
                }

                if !derived_variables.contains(variable) {
                    derived_variables.insert(variable);
                } else {
                    return Err(ParseError::MultipleDefinitions(variable.clone()));
                }
            }
        }

        // Each complex term in the body and head must only use safe variables
        // TODO: Allow the use of derived variables.
        //       To implement this, the planner has to apply the filters
        //       after the appends and not right after the join
        for term in body
            .iter()
            .flat_map(|l| l.terms())
            .chain(head.iter().flat_map(|a| a.terms()))
        {
            if term.is_primitive() {
                continue;
            }

            for variable in term.variables() {
                if !safe_variables.contains(variable) {
                    return Err(ParseError::UnsafeComplexTerm(
                        term.to_string(),
                        variable.clone(),
                    ));
                }
            }
        }

        // Every constraint that is not an assignment must only use derived or safe varaibles
        for constraint in &constraints {
            if let Some((variable, _)) = constraint.has_form_assignment() {
                if derived_variables.contains(variable) {
                    continue;
                }
            }

            for term in [constraint.left(), constraint.right()] {
                for variable in term.variables() {
                    if !safe_variables.contains(variable) && !derived_variables.contains(variable) {
                        return Err(ParseError::UnsafeComplexTerm(
                            term.to_string(),
                            variable.clone(),
                        ));
                    }
                }
            }
        }

        // Head atoms may only use variables that are either bound or derived
        for variable in head.iter().flat_map(|a| a.variables()) {
            if variable.is_unnamed() {
                return Err(ParseError::UnnamedInHead);
            }

            if variable.is_universal()
                && !safe_variables.contains(variable)
                && !derived_variables.contains(variable)
            {
                return Err(ParseError::UnsafeHeadVariable(variable.clone()));
            }
        }

        // Unsafe variables in negative literals may only be used within one atom
        let mut unsafe_negative_variables = HashSet::<Variable>::new();
        for negative_literal in negative {
            let mut current_unsafe = HashSet::<Variable>::new();

            for primitive_term in negative_literal.primitive_terms() {
                if let PrimitiveTerm::Variable(variable) = primitive_term {
                    if derived_variables.contains(&variable) || safe_variables.contains(variable) {
                        continue;
                    }

                    if unsafe_negative_variables.contains(variable) {
                        return Err(ParseError::UnsafeVariableInMultipleNegativeLiterals(
                            variable.clone(),
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

        Ok(Rule {
            head,
            body,
            constraints,
        })
    }

    /// Return all variables that are "safe".
    /// A variable is safe if it occurs in a positive body literal.
    fn safe_variables_literals(literals: &[Literal]) -> HashSet<Variable> {
        let mut result = HashSet::new();

        for literal in literals {
            if let Literal::Positive(atom) = literal {
                for term in atom.terms() {
                    if let Term::Primitive(PrimitiveTerm::Variable(variable)) = term {
                        result.insert(variable.clone());
                    }
                }
            }
        }

        result
    }

    /// Return all variables that are "safe".
    /// A variable is safe if it occurs in a positive body literal.
    pub fn safe_variables(&self) -> HashSet<Variable> {
        Self::safe_variables_literals(&self.body)
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

    /// Return the constraints of the rule - immutable.
    #[must_use]
    pub fn constraints(&self) -> &Vec<Constraint> {
        &self.constraints
    }

    /// Return the filters of the rule - mutable.
    #[must_use]
    pub fn constraints_mut(&mut self) -> &mut Vec<Constraint> {
        &mut self.constraints
    }

    /// Replaces [`Variable`]s with [`super::Term`]s according to the provided assignment.
    pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
        self.body
            .iter_mut()
            .for_each(|l| l.apply_assignment(assignment));
        self.head
            .iter_mut()
            .for_each(|a| a.apply_assignment(assignment));
        self.constraints
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

        if !self.constraints.is_empty() {
            f.write_str(", ")?;
        }

        for (index, constraint) in self.constraints.iter().enumerate() {
            constraint.fmt(f)?;

            if index < self.constraints.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(" .")
    }
}
