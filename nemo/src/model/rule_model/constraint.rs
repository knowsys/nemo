use crate::model::{Term, Variable, VariableAssignment};

use super::PrimitiveTerm;

/// Represents a constraint which is expressed as a binary operator applied to two terms
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Constraint {
    /// Two terms are equal.
    Equals(Term, Term),
    /// Two terms are unequal.
    Unequals(Term, Term),
    /// Value of the left term is less than the value of the right term.
    LessThan(Term, Term),
    /// Value of the left term is greater than the value of the right term.
    GreaterThan(Term, Term),
    /// Value of the left term is less than or equal to the value of the right term.
    LessThanEq(Term, Term),
    /// Value of the left term is greater than or equal to the value of the right term.
    GreaterThanEq(Term, Term),
}

impl Constraint {
    /// Return the left and right term used in the constraint.
    pub fn terms(&self) -> (&Term, &Term) {
        match &self {
            Constraint::Equals(left, right)
            | Constraint::Unequals(left, right)
            | Constraint::LessThan(left, right)
            | Constraint::GreaterThan(left, right)
            | Constraint::LessThanEq(left, right)
            | Constraint::GreaterThanEq(left, right) => (left, right),
        }
    }

    /// Return a mutable reference to the left and right term used in the constraint.
    pub fn terms_mut(&mut self) -> (&mut Term, &mut Term) {
        match self {
            Constraint::Equals(left, right)
            | Constraint::Unequals(left, right)
            | Constraint::LessThan(left, right)
            | Constraint::GreaterThan(left, right)
            | Constraint::LessThanEq(left, right)
            | Constraint::GreaterThanEq(left, right) => (left, right),
        }
    }

    /// Return the left term used in the constraint.
    pub fn left(&self) -> &Term {
        self.terms().0
    }

    /// Return the right term used in the constraint.
    pub fn right(&self) -> &Term {
        self.terms().1
    }

    /// Return All the variables used within this constraint.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        self.left().variables().chain(self.right().variables())
    }

    /// Return whether this type of constraints only works on numeric values
    pub fn is_numeric(&self) -> bool {
        !(matches!(self, Constraint::Equals(_, _)) || matches!(self, Constraint::Unequals(_, _)))
    }

    /// Replaces [`Variable`]s with [`Term`]s according to the provided assignment.
    pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
        let (left, right) = self.terms_mut();

        left.apply_assignment(assignment);
        right.apply_assignment(assignment);
    }

    /// Return whether the constraint could be interpreted as an assignment,
    /// i.e. has the form `?Variable = Term`.
    ///
    /// If so returns a tuple containing the variable and the term.
    /// Returns `None` otherwise.
    pub fn has_form_assignment(&self) -> Option<(&Variable, &Term)> {
        if let Constraint::Equals(Term::Primitive(PrimitiveTerm::Variable(variable)), term) = self {
            Some((variable, term))
        } else if let Constraint::Equals(term, Term::Primitive(PrimitiveTerm::Variable(variable))) =
            self
        {
            Some((variable, term))
        } else {
            None
        }
    }
}

impl Constraint {
    /// Returns a string representation for the operation defined by this constraint.
    fn operator_string(&self) -> &'static str {
        match self {
            Constraint::Equals(_, _) => "=",
            Constraint::Unequals(_, _) => "!=",
            Constraint::LessThan(_, _) => "<",
            Constraint::GreaterThan(_, _) => ">",
            Constraint::LessThanEq(_, _) => "<=",
            Constraint::GreaterThanEq(_, _) => ">=",
        }
    }
}

impl std::fmt::Display for Constraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (left, right) = self.terms();

        f.write_fmt(format_args!(
            "{} {} {}",
            left,
            self.operator_string(),
            right
        ))
    }
}
