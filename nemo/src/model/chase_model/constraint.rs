use crate::model::{Condition, Term, Variable, VariableAssignment};

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
    /// Create a new [`Constraint`].
    ///
    /// # Panics
    /// Panics if the provided condition is not a constraint.
    pub fn from_condition(condition: Condition) -> Option<Self> {
        if let Condition::Constraint(constraint) = condition {
            Some(constraint)
        } else {
            None
        }
    }

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

    /// Return whether this type of condition only works on numeric values
    pub fn is_numeric(&self) -> bool {
        !(matches!(self, Constraint::Equals(_, _)) || matches!(self, Constraint::Unequals(_, _)))
    }

    /// Replaces [`Variable`]s with [`Term`]s according to the provided assignment.
    pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
        let (left, right) = self.terms_mut();

        left.apply_assignment(assignment);
        right.apply_assignment(assignment);
    }
}

impl Constraint {
    /// Returns a string representation for the operation defined by this condition.
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
