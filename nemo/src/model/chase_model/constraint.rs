use crate::model::{Condition, Term, Variable};

/// Represents a constraint on some values.
///
/// Uses the same internal representation as [`Condition`],
/// however it disallows the Assignment operator
#[derive(Debug, Clone)]
pub struct Constraint(Condition);

impl Constraint {
    /// Create a new [`Constraint`].
    ///
    /// # Panics
    /// Panics if the provided condition is an assignment.
    pub fn new(condition: Condition) -> Self {
        if let Condition::Assignment(_, _) = condition {
            unreachable!("An assignment is not a constraint.");
        }

        Self(condition)
    }

    /// Return the underlying [`Condition`].
    pub fn condition(&self) -> &Condition {
        &self.0
    }

    /// Return the left and right term used in the constraint.
    pub fn terms(&self) -> (&Term, &Term) {
        match &self.0 {
            Condition::Assignment(_, _) => unreachable!("Not a valid constraint"),
            Condition::Equals(left, right) => (left, right),
            Condition::Unequals(left, right) => (left, right),
            Condition::LessThan(left, right) => (left, right),
            Condition::GreaterThan(left, right) => (left, right),
            Condition::LessThanEq(left, right) => (left, right),
            Condition::GreaterThanEq(left, right) => (left, right),
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
        self.0.is_numeric()
    }
}
