use crate::model::Condition;

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
}
