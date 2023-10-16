use crate::model::{Term, Variable};

/// Indicates that a new value must be creater accodring to [`Term`].
/// The result will be "stored" in the given variable
#[derive(Debug, Clone)]
pub struct Constructor {
    variable: Variable,
    term: Term,
}

impl Constructor {
    /// Create a new [`Constructor`].
    ///
    /// # Panics
    /// Pancis if the provided term is an aggregate (We handle those in a separate construct).
    pub fn new(variable: Variable, term: Term) -> Self {
        if let Term::Aggregation(_) = term {
            panic!("An aggregate is not a constructor");
        }

        Self { variable, term }
    }

    /// Return the variable which associated with the result of this constructor.
    pub fn variable(&self) -> &Variable {
        &self.variable
    }

    /// Return the term which computes the result of this constructor.
    pub fn term(&self) -> &Term {
        &self.term
    }
}
