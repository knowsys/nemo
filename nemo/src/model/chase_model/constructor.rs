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
    pub fn new(variable: Variable, term: Term) -> Self {
        Self  {
            variable,
            term
        }
    }
}