use super::{Identifier, Variable};

/// Aggregate occurring in a predicate in the head
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct Aggregate {
    pub(crate) aggregate_identifier: Identifier,
    pub(crate) variable_identifiers: Vec<Identifier>,
}

impl Aggregate {
    /// Substitutes all occurrences of `var` for `subst`.
    pub fn substitute_variable(&mut self, var: &Variable, subst: &Variable) {
        let Variable::Universal(var) = var else {
            panic!("cannot substitute existential variable")
        };

        let Variable::Universal(subst) = subst else {
            panic!("cannot substitute with existential variable")
        };

        for id in &mut self.variable_identifiers {
            if id == var {
                *id = subst.clone();
            }
        }
    }
}

impl std::fmt::Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "#{}({:?})",
            &self.aggregate_identifier, self.variable_identifiers
        )
    }
}
