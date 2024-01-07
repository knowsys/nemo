use crate::model::{Identifier, Variable};

/// Prefix used for generated aggregate variables in a [`ChaseRule`]
pub(super) const AGGREGATE_VARIABLE_PREFIX: &str = "_AGGREGATE_";
/// Prefix used for generated variables encoding equality constraints in a [`ChaseRule`]
pub(super) const EQUALITY_VARIABLE_PREFIX: &str = "_EQUALITY_";
/// Prefix used for generated variables for storing the value of complex terms in a [`ChaseRule`].
pub(super) const CONSTRUCT_VARIABLE_PREFIX: &str = "_CONSTRUCT_";

fn is_aggregate_identifier(identifier: &Identifier) -> bool {
    identifier.name().starts_with(AGGREGATE_VARIABLE_PREFIX)
}

/// Check if a variable is a aggregate placeholder variable, representing the output of an aggregate.
pub(crate) fn is_aggregate_variable(variable: &Variable) -> bool {
    match variable {
        Variable::Universal(identifier) => is_aggregate_identifier(identifier),
        Variable::Existential(identifier) => {
            debug_assert!(
                !is_aggregate_identifier(identifier),
                "aggregate variables must be universal variables"
            );
            false
        }
    }
}

fn is_construct_identifier(identifier: &Identifier) -> bool {
    identifier.name().starts_with(CONSTRUCT_VARIABLE_PREFIX)
}

/// Check if a variable is a constructor variable
pub(crate) fn is_construct_variable(variable: &Variable) -> bool {
    match variable {
        Variable::Universal(identifier) => is_construct_identifier(identifier),
        Variable::Existential(identifier) => {
            debug_assert!(
                !is_construct_identifier(identifier),
                "construct variables must be universal variables"
            );
            false
        }
    }
}
