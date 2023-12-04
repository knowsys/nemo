use crate::model::{chase_model::CONSTRUCT_VARIABLE_PREFIX, Identifier, Variable};

use super::AGGREGATE_VARIABLE_PREFIX;

fn is_aggregate_identifier(identifier: &Identifier) -> bool {
    identifier.name().starts_with(AGGREGATE_VARIABLE_PREFIX)
}

/// Check if a variable is a aggregate placeholder variable, representing the output of an aggregate.
///
/// See [`AGGREGATE_VARIABLE_PREFIX`]
pub fn is_aggregate_variable(variable: &Variable) -> bool {
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
///
/// See [`CONSTRUCT_VARIABLE_PREFIX`]
pub fn is_construct_variable(variable: &Variable) -> bool {
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
