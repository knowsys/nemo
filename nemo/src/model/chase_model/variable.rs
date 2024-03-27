use crate::model::Variable;

/// Prefix used for generated aggregate variables in a [super::ChaseRule]
pub(super) const AGGREGATE_VARIABLE_PREFIX: &str = "_AGGREGATE_";
/// Prefix used for generated variables encoding equality constraints in a [super::ChaseRule]
pub(super) const EQUALITY_VARIABLE_PREFIX: &str = "_EQUALITY_";
/// Prefix used for generated variables for storing the value of complex terms in a [super::ChaseRule].
pub(super) const CONSTRUCT_VARIABLE_PREFIX: &str = "_CONSTRUCT_";

fn is_aggregate_identifier(var_name: &str) -> bool {
    var_name.starts_with(AGGREGATE_VARIABLE_PREFIX)
}

/// Check if a variable is a aggregate placeholder variable, representing the output of an aggregate.
pub(crate) fn is_aggregate_variable(variable: &Variable) -> bool {
    match variable {
        Variable::Universal(var_name) => is_aggregate_identifier(var_name),
        Variable::Existential(var_name) => {
            debug_assert!(
                !is_aggregate_identifier(var_name),
                "aggregate variables must be universal variables"
            );
            false
        }
        Variable::UnnamedUniversal(_) => false,
    }
}
