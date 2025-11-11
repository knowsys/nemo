//! Mapping from [FunctionTree] to SPARQL functions.

use nemo_physical::{function::tree::FunctionTree, tabular::operations::OperationColumnMarker};
use spargebra::{algebra::Expression, term::Variable};

/// Try to create a SPARQL FILTER expression from the given function.
pub(crate) fn try_expression_from_function(
    _variables: &[Variable],
    function: &FunctionTree<OperationColumnMarker>,
) -> Option<Expression> {
    // TODO(mam): actually check which functions we can support here
    None
}
