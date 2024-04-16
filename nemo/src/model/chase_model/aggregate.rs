use std::collections::HashSet;

use nemo_physical::aggregates::operation::AggregateOperation;

use crate::model::{Aggregate, LogicalAggregateOperation, PrimitiveTerm, Term, Variable};

/// Specifies how the values for a placeholder aggregate variable will get computed.
///
/// Terminology:
/// * `input_variables` are the distinct variables and the aggregated input variable, not including the group-by variables
/// * `output_variable` is the single aggregated output variable
///
/// See [nemo_physical::tabular::operations::TrieScanAggregate]
#[derive(Debug, Clone)]
pub struct ChaseAggregate {
    pub(crate) aggregate_operation: AggregateOperation,

    pub(crate) input_variable: Variable,
    pub(crate) distinct_variables: Vec<Variable>,

    pub(crate) group_by_variables: HashSet<Variable>,

    pub(crate) output_variable: Variable,
}

impl ChaseAggregate {
    /// Convert an [Aggregate] to a [ChaseAggregate], given a placeholder name for the output variable
    pub fn from_aggregate(
        aggregate: Aggregate,
        output_variable: Variable,
        group_by_variables: HashSet<Variable>,
    ) -> ChaseAggregate {
        let logical_aggregate_operation = aggregate.logical_aggregate_operation;

        let physical_operation = match logical_aggregate_operation {
            LogicalAggregateOperation::CountValues => AggregateOperation::Count,
            LogicalAggregateOperation::MaxNumber => AggregateOperation::Max,
            LogicalAggregateOperation::MinNumber => AggregateOperation::Min,
            LogicalAggregateOperation::SumOfNumbers => AggregateOperation::Sum,
        };

        let mut variables = aggregate
            .terms
            .into_iter()
            .map(|t| {
                if let Term::Primitive(PrimitiveTerm::Variable(variable)) = t {
                    variable
                } else {
                    unreachable!("Non-variable terms are not allowed in chase aggregates.");
                }
            })
            .collect::<Vec<Variable>>();

        let input_variable = variables.remove(0);
        let distinct_variables = variables;

        Self {
            aggregate_operation: physical_operation,
            output_variable,
            input_variable,
            distinct_variables,
            group_by_variables,
        }
    }

    /// Return the aggregated input variable, which is the first of the input variables
    pub fn aggregated_input_variable(&self) -> &Variable {
        &self.input_variable
    }
}
