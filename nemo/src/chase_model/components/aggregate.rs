//! This module defines [ChaseAggregate].

use crate::rule_model::{
    components::{
        term::{aggregate::AggregateKind, primitive::variable::Variable},
        IterableVariables,
    },
    origin::Origin,
};

use super::ChaseComponent;

/// Specifies how the values for a placeholder aggregate variable will get computed.
///
/// Terminology:
/// * `input_variables` are the distinct variables and the aggregated input variable, not including the group-by variables
/// * `output_variable` is the single aggregated output variable
///
/// See [nemo_physical::tabular::operations::TrieScanAggregate]
#[derive(Debug, Clone)]
pub(crate) struct ChaseAggregate {
    /// Origin of this component
    origin: Origin,

    /// Type of aggregate operation
    kind: AggregateKind,

    /// Variable that contains the value over which the aggregate is computed
    input_variable: Variable,
    /// Variable that will contain the result of this operation
    output_variable: Variable,

    /// Distinct variables
    distinct_variables: Vec<Variable>,
    /// Group-by variables
    group_by_variables: Vec<Variable>,
}

impl ChaseAggregate {
    /// Create a new [ChaseAggregate].
    pub fn new(
        origin: Origin,
        kind: AggregateKind,
        input_variable: Variable,
        output_variable: Variable,
        distinct_variables: Vec<Variable>,
        group_by_variables: Vec<Variable>,
    ) -> Self {
        Self {
            origin,
            kind,
            input_variable,
            output_variable,
            distinct_variables,
            group_by_variables,
        }
    }

    /// Return the aggregated input variable, which is the first of the input variables.
    pub fn input_variable(&self) -> &Variable {
        &self.input_variable
    }

    /// Return the output variable.
    pub fn output_variable(&self) -> &Variable {
        &self.output_variable
    }

    /// Return the distinct variables.
    pub fn distinct_variables(&self) -> &Vec<Variable> {
        &self.distinct_variables
    }

    /// Return the group by variable.
    pub fn group_by_variables(&self) -> &Vec<Variable> {
        &self.group_by_variables
    }

    /// Return which operation is performed.
    pub fn aggregate_kind(&self) -> AggregateKind {
        self.kind
    }
}

impl ChaseComponent for ChaseAggregate {
    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }
}

impl IterableVariables for ChaseAggregate {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        let input_variables = Some(&self.input_variable).into_iter();
        let output_variables = Some(&self.output_variable).into_iter();
        let distinct_variables = self.distinct_variables.iter();
        let group_by_variables = self.group_by_variables.iter();

        Box::new(
            input_variables
                .chain(output_variables)
                .chain(distinct_variables)
                .chain(group_by_variables),
        )
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        let input_variables = Some(&mut self.input_variable).into_iter();
        let output_variables = Some(&mut self.output_variable).into_iter();
        let distinct_variables = self.distinct_variables.iter_mut();
        let group_by_variables = self.group_by_variables.iter_mut();

        Box::new(
            input_variables
                .chain(output_variables)
                .chain(distinct_variables)
                .chain(group_by_variables),
        )
    }
}
