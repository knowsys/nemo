//! This module defines [ChaseAggregate].

use std::{collections::HashSet, fmt::Display};

use crate::{
    execution::planning::normalization::operation::Operation,
    rule_model::components::term::{aggregate::AggregateKind, primitive::variable::Variable},
    syntax,
    util::seperated_list::DisplaySeperatedList,
};

/// Aggegration over a set of values,
/// defined by input, output, distinct and group-by variables.
#[derive(Debug, Clone)]
pub struct Aggregation {
    /// Type of aggregate operation
    /// (e.g. sum, count, min, max)
    kind: AggregateKind,

    /// Variable containing the value over which the aggregate is computed,
    /// e.g. the values that are summed in a `sum` aggregate
    input_variable: Variable,
    /// Variable containing the result of the aggregation,
    /// e.g. the resulting sum in a `sum` aggregate
    output_variable: Variable,

    /// Variables used to determine distinctness of input values.
    /// If non-empty, the aggregate considers only unique combinations
    /// of these variables when computing the result
    distinct_variables: Vec<Variable>,
    /// Variables used to partition the input into groups,
    /// which are aggregated separately
    group_by_variables: Vec<Variable>,
}

impl Display for Aggregation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let distinct = if !self.distinct_variables.is_empty() {
            format!(
                ", {}",
                DisplaySeperatedList::display(
                    self.distinct_variables.iter(),
                    &format!("{} ", syntax::SEQUENCE_SEPARATOR)
                ),
            )
        } else {
            String::default()
        };

        f.write_fmt(format_args!(
            "{} = {}({}{})",
            self.output_variable, self.kind, self.input_variable, distinct
        ))
    }
}

impl Aggregation {
    /// Create a new [Aggregation].
    pub fn new(
        kind: AggregateKind,
        input_variable: Variable,
        output_variable: Variable,
        distinct_variables: Vec<Variable>,
        group_by_variables: Vec<Variable>,
    ) -> Self {
        Self {
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

    /// Return an iterator over all variables contained in this aggregate.
    pub fn variables(&self) -> Box<dyn Iterator<Item = &Variable> + '_> {
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
}

impl Aggregation {
    /// Receives a [crate::rule_model::components::term::aggregate::Aggregate]
    /// and normalizes it into an [Aggregation].
    ///
    /// Also returns an additional [Operation] that needs to be performed
    /// to compute the input for the aggregation.
    ///
    /// # Panics
    /// Panics if the program is ill-formed
    /// and hence this aggregation contains structured terms or is recursive
    pub fn normalize_aggregation(
        group_by: HashSet<Variable>,
        aggregate: &crate::rule_model::components::term::aggregate::Aggregate,
    ) -> (Self, Option<Operation>) {
        let kind = aggregate.aggregate_kind();

        let (input_variable, operation) = match aggregate.aggregate_term() {
            crate::rule_model::components::term::Term::Primitive(
                crate::rule_model::components::term::primitive::Primitive::Variable(variable),
            ) => (variable.clone(), None),
            crate::rule_model::components::term::Term::Primitive(
                crate::rule_model::components::term::primitive::Primitive::Ground(ground),
            ) => {
                let new_variable = Variable::universal("_AGGREGATION_IN");
                let new_operation = Operation::new_assignment(
                    new_variable.clone(),
                    Operation::new_ground(ground.clone()),
                );

                (new_variable, Some(new_operation))
            }
            crate::rule_model::components::term::Term::Operation(operation) => {
                let new_variable = Variable::universal("_AGGREGATION_IN");
                let new_operation = Operation::new_assignment(
                    new_variable.clone(),
                    Operation::normalize_body_operation(operation),
                );

                (new_variable, Some(new_operation))
            }
            _ => panic!(
                "invalid program: aggregation term is not primitive or contains additional aggregates"
            ),
        };

        let output_variable = Variable::universal("_AGGREGATION_OUT");
        let distinct_variables = aggregate.distinct().cloned().collect::<Vec<_>>();
        let group_by_variables = group_by.into_iter().collect::<Vec<_>>();

        let aggregation = Self {
            kind,
            input_variable,
            output_variable,
            distinct_variables,
            group_by_variables,
        };

        (aggregation, operation)
    }
}
