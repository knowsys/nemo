//! This module contains functions for creating [ChaseAggregate]s.

use std::collections::HashSet;

use crate::{
    chase_model::components::{
        aggregate::ChaseAggregate,
        operation::ChaseOperation,
        rule::ChaseRule,
        term::operation_term::{Operation, OperationTerm},
    },
    rule_model::components::{
        term::{
            primitive::{variable::Variable, Primitive},
            Term,
        },
        ProgramComponent,
    },
};

use super::ProgramChaseTranslation;

impl ProgramChaseTranslation {
    /// Create a [ChaseAggregate] from a given
    /// [Aggregate][crate::rule_model::components::term::aggregate::Aggregate].
    ///
    /// # Panics
    /// Panics if aggregation term contains a structured term or another aggregation.
    pub(crate) fn build_aggregate(
        &mut self,
        result: &mut ChaseRule,
        aggregate: &crate::rule_model::components::term::aggregate::Aggregate,
        group_by_variables: &HashSet<Variable>,
    ) -> ChaseAggregate {
        let origin = aggregate.origin().clone();
        let kind = aggregate.kind();
        let input_variable = match aggregate.aggregate_term() {
            Term::Primitive(Primitive::Variable(variable)) => variable.clone(),
            Term::Primitive(primitive) => {
                let new_variable = Variable::universal(&self.create_fresh_variable());
                result.add_positive_operation(ChaseOperation::new(
                    origin,
                    new_variable.clone(),
                    OperationTerm::Primitive(primitive.clone()),
                ));

                new_variable
            }
            Term::Operation(operation) => {
                let new_variable = Variable::universal(&self.create_fresh_variable());
                result.add_positive_operation(ChaseOperation::new(
                    origin,
                    new_variable.clone(),
                    Self::build_operation_term(operation),
                ));

                new_variable
            }
            Term::Aggregate(_) => unreachable!("invalid program: Recursive aggregates not allowed"),
            _ => unreachable!("invalid program: complex terms not allowed"),
        };
        let output_variable = Variable::universal(&self.create_fresh_variable());
        let distinct_variables = aggregate.distinct().cloned().collect();

        ChaseAggregate::new(
            origin,
            kind,
            input_variable,
            output_variable,
            distinct_variables,
            group_by_variables.clone(),
        )
    }

    /// Create an [OperationTerm] from a given
    /// [Operation][crate::rule_model::components::term::operation::Operation].
    ///
    /// If this function encounters an aggregate it will use its `output_variable` instead.
    /// In this case the given `chase_aggregate` parameter will be set.
    ///
    /// # Panics
    /// Panics if the operation is not "pure", i.e. if it contains as subterms
    /// terms that are not operations or primitive terms.
    fn build_operation_term_with_aggregate(
        &mut self,
        result: &mut ChaseRule,
        operation: &crate::rule_model::components::term::operation::Operation,
        group_by_variables: &HashSet<Variable>,
        chase_aggregate: &mut Option<ChaseAggregate>,
    ) -> OperationTerm {
        let origin = operation.origin().clone();
        let kind = operation.kind();
        let mut subterms = Vec::new();

        for argument in operation.arguments() {
            match argument {
                Term::Primitive(primitive) => {
                    subterms.push(OperationTerm::Primitive(primitive.clone()))
                }
                Term::Operation(operation) => subterms.push(Self::build_operation_term(operation)),
                Term::Aggregate(aggregate) => {
                    let new_aggregate = self.build_aggregate(result, aggregate, group_by_variables);

                    subterms.push(OperationTerm::Primitive(Primitive::Variable(
                        new_aggregate.output_variable().clone(),
                    )));

                    *chase_aggregate = Some(new_aggregate);
                }
                _ => unreachable!(
                    "invalid program: operation term does not only consist of operation terms"
                ),
            }
        }

        OperationTerm::Operation(Operation::new(origin, kind, subterms))
    }

    /// Create a [ChaseOperation] from a given
    /// [Operation][crate::rule_model::components::term::operation::Operation].
    /// that may potentially contain an aggregate.
    /// If this is the case, then `chase_aggregate` will be set to `Some`.
    ///
    /// # Panics
    /// Panics if operation contains complex terms or multiple aggregates.
    pub(crate) fn build_operation_with_aggregate(
        &mut self,
        result: &mut ChaseRule,
        operation: &crate::rule_model::components::term::operation::Operation,
        group_by_variables: &HashSet<Variable>,
        output_variable: Variable,
        chase_aggregate: &mut Option<ChaseAggregate>,
    ) -> ChaseOperation {
        let operation_term = self.build_operation_term_with_aggregate(
            result,
            operation,
            group_by_variables,
            chase_aggregate,
        );
        ChaseOperation::new(operation.origin().clone(), output_variable, operation_term)
    }
}
