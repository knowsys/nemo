//! This module contains functions for creating [ChaseAggregate]s.

use crate::{
    chase_model::components::{
        aggregate::ChaseAggregate,
        operation::ChaseOperation,
        rule::ChaseRule,
        term::operation_term::{Operation, OperationTerm},
        ChaseComponent,
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
        group_by_variables: &[Variable],
        output_variable: Variable,
    ) -> ChaseAggregate {
        let origin = *aggregate.origin();
        let kind = aggregate.aggregate_kind();
        let input_variable = match aggregate.aggregate_term() {
            Term::Primitive(Primitive::Variable(variable)) => variable.clone(),
            Term::Primitive(primitive) => {
                let new_variable = self.create_fresh_variable();
                result.add_positive_operation(
                    ChaseOperation::new(
                        new_variable.clone(),
                        OperationTerm::Primitive(primitive.clone()),
                    )
                    .set_origin(origin),
                );

                new_variable
            }
            Term::Operation(operation) => {
                let new_variable = self.create_fresh_variable();
                result.add_positive_operation(
                    ChaseOperation::new(
                        new_variable.clone(),
                        Self::build_operation_term(operation),
                    )
                    .set_origin(origin),
                );

                new_variable
            }
            Term::Aggregate(_) => unreachable!("invalid program: Recursive aggregates not allowed"),
            _ => unreachable!("invalid program: complex terms not allowed"),
        };
        let distinct_variables = aggregate.distinct().cloned().collect();

        ChaseAggregate::new(
            origin,
            kind,
            input_variable,
            output_variable,
            distinct_variables,
            group_by_variables.to_vec(),
        )
    }

    /// Create an [OperationTerm] from a given
    /// [Operation][crate::rule_model::components::term::operation::Operation].
    ///
    /// If this function encounters an aggregate it will
    /// use the provided variable instead.
    ///
    /// # Panics
    /// Panics if the operation is not "pure", i.e. if it contains as subterms
    /// terms that are not operations or primitive terms.
    fn build_operation_term_with_aggregate<'a>(
        operation: &'a crate::rule_model::components::term::operation::Operation,
        aggregation_variable: &Variable,
    ) -> (
        OperationTerm,
        Option<&'a crate::rule_model::components::term::aggregate::Aggregate>,
    ) {
        let origin = *operation.origin();
        let kind = operation.operation_kind();
        let mut subterms = Vec::new();

        let mut aggregation_result = None;

        for argument in operation.arguments() {
            match argument {
                Term::Primitive(primitive) => {
                    subterms.push(OperationTerm::Primitive(primitive.clone()))
                }
                Term::Operation(operation) => {
                    let (term, result) =
                        Self::build_operation_term_with_aggregate(operation, aggregation_variable);
                    if aggregation_result.is_none() {
                        aggregation_result = result;
                    }

                    subterms.push(term);
                }
                Term::Aggregate(aggregate) => {
                    subterms.push(OperationTerm::Primitive(Primitive::Variable(
                        aggregation_variable.clone(),
                    )));

                    aggregation_result = Some(aggregate);
                }
                _ => unreachable!(
                    "invalid program: operation term does not only consist of operation terms"
                ),
            }
        }

        (
            OperationTerm::Operation(Operation::new(kind, subterms).set_origin(origin)),
            aggregation_result,
        )
    }

    /// Create a [ChaseOperation] from a given
    /// [Operation][crate::rule_model::components::term::operation::Operation].
    /// that may potentially contain an aggregate.
    /// If this is the case, then `chase_aggregate` will be set to `Some`.
    ///
    /// # Panics
    /// Panics if operation contains complex terms or multiple aggregates.
    pub(crate) fn build_operation_with_aggregate<'a>(
        &mut self,
        operation: &'a crate::rule_model::components::term::operation::Operation,
        aggregation_variable: Variable,
        output_variable: Variable,
    ) -> (
        ChaseOperation,
        Option<&'a crate::rule_model::components::term::aggregate::Aggregate>,
    ) {
        let (operation_term, aggregate) =
            Self::build_operation_term_with_aggregate(operation, &aggregation_variable);

        (
            ChaseOperation::new(output_variable, operation_term).set_origin(*operation.origin()),
            aggregate,
        )
    }
}
