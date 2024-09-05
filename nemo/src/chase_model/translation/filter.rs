//! This module contains functions for creating [ChaseFilter]s

use crate::{
    chase_model::components::{
        filter::ChaseFilter,
        term::operation_term::{Operation, OperationTerm},
        ChaseComponent,
    },
    rule_model::components::{
        term::{
            operation::operation_kind::OperationKind,
            primitive::{variable::Variable, Primitive},
        },
        ProgramComponent,
    },
};

use super::ProgramChaseTranslation;

impl ProgramChaseTranslation {
    /// Create a new filter that only allows the variable to take on the values
    /// of the result of the given [Operation][crate::rule_model::components::term::operation::Operation].
    pub(crate) fn build_filter_operation(
        variable: &Variable,
        operation: &crate::rule_model::components::term::operation::Operation,
    ) -> ChaseFilter {
        let origin = operation.origin().clone();
        let operation = Self::build_operation_term(operation);

        let filter = OperationTerm::Operation(
            Operation::new(
                OperationKind::Equal,
                vec![
                    OperationTerm::Primitive(Primitive::from(variable.clone())),
                    operation,
                ],
            )
            .set_origin(origin.clone()),
        );

        ChaseFilter::new(filter).set_origin(origin)
    }

    /// Create a new filter that binds the values of the variable to the provided primitive term.
    pub(crate) fn build_filter_primitive(variable: &Variable, term: &Primitive) -> ChaseFilter {
        let filter = Operation::new(
            OperationKind::Equal,
            vec![
                OperationTerm::Primitive(Primitive::from(variable.clone())),
                OperationTerm::Primitive(term.clone()),
            ],
        )
        .set_origin(term.origin().clone());

        ChaseFilter::new(OperationTerm::Operation(filter)).set_origin(term.origin().clone())
    }
}
