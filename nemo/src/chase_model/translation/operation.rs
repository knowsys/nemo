//! This module defines a function from translating
//! operations in the logical model to operations in the chase model.

use crate::{
    chase_model::components::{
        operation::ChaseOperation,
        term::operation_term::{Operation, OperationTerm},
        ChaseComponent,
    },
    rule_model::components::{
        term::{primitive::variable::Variable, Term},
        ProgramComponent,
    },
};

use super::ProgramChaseTranslation;

impl ProgramChaseTranslation {
    /// Create an [OperationTerm] from a given
    /// [Operation][crate::rule_model::components::term::operation::Operation].
    ///
    /// # Panics
    /// Panics if the operation is not "pure", i.e. if it contains as subterms
    /// terms that are not operations or primitive terms.
    pub(crate) fn build_operation_term(
        operation: &crate::rule_model::components::term::operation::Operation,
    ) -> OperationTerm {
        let origin = operation.origin().clone();
        let kind = operation.operation_kind();
        let mut subterms = Vec::new();

        for argument in operation.arguments() {
            match argument {
                Term::Primitive(primitive) => {
                    subterms.push(OperationTerm::Primitive(primitive.clone()))
                }
                Term::Operation(operation) => subterms.push(Self::build_operation_term(operation)),
                _ => unreachable!(
                    "invalid program: operation term does not only consist of operation terms"
                ),
            }
        }

        OperationTerm::Operation(Operation::new(kind, subterms).set_origin(origin))
    }

    /// Create a [ChaseOperation] form a given
    /// [Operation][crate::rule_model::components::term::operation::Operation].
    ///
    /// # Panics
    /// Panics if the operation is not "pure", i.e. if it contains as subterms
    /// terms that are not operations or primitive terms.
    pub(crate) fn build_operation(
        output_variable: &Variable,
        operation: &crate::rule_model::components::term::operation::Operation,
    ) -> ChaseOperation {
        let origin = operation.origin().clone();
        let operation = Self::build_operation_term(operation);

        ChaseOperation::new(output_variable.clone(), operation).set_origin(origin)
    }
}
