//! This module defines a function from translating
//! operations in the logical model to operations in the chase model.

use crate::{
    chase_model::components::{
        operation::ChaseOperation,
        term::operation_term::{Operation, OperationTerm},
    },
    rule_model::components::term::{Term, primitive::variable::Variable},
};

use super::ProgramChaseTranslation;

impl ProgramChaseTranslation {
    /// Create a [OperationTerm] from a given [Term].
    ///
    /// # Panics
    /// Panics if term is not primitive or an operation.
    fn operation_term(term: &Term) -> OperationTerm {
        match term {
            Term::Primitive(primitive) => OperationTerm::Primitive(primitive.clone()),
            Term::Operation(operation) => Self::build_operation_term(operation),
            _ => unreachable!(
                "invalid program: operation term does not only consist of operation terms"
            ),
        }
    }

    /// Create an [OperationTerm] from a given
    /// [Operation][crate::rule_model::components::term::operation::Operation].
    ///
    /// # Panics
    /// Panics if the operation is not "pure", i.e. if it contains as subterms
    /// terms that are not operations or primitive terms.
    pub(crate) fn build_operation_term(
        operation: &crate::rule_model::components::term::operation::Operation,
    ) -> OperationTerm {
        let kind = operation.operation_kind();
        let subterms = operation.terms().map(Self::operation_term).collect();

        OperationTerm::Operation(Operation::new(kind, subterms))
    }

    /// Create a [ChaseOperation] form a given [Term].
    ///
    /// # Panics
    /// Panics if the operation is not "pure", i.e. if it contains as subterms
    /// terms that are not operations or primitive terms.
    pub(crate) fn build_operation(output_variable: &Variable, term: &Term) -> ChaseOperation {
        let operation = Self::operation_term(term);

        ChaseOperation::new(output_variable.clone(), operation)
    }
}
