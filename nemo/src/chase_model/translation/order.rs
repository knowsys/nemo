//! This module contains functions for creating [ChaseFilter]s

use crate::{
    chase_model::components::{
        filter::ChaseFilter,
        order::ChaseOrder,
        term::operation_term::{Operation, OperationTerm},
        ChaseComponent,
    },
    rule_model::components::{
        term::operation::operation_kind::OperationKind, IterableVariables, ProgramComponent,
    },
};

use super::ProgramChaseTranslation;

impl ProgramChaseTranslation {
    /// Create a new [ChaseOrder]
    /// from a given [Operation][crate::rule_model::components::order::Order].
    pub(crate) fn build_order(
        &mut self,
        order: &crate::rule_model::components::order::Order,
    ) -> ChaseOrder {
        let origin = *order.origin();

        let (atom_dominating, dominating_filters) = self.build_body_atom(order.dominating());
        let (atom_dominated, dominated_filters) = self.build_body_atom(order.dominated());

        let variables_dominating = atom_dominating.variables().cloned().collect::<Vec<_>>();
        let variables_dominated = atom_dominated.variables().cloned().collect::<Vec<_>>();

        let mut conditions = order
            .condition()
            .iter()
            .map(|operation| Self::build_operation_term(operation))
            .collect::<Vec<_>>();
        conditions.extend(
            dominated_filters
                .into_iter()
                .map(|filter| filter.filter_owned()),
        );
        conditions.extend(
            dominating_filters
                .into_iter()
                .map(|filter| filter.filter_owned()),
        );

        let condition = OperationTerm::Operation(Operation::new(
            OperationKind::BooleanConjunction,
            conditions,
        ));

        let filter = ChaseFilter::new(condition);

        ChaseOrder::new(variables_dominating, variables_dominated, filter).set_origin(origin)
    }
}
