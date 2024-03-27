//! This module contains helper functions
//! for obtaining the union of all the subtables of a predicate.

use std::ops::Range;

use nemo_physical::{
    management::execution_plan::{ColumnOrder, ExecutionNodeRef, ExecutionPlan},
    tabular::operations::OperationTable,
};

use crate::{model::Identifier, table_manager::TableManager};

/// Given a predicate and a range of execution steps,
/// adds to the given [ExecutionPlan]
/// a node representing the union of subtables within that range.
///
/// Note that only the output of the union will receive column markers.
pub(crate) fn subplan_union(
    plan: &mut ExecutionPlan,
    table_manager: &TableManager,
    predicate: &Identifier,
    steps: Range<usize>,
    output_markers: OperationTable,
) -> ExecutionNodeRef {
    let subtables = table_manager
        .tables_in_range(predicate, &steps)
        .into_iter()
        .map(|id| plan.fetch_table(OperationTable::default(), id))
        .collect();

    plan.union(output_markers, subtables)
}

/// Given a predicate and a range of execution steps,
/// adds to the given [ExecutionPlan]
/// a node representing the union of subtables within that range
/// that should be reordered according to the provided [ColumnOrder].
///
/// Note that only the output of the union will receive column markers.
pub(super) fn _subplan_union_reordered(
    plan: &mut ExecutionPlan,
    table_manager: &TableManager,
    predicate: &Identifier,
    steps: Range<usize>,
    output_markers: OperationTable,
    column_order: ColumnOrder,
) -> ExecutionNodeRef {
    let subtables = table_manager
        .tables_in_range(predicate, &steps)
        .into_iter()
        .map(|id| plan.fetch_table_reordered(OperationTable::default(), id, column_order.clone()))
        .collect();

    plan.union(output_markers, subtables)
}
