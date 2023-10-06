//! Functionality for creating an execution plan that implements negation.

use std::collections::HashMap;

use nemo_physical::{
    management::{database::ColumnOrder, execution_plan::ExecutionNodeRef},
    tabular::operations::{triescan_minus::SubtractInfo, triescan_project::ProjectReordering},
    util::mapping::traits::NatMapping,
};

use crate::{
    execution::planning::plan_util::{compute_filters, subplan_union_reordered},
    model::{chase_model::ChaseAtom, Condition, PrimitiveType, Variable},
    program_analysis::variable_order::VariableOrder,
    table_manager::{SubtableExecutionPlan, TableManager},
};

/// Bundels information relevant to constructing the negation sub plan of an execution plan
/// for one atom.
struct AtomNegationInfo {
    /// The variable order restricted to the variables that occur in this atom.
    restricted_variable_order: VariableOrder,
    /// Necessary reordering of the underlying table in order to comply with the above variable order.
    reorder: ColumnOrder,
    /// [`ProjectReordering`] that encodes how auxillary variables should be projected away when subtracting the contents of this atom.
    projection: ProjectReordering,
    /// How the contents of the atom will be subtracted from the main body join.
    subtraction: SubtractInfo,
}

impl AtomNegationInfo {
    fn new(positive_order: &VariableOrder, atom: &ChaseAtom) -> AtomNegationInfo {
        let atom_variables: Vec<Variable> = atom.variables().cloned().collect();

        let mut restricted_variable_order =
            positive_order.restrict_to(&atom.variables().cloned().collect());

        let old_variables = restricted_variable_order.len();
        let mut new_variables: usize = 0;
        for variable in &atom_variables {
            if !restricted_variable_order.contains(variable) {
                restricted_variable_order.push(variable.clone());
                new_variables += 1;
            }
        }

        let atom_variables_reordered = restricted_variable_order.as_ordered_list();

        let reorder = ColumnOrder::from_transformation(&atom_variables, &atom_variables_reordered);
        let projection = ProjectReordering::from_vector(
            (0..old_variables).collect(),
            old_variables + new_variables,
        );

        let mut used_variables = Vec::<usize>::new();
        for (index, variable) in positive_order.as_ordered_list().iter().enumerate() {
            if atom_variables_reordered.contains(variable) {
                used_variables.push(index);
            }
        }

        let subtraction = SubtractInfo::new(used_variables);

        AtomNegationInfo {
            restricted_variable_order,
            reorder,
            projection,
            subtraction,
        }
    }
}

/// Generator for creating excution plans which applies the negated [`Atom`]s and [`Filter`]s.
#[derive(Debug)]
pub(super) struct NegationGenerator {
    /// Logical types of all the variables.
    pub variable_types: HashMap<Variable, PrimitiveType>,
    /// The negated atoms.
    pub atoms: Vec<ChaseAtom>,
    /// The negated filters.
    pub filters: Vec<Condition>,
}

impl NegationGenerator {
    /// Generates the plan for negation.
    pub(super) fn generate_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        table_manager: &TableManager,
        node_main: ExecutionNodeRef,
        variable_order_main: &VariableOrder,
        current_step_number: usize,
    ) -> ExecutionNodeRef {
        let mut subtract_infos = Vec::<SubtractInfo>::new();
        let nodes_subtracted: Vec<ExecutionNodeRef> = self
            .atoms
            .iter()
            .map(|a| {
                let info = AtomNegationInfo::new(variable_order_main, a);
                subtract_infos.push(info.subtraction);

                let node_union = subplan_union_reordered(
                    plan.plan_mut(),
                    table_manager,
                    a.predicate(),
                    &(0..current_step_number),
                    info.reorder,
                );

                let (classes, assignments) = compute_filters(
                    &info.restricted_variable_order,
                    &self.filters,
                    &self.variable_types,
                );

                let node_filtered = plan.plan_mut().select_value(node_union, assignments);
                let node_filtered = plan.plan_mut().select_equal(node_filtered, classes);

                let node_result = if info.projection.is_identity() {
                    node_filtered
                } else {
                    plan.add_temporary_table(node_filtered.clone(), "Subtable Negation");

                    plan.plan_mut().project(node_filtered, info.projection)
                };

                node_result
            })
            .collect();

        plan.plan_mut()
            .subtract(node_main, nodes_subtracted, subtract_infos)
    }
}
