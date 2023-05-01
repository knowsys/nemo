//! Functionality for creating an execution plan that implements negation.

use std::collections::HashMap;

use crate::{
    logical::{
        execution::planning::plan_util::{compute_filters, subplan_union_reordered},
        model::{Atom, Filter, Term, Variable},
        program_analysis::variable_order::VariableOrder,
        table_manager::SubtableExecutionPlan,
        types::LogicalTypeEnum,
        TableManager,
    },
    physical::{
        management::{database::ColumnOrder, execution_plan::ExecutionNodeRef},
        tabular::operations::{triescan_minus::SubtractInfo, triescan_project::ProjectReordering},
        util::mapping::traits::NatMapping,
    },
};

struct AtomInfo {
    restricted_variable_order: VariableOrder,
    reorder: ColumnOrder,
    projection: ProjectReordering,
    subtraction: SubtractInfo,
}

impl AtomInfo {
    fn new(positive_order: &VariableOrder, atom: &Atom) -> AtomInfo {
        let mut restricted_variable_order =
            positive_order.restrict_to(&atom.variables().cloned().collect());

        let old_variables = restricted_variable_order.len();
        let mut new_variables: usize = 0;
        for term in atom.terms() {
            if let Term::Variable(variable) = term {
                if !restricted_variable_order.contains(variable) {
                    restricted_variable_order.push(variable.clone());
                    new_variables += 1;
                }
            }
        }

        let atom_variables_reordered = restricted_variable_order.as_ordered_list();
        let atom_variables: Vec<Variable> = atom
            .terms()
            .iter()
            .map(|t| {
                if let Term::Variable(v) = t {
                    v.clone()
                } else {
                    unreachable!("Atom should have been normalized.")
                }
            })
            .collect();

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

        AtomInfo {
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
    pub variable_types: HashMap<Variable, LogicalTypeEnum>,
    /// The negated atoms.
    pub atoms: Vec<Atom>,
    /// The negated filters.
    pub filters: Vec<Filter>,
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
                let info = AtomInfo::new(variable_order_main, a);
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
