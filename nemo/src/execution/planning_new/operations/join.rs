//! This module defines [GeneratorJoin].

use std::collections::HashSet;

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    chase_model::analysis::variable_order::VariableOrder,
    execution::planning_new::{
        normalization::atom::body::BodyAtom,
        operations::{
            RuntimeInformation,
            union::{GeneratorUnion, UnionRange},
        },
    },
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator of join nodes in execution plans
#[derive(Debug)]
pub struct GeneratorJoin {
    /// Body atoms that will be joined
    atoms: Vec<BodyAtom>,
    /// Step ranges of the respective subtables
    ranges: Vec<UnionRange>,

    /// Variable order
    order: VariableOrder,
}

impl GeneratorJoin {
    /// Create a new [Join].
    pub fn new(atoms: Vec<BodyAtom>, ranges: Vec<UnionRange>, order: &VariableOrder) -> Self {
        debug_assert!(atoms.len() == ranges.len());

        let variables = atoms
            .iter()
            .flat_map(|atom| atom.terms())
            .cloned()
            .collect::<HashSet<_>>();
        let order = order.restrict_to(&variables);

        Self {
            atoms,
            ranges,
            order,
        }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let subtables = self
            .atoms
            .iter()
            .zip(self.ranges.iter())
            .map(|(atom, range)| {
                GeneratorUnion::new_atom(atom, range.clone()).create_plan(plan, runtime)
            })
            .collect::<Vec<_>>();

        let markers = runtime.translation.operation_table(self.order.iter());

        plan.plan_mut().join(markers, subtables)
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        self.order.as_ordered_list()
    }
}
