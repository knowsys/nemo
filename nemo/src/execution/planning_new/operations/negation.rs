//! This module defines [v].

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning_new::{
        normalization::{atom::body::BodyAtom, operation::Operation},
        operations::{
            RuntimeInformation,
            filter::GeneratorFilter,
            union::{GeneratorUnion, UnionRange},
        },
    },
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator of negation nodes in execution plans
#[derive(Debug)]
pub struct GeneratorNegation {
    /// Safe variables
    variables: Vec<Variable>,

    /// Negated atoms together with additional filters
    atoms: Vec<(BodyAtom, GeneratorFilter)>,
}

impl GeneratorNegation {
    /// Create a new [GeneratorNegation].
    pub fn new(
        positive_variables: &[Variable],
        atoms: Vec<BodyAtom>,
        operations: &mut Vec<Operation>,
    ) -> Self {
        let mut negated_atoms = Vec::<(BodyAtom, GeneratorFilter)>::default();

        for atom in atoms {
            let mut variables = positive_variables.to_vec();
            variables.extend(atom.terms().cloned());

            let filter = GeneratorFilter::new(variables, operations);
            negated_atoms.push((atom, filter));
        }

        Self {
            variables: positive_variables.to_vec(),
            atoms: negated_atoms,
        }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let mut nodes_subtracted = Vec::<ExecutionNodeRef>::default();

        for (atom, filter) in &self.atoms {
            let node_union =
                GeneratorUnion::new_atom(atom, UnionRange::All).create_plan(plan, runtime);
            let node_filter = filter.create_plan(plan, node_union, runtime);

            nodes_subtracted.push(node_filter);
        }

        plan.plan_mut().subtract(input_node, nodes_subtracted)
    }
}
