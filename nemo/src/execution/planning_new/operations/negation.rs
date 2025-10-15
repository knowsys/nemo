//! This module defines [v].

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning_new::{
        RuntimeInformation,
        normalization::{atom::body::BodyAtom, operation::Operation},
        operations::{
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
    ///
    /// Only uses the negative atoms that contain at least
    /// one positive variable and removes those from `atoms`.
    pub fn new(
        positive_variables: &[Variable],
        atoms: &mut Vec<BodyAtom>,
        operations: &mut Vec<Operation>,
    ) -> Self {
        let mut negated_atoms = Vec::<(BodyAtom, GeneratorFilter)>::default();
        let mut keep = vec![true; atoms.len()];

        for (atom, keep_atom) in atoms.iter().zip(keep.iter_mut()) {
            let mut variables = positive_variables.to_vec();

            if variables
                .iter()
                .all(|variable| !positive_variables.contains(variable))
            {
                *keep_atom = false;
                continue;
            }

            variables.extend(atom.terms().cloned());

            let filter = GeneratorFilter::new(variables, operations);
            negated_atoms.push((atom.clone(), filter));
        }

        let mut keep_iter = keep.iter();
        atoms.retain(|_| *keep_iter.next().expect("keep is as long as atoms"));

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

    /// Returns `Some(self)` or `None`
    /// depending on whether this is a noop,
    /// i.e. does not affect the result.
    pub fn or_none(self) -> Option<Self> {
        if !self.atoms.is_empty() {
            Some(self)
        } else {
            None
        }
    }
}
