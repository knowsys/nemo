//! This module defines [GeneratorUnion].

use std::ops::Range;

use nemo_physical::{
    management::execution_plan::ExecutionNodeRef, tabular::operations::OperationTable,
};

use crate::{
    execution::planning::{RuntimeInformation, normalization::atom::body::BodyAtom},
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
    table_manager::SubtableExecutionPlan,
};

/// Represents a range of subtables
#[derive(Debug, Clone)]
pub enum UnionRange {
    /// Explicit range of steps
    _Steps(Range<usize>),
    /// All subtables before the specified step
    _Before(usize),
    /// All available subtables
    All,
    /// Only new subtables (relative to last rule application)
    New,
    /// Only old subtables (relative to old rule application)
    Old,
    /// Ony new subtables (relative to last rule application)
    /// excluding the step the rule was applied in
    NewExclusive,
    /// Only old subtables (relative to last rule application)
    /// including the step the rule was applied in
    OldInclusive,
}

impl UnionRange {
    /// Convert this [UnionRange] into a [Range].
    pub fn as_range(&self, step_current: usize, step_last_application: usize) -> Range<usize> {
        match self {
            UnionRange::_Steps(range) => range.clone(),
            UnionRange::_Before(step) => 0..*step,
            UnionRange::All => 0..step_current,
            UnionRange::New => step_last_application..step_current,
            UnionRange::Old => 0..step_last_application,
            UnionRange::NewExclusive => (step_last_application + 1)..step_current,
            UnionRange::OldInclusive => 0..(step_last_application + 1),
        }
    }
}

/// Generator of union nodes,
/// representing tables associated with a predicate
/// and a range of subtables.
#[derive(Debug)]
pub struct GeneratorUnion {
    /// Predicate
    predicate: Tag,
    /// Variables that mark the table columns
    variables: Vec<Variable>,
    /// Step range of the subtables
    range: UnionRange,
}

impl GeneratorUnion {
    /// Create a new [GeneratorUnion].
    pub fn new(predicate: Tag, variables: Vec<Variable>, range: UnionRange) -> Self {
        Self {
            predicate,
            variables,
            range,
        }
    }

    /// Create a new [GeneratorUnion] that produces unmarked execution nodes.
    pub fn new_unmarked(predicate: Tag, range: UnionRange) -> Self {
        Self {
            predicate,
            variables: Vec::default(),
            range,
        }
    }

    /// Create a new [Union] form a [BodyAtom].
    pub fn new_atom(atom: &BodyAtom, range: UnionRange) -> Self {
        Self::new(atom.predicate(), atom.terms().cloned().collect(), range)
    }

    /// Append this operation to the plan
    /// including the nodes in `start`.
    fn create_plan_with_vector(
        &self,
        plan: &mut SubtableExecutionPlan,
        runtime: &RuntimeInformation,
        start: Vec<ExecutionNodeRef>,
    ) -> ExecutionNodeRef {
        let range = self
            .range
            .as_range(runtime.step_current, runtime.step_last_application);
        let mut subtables = start;
        subtables.extend(
            runtime
                .table_manager
                .tables_in_range(&self.predicate, &range)
                .into_iter()
                .map(|id| plan.plan_mut().fetch_table(OperationTable::default(), id)),
        );

        let markers = runtime.translation.operation_table(self.variables.iter());

        plan.plan_mut().union(markers, subtables)
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        self.create_plan_with_vector(plan, runtime, Vec::default())
    }

    /// Append this operation to the plan,
    /// including the given `input_node` in the union.
    pub fn create_plan_with(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        self.create_plan_with_vector(plan, runtime, vec![input_node])
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        self.variables.clone()
    }

    /// Return the predicate over which this union is performed.
    pub fn predicate(&self) -> &Tag {
        &self.predicate
    }
}
