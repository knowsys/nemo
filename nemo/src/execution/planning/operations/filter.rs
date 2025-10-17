//! This module defines [GeneratorFilter].

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning::{RuntimeInformation, normalization::operation::Operation},
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator of filter nodes in execution plans
#[derive(Debug)]
pub struct GeneratorFilter {
    /// Variables
    _variables: Vec<Variable>,

    /// Filters
    filters: Vec<Operation>,
}

impl GeneratorFilter {
    /// Create a new [GeneratorFilter].
    ///
    /// This operation is applied to [ExecutionBinding]
    /// and applies all `operations` that only use bound variables,
    /// and then removes those operations from the list.
    pub fn new(input_variables: Vec<Variable>, operations: &mut Vec<Operation>) -> Self {
        let mut filters = Vec::<Operation>::default();

        operations.retain(|filter| {
            if filter
                .variables()
                .all(|variable| input_variables.contains(variable))
            {
                filters.push(filter.clone());
                false
            } else {
                true
            }
        });

        Self {
            _variables: input_variables,
            filters,
        }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let filters = self
            .filters
            .iter()
            .map(|filter| filter.function_tree(&runtime.translation))
            .collect::<Vec<_>>();

        plan.plan_mut().filter(input_node, filters)
    }

    /// Return an iterator over all filters that will be applied.
    pub fn filters(&self) -> impl Iterator<Item = &Operation> {
        self.filters.iter()
    }

    /// Returns `Some(self)` or `None` depending on whether this is a noop,
    /// i.e. does not affect the result.
    pub fn or_none(self) -> Option<Self> {
        if !self.filters.is_empty() {
            Some(self)
        } else {
            None
        }
    }
}
