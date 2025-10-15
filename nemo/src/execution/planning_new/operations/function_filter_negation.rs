//! This module defines [GeneratorFunctionFilterNegation].

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning_new::{
        RuntimeInformation,
        normalization::{atom::body::BodyAtom, operation::Operation},
        operations::{
            filter::GeneratorFilter, function::GeneratorFunction, negation::GeneratorNegation,
        },
    },
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator of execution plan nodes
/// combining [GeneratorFunction], [GeneratorFilter],
/// and [GeneratorNegation]
#[derive(Debug)]
pub struct GeneratorFunctionFilterNegation {
    /// Function
    function: Option<GeneratorFunction>,
    /// Filter
    filter: Option<GeneratorFilter>,
    /// Negation
    negation: Option<GeneratorNegation>,

    /// Variables
    variables: Vec<Variable>,
}

impl GeneratorFunctionFilterNegation {
    /// Create a new [GeneratorFunctionFilterNegation].
    pub fn new(
        input_variables: Vec<Variable>,
        operations: &mut Vec<Operation>,
        atoms_negation: &mut Vec<BodyAtom>,
    ) -> Self {
        let generator_function = GeneratorFunction::new(input_variables, operations);
        let current_variables = generator_function.output_variables();

        let generator_filter = GeneratorFilter::new(current_variables.clone(), operations);

        let generator_negation =
            GeneratorNegation::new(&current_variables, atoms_negation, operations);

        Self {
            function: generator_function.or_none(),
            filter: generator_filter.or_none(),
            negation: generator_negation.or_none(),
            variables: current_variables,
        }
    }

    /// Append this operation to the plan.
    pub fn create_plan<'a>(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation<'a>,
    ) -> ExecutionNodeRef {
        let mut current_node = input_node;

        if let Some(generator) = self.function.as_ref() {
            current_node = generator.create_plan(plan, current_node, runtime);
        }

        if let Some(generator) = self.filter.as_ref() {
            current_node = generator.create_plan(plan, current_node, runtime);
        }

        if let Some(generator) = self.negation.as_ref() {
            current_node = generator.create_plan(plan, current_node, runtime);
        }

        current_node
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        self.variables.clone()
    }

    /// Return an iterator over all filters that will be applied.
    pub fn filters(&self) -> impl Iterator<Item = &Operation> {
        self.filter
            .as_ref()
            .map(|filter| filter.filters())
            .into_iter()
            .flatten()
    }

    /// Return an iterator over all functions that will be applied.
    pub fn functions(&self) -> impl Iterator<Item = &(Variable, Operation)> {
        self.function
            .as_ref()
            .map(|function| function.functions())
            .into_iter()
            .flatten()
    }

    /// Return an iterator over all negation that will be applied.
    pub fn negations(&self) -> impl Iterator<Item = (BodyAtom, Vec<Operation>)> {
        self.negation
            .as_ref()
            .map(|negation| negation.atoms())
            .into_iter()
            .flatten()
    }

    /// Returns `Some(self)` or `None` depending on whether this is a noop,
    /// i.e. does not affect the result.
    pub fn or_none(self) -> Option<Self> {
        if !self.function.is_none() || !self.filter.is_none() || !self.negation.is_none() {
            Some(self)
        } else {
            None
        }
    }
}
