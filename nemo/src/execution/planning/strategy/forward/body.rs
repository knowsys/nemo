//! This module defines [StrategyBody].

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning::{
        RuntimeInformation,
        analysis::variable_order::VariableOrder,
        normalization::{
            atom::{body::BodyAtom, import::ImportAtom},
            operation::Operation,
        },
        operations::{
            function_filter_negation::GeneratorFunctionFilterNegation, import::GeneratorImport,
            join_seminaive::GeneratorJoinSeminaive,
        },
    },
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
    table_manager::SubtableExecutionPlan,
};

/// Generator of an execution plan that evaluates the body of a rule
#[derive(Debug)]
pub struct StrategyBody {
    // Seminaive join
    seminaive_join: GeneratorJoinSeminaive,
    // Filter
    seminaive_filter: Option<GeneratorFunctionFilterNegation>,

    /// Import
    import: Option<GeneratorImport>,
    /// Import Filter
    import_filter: Option<GeneratorFunctionFilterNegation>,

    /// Variables
    variables: Vec<Variable>,
}

impl StrategyBody {
    /// Create a new [StrategyBody].
    pub fn new(
        order: VariableOrder,
        positive: Vec<BodyAtom>,
        mut negative: Vec<BodyAtom>,
        imports: Vec<ImportAtom>,
        operations: &mut Vec<Operation>,
    ) -> Self {
        let seminaive_join = GeneratorJoinSeminaive::new(positive, &order);
        let mut current_variables = seminaive_join.output_variables();

        let seminaive_filter =
            GeneratorFunctionFilterNegation::new(current_variables, operations, &mut negative);
        current_variables = seminaive_filter.output_variables();

        let import = GeneratorImport::new(current_variables, imports, &order);
        current_variables = import.output_variables();

        let import_filter =
            GeneratorFunctionFilterNegation::new(current_variables, operations, &mut negative);

        Self {
            seminaive_join,
            seminaive_filter: seminaive_filter.or_none(),
            import: import.or_none(),
            import_filter: import_filter.or_none(),
            variables: order.as_ordered_list(),
        }
    }

    /// Return an iterator over all special predicates needed to execute this strategy.
    pub fn special_predicates(&self) -> impl Iterator<Item = (Tag, usize)> {
        self.import
            .as_ref()
            .map(|import| import.special_predicates())
            .into_iter()
            .flatten()
    }

    /// Append this operation to the plan.
    pub async fn create_plan<'a>(
        &self,
        plan: &mut SubtableExecutionPlan,
        runtime: &RuntimeInformation<'a>,
    ) -> ExecutionNodeRef {
        let mut current_node = self.seminaive_join.create_plan(plan, runtime);

        if let Some(generator) = self.seminaive_filter.as_ref() {
            current_node = generator.create_plan(plan, current_node, runtime);
        }

        if let Some(generator) = self.import.as_ref() {
            current_node = generator.create_plan(plan, current_node, runtime).await;
        }

        if let Some(generator) = self.import_filter.as_ref() {
            current_node = generator.create_plan(plan, current_node, runtime);
        }

        current_node
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        self.variables.clone()
    }
}
