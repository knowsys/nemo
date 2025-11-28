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
            join_cartesian::GeneratorJoinCartesian, join_imports::GeneratorJoinImports,
            join_imports_general::GeneratorJoinImportsGeneral,
            join_imports_simple::GeneratorJoinImportsSimple,
            join_seminaive::GeneratorJoinSeminaive,
        },
    },
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
    table_manager::SubtableExecutionPlan,
};

/// Generator of an execution plan that evaluates the body of a rule
#[derive(Debug)]
pub enum StrategyBody {
    /// Plain Datalog without imports
    Plain {
        /// Seminaive join
        join: Box<GeneratorJoinSeminaive>,
        /// Additional filters
        filter: Option<GeneratorFunctionFilterNegation>,
    },
    Import {
        /// Cartesian seminaive join
        join: Box<GeneratorJoinCartesian>,
        /// Incremental import
        import: Box<GeneratorImport>,
        /// Join of the import atoms
        merge: Box<GeneratorJoinImports>,
    },
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
        if imports.is_empty() {
            let join = Box::new(GeneratorJoinSeminaive::new(positive, &order));
            let filter = GeneratorFunctionFilterNegation::new(
                join.output_variables(),
                operations,
                &mut negative,
            )
            .or_none();

            Self::Plain { join, filter }
        } else {
            let join = GeneratorJoinCartesian::new(&order, &positive, operations, &mut negative);
            let variables_join = join.output_variables();

            let import = GeneratorImport::new(variables_join, &imports);

            let merge = if join.is_single_join() {
                GeneratorJoinImports::Simple(GeneratorJoinImportsSimple::new(
                    &order,
                    join.output_variables().first().cloned().unwrap_or_default(),
                    imports,
                    operations,
                    &mut negative,
                ))
            } else {
                GeneratorJoinImports::General(GeneratorJoinImportsGeneral::new(
                    &order,
                    positive,
                    imports,
                    operations,
                    &mut negative,
                ))
            };

            Self::Import {
                join: Box::new(join),
                import: Box::new(import),
                merge: Box::new(merge),
            }
        }
    }

    /// Return an iterator over all special predicates needed to execute this strategy.
    pub fn special_predicates(&self) -> Box<dyn Iterator<Item = (Tag, usize)> + '_> {
        match self {
            StrategyBody::Plain { join: _, filter: _ } => Box::new(std::iter::empty()),
            StrategyBody::Import {
                join: _,
                import,
                merge: _,
            } => Box::new(import.special_predicates()),
        }
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        match self {
            StrategyBody::Plain { join, filter } => match filter {
                Some(filter) => filter.output_variables(),
                None => join.output_variables(),
            },
            StrategyBody::Import {
                join: _,
                import: _,
                merge,
            } => merge.output_variables(),
        }
    }

    /// Append this operation to the plan.
    pub async fn create_plan<'a>(
        &self,
        plan: &mut SubtableExecutionPlan,
        runtime: &RuntimeInformation<'a>,
    ) -> ExecutionNodeRef {
        let node_body = match self {
            StrategyBody::Plain { join, filter } => {
                let mut node = join.create_plan(plan, runtime);

                if let Some(filter) = filter {
                    node = filter.create_plan(plan, node, runtime);
                }

                node
            }
            StrategyBody::Import {
                join,
                import,
                merge,
            } => {
                let nodes_join = join.create_plan(plan, runtime);

                let merge_input = if join.is_single_join() {
                    Some(nodes_join[0].clone())
                } else {
                    None
                };

                let new_imports = import.create_plan(plan, nodes_join, runtime).await;

                merge.create_plan(plan, merge_input, new_imports, runtime)
            }
        };

        plan.add_temporary_table(node_body.clone(), "Body");

        node_body
    }
}
