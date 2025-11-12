//! This module defines [GeneratorJoinImports].

use std::collections::HashSet;

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
            function_filter_negation::GeneratorFunctionFilterNegation,
            join_seminaive::GeneratorJoinSeminaive,
            union::{GeneratorUnion, UnionRange},
        },
    },
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator for execution plan nodes
/// that join incrementally imported tables.
#[derive(Debug)]
pub struct GeneratorJoinImports {
    /// Variable order
    order: VariableOrder,

    /// Join
    join: GeneratorJoinSeminaive,
    /// Imports
    imports: Vec<GeneratorUnion>,
    /// Filters
    filters: Option<GeneratorFunctionFilterNegation>,
}

impl GeneratorJoinImports {
    /// Create a new [GeneratorJoinImports].
    pub fn new(
        order: &VariableOrder,
        atoms: Vec<BodyAtom>,
        import_atoms: Vec<ImportAtom>,
        operations: &mut Vec<Operation>,
        atoms_negation: &mut Vec<BodyAtom>,
    ) -> Self {
        let variables = atoms
            .iter()
            .flat_map(|atom| atom.terms().cloned())
            .chain(
                import_atoms
                    .iter()
                    .flat_map(|atom| atom.variables().cloned()),
            )
            .collect::<Vec<_>>();

        let join = GeneratorJoinSeminaive::new(atoms, order);
        let imports = import_atoms
            .into_iter()
            .map(|atom| {
                GeneratorUnion::new(atom.predicate(), atom.variables_cloned(), UnionRange::All)
            })
            .collect::<Vec<_>>();
        let filters =
            GeneratorFunctionFilterNegation::new(variables.clone(), operations, atoms_negation)
                .or_none();

        let variables = match &filters {
            Some(filters) => variables
                .into_iter()
                .chain(filters.output_variables().into_iter())
                .collect::<HashSet<_>>(),
            None => variables.into_iter().collect::<HashSet<_>>(),
        };

        let order = order.restrict_to(&variables);

        Self {
            order,
            join,
            imports,
            filters,
        }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        input: Option<ExecutionNodeRef>,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let node_input = match input {
            Some(input) => input,
            None => self.join.create_plan(plan, runtime),
        };

        let mut join_tables = vec![node_input];
        join_tables.extend(
            self.imports
                .iter()
                .map(|import| import.create_plan(plan, runtime)),
        );

        let markers = runtime
            .translation
            .operation_table(self.order.as_ordered_list().iter());

        let mut node_result = plan.plan_mut().join(markers, join_tables);

        if let Some(filters) = &self.filters {
            node_result = filters.create_plan(plan, node_result, runtime);
        }

        node_result
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        self.order.as_ordered_list()
    }
}
