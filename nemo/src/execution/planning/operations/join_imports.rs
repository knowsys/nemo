//! This module defines [GeneratorJoinImports].

use std::collections::{HashMap, HashSet};

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
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
    table_manager::SubtableExecutionPlan,
};

/// Generator for execution plan nodes
/// that join incrementally imported tables.
#[derive(Debug)]
pub struct GeneratorJoinImports {
    /// Variable order
    join_order: VariableOrder,

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
            .collect::<HashSet<_>>();

        let join_order = order.restrict_to(&variables.into_iter().collect::<HashSet<_>>());

        let join = GeneratorJoinSeminaive::new(atoms, &join_order);
        let imports = import_atoms
            .into_iter()
            .map(|atom| {
                GeneratorUnion::new(atom.predicate(), atom.variables_cloned(), UnionRange::All)
            })
            .collect::<Vec<_>>();
        let filters = GeneratorFunctionFilterNegation::new(
            join_order.as_ordered_list(),
            operations,
            atoms_negation,
        )
        .or_none();

        Self {
            join_order,
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
        new_imports: HashMap<Tag, ExecutionNodeRef>,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let node_input = match input {
            Some(input) => input,
            None => self.join.create_plan(plan, runtime),
        };

        let mut join_tables = vec![node_input];
        for import in &self.imports {
            let mut node_imports = import.create_plan(plan, runtime);
            let import_markers = runtime
                .translation
                .operation_table(import.output_variables().iter());

            if let Some(mut node_new_import) = new_imports.get(import.predicate()).cloned() {
                node_new_import.set_markers(import_markers);
                node_imports.add_subnode(node_new_import);
            }

            join_tables.push(node_imports);
        }

        let markers = runtime
            .translation
            .operation_table(self.join_order.as_ordered_list().iter());

        let mut node_result = plan.plan_mut().join(markers, join_tables);

        if let Some(filters) = &self.filters {
            node_result = filters.create_plan(plan, node_result, runtime);
        }

        node_result
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        match &self.filters {
            Some(filters) => filters.output_variables(),
            None => self.join.output_variables(),
        }
    }
}
