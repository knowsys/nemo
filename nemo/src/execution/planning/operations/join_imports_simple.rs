//! This module defines [GeneratorJoinImportsSimple].

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
            filter::GeneratorFilter,
            function_filter_negation::GeneratorFunctionFilterNegation,
            union::{GeneratorUnion, UnionRange},
        },
    },
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
    table_manager::SubtableExecutionPlan,
};

/// Generator for execution plan nodes
/// that join incrementally imported tables,
/// in the simple case where the positive
/// body consists of only a single cartesian factor
#[derive(Debug)]
pub struct GeneratorJoinImportsSimple {
    /// Variable order
    join_order: VariableOrder,

    /// Imports
    imports: Vec<GeneratorUnion>,
    /// Negated imports
    negative_imports: Vec<GeneratorUnion>,
    /// Filter for negatives imports
    negative_filters: Vec<Option<GeneratorFilter>>,
    /// Filters
    filters: Option<GeneratorFunctionFilterNegation>,
}

impl GeneratorJoinImportsSimple {
    /// Create a new [GeneratorJoinImportsSimple].
    pub fn new(
        order: &VariableOrder,
        postive_variables: Vec<Variable>,
        import_atoms: Vec<ImportAtom>,
        negative_import_atoms: Vec<ImportAtom>,
        operations: &mut Vec<Operation>,
        atoms_negation: &mut Vec<BodyAtom>,
    ) -> Self {
        let variables = postive_variables
            .into_iter()
            .chain(
                import_atoms
                    .iter()
                    .flat_map(|atom| atom.variables().cloned()),
            )
            .collect::<HashSet<_>>();

        let join_order = order.restrict_to(&variables.into_iter().collect::<HashSet<_>>());

        let imports = import_atoms
            .into_iter()
            .map(|atom| {
                GeneratorUnion::new(atom.predicate(), atom.variables_cloned(), UnionRange::All)
            })
            .collect::<Vec<_>>();

        let mut negative_imports = Vec::default();
        let mut negative_filters = Vec::default();

        for atom in negative_import_atoms.into_iter() {
            let union =
                GeneratorUnion::new(atom.predicate(), atom.variables_cloned(), UnionRange::All);

            let filter = GeneratorFilter::new(atom.variables_cloned(), operations);

            negative_imports.push(union);
            negative_filters.push(filter.or_none());
        }

        let filters = GeneratorFunctionFilterNegation::new(
            join_order.as_ordered_list(),
            operations,
            atoms_negation,
        )
        .or_none();

        Self {
            join_order,
            imports,
            negative_imports,
            negative_filters,
            filters,
        }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        node_positive: ExecutionNodeRef,
        new_imports: HashMap<Tag, ExecutionNodeRef>,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let mut join_tables = vec![node_positive];

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

        if !self.negative_imports.is_empty() {
            let mut subtracted = Vec::default();

            for (import, filter) in self
                .negative_imports
                .iter()
                .zip(self.negative_filters.iter())
            {
                let mut node_imports = import.create_plan(plan, runtime);
                let import_markers = runtime
                    .translation
                    .operation_table(import.output_variables().iter());

                if let Some(mut node_new_import) = new_imports.get(import.predicate()).cloned() {
                    node_new_import.set_markers(import_markers);
                    node_imports.add_subnode(node_new_import);
                }

                if let Some(filter) = filter {
                    node_imports = filter.create_plan(plan, node_imports, runtime);
                }

                subtracted.push(node_imports);
            }

            node_result = plan.plan_mut().subtract(node_result, subtracted);
        }

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
            None => self.join_order.as_ordered_list(),
        }
    }
}
