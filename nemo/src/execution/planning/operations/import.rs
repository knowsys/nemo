//! This module contains helper functions for implementing incremental imports

use nemo_physical::{
    management::execution_plan::{ColumnOrder, ExecutionNodeRef},
    tabular::operations::OperationTable,
};

use crate::{
    chase_model::components::{
        atom::variable_atom::VariableAtom, filter::ChaseFilter, import::ChaseImportClause,
        operation::ChaseOperation,
    },
    execution::{
        planning::operations::{
            filter::node_filter, functions::node_functions, negation::node_negation,
            union::subplan_union,
        },
        rule_execution::VariableTranslation,
    },
    io::ImportManager,
    rule_model::components::tag::Tag,
    table_manager::{SubtableExecutionPlan, SubtableIdentifier, TableManager},
};

/// Compute an exeuction plan for on-demand importing of tables.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn node_imports(
    subtable_plan: &mut SubtableExecutionPlan,
    table_manager: &TableManager,
    import_manager: &ImportManager,
    variable_translation: &VariableTranslation,
    current_step_number: usize,
    input_node: ExecutionNodeRef,
    imports: &Vec<ChaseImportClause>,
    import_operations: &[ChaseOperation],
    import_filters: &[ChaseFilter],
    import_subtracted_atoms: &[VariableAtom],
    import_subtracted_filters: &[Vec<ChaseFilter>],
) -> ExecutionNodeRef {
    let mut imported_tables = Vec::<ExecutionNodeRef>::default();
    let mut old_imported_tables = Vec::<ExecutionNodeRef>::default();

    let markers_input = input_node.markers_cloned();

    // Perform the import operations
    for import in imports {
        let markers_import = variable_translation.operation_table(import.bindings().iter());

        // First we project the content of the input node to the required columns

        let markers_projection = binding_table_markers(&markers_input, &markers_import);
        let node_import_bindings = subtable_plan
            .plan_mut()
            .projectreorder(markers_projection.clone(), input_node.clone());

        // To only request new information, we subtract the old bindings

        let (binding_predicate, _arity) =
            binding_table_predicate_name(import.predicate(), &markers_input, &markers_import);
        let old_bindings = subplan_union(
            subtable_plan.plan_mut(),
            table_manager,
            &binding_predicate,
            0..current_step_number,
            markers_projection.clone(),
        );
        let node_new_bindings = subtable_plan
            .plan_mut()
            .subtract(node_import_bindings, vec![old_bindings]);

        // Now we can add the import

        let provider = import_manager
            .table_provider_from_handler(import.handler())
            .await
            .expect("invalid import");

        let node_import = subtable_plan.plan_mut().import(
            markers_import.clone(),
            node_new_bindings.clone(),
            provider,
        );
        imported_tables.push(node_import.clone());

        let import_table_name = table_manager.generate_table_name(
            import.predicate(),
            &ColumnOrder::default(),
            current_step_number,
        );

        // We also need to compute the union of all old imported tables, in case something matches with this

        let node_old_imports = subplan_union(
            subtable_plan.plan_mut(),
            table_manager,
            import.predicate(),
            0..current_step_number,
            markers_import,
        );

        old_imported_tables.push(node_old_imports);

        // We save the imported table and the input bindings

        subtable_plan.add_permanent_table(
            node_import,
            "Rule Import",
            &import_table_name,
            SubtableIdentifier::new(import.predicate().clone(), current_step_number - 1),
        );

        subtable_plan.add_permanent_table(
            node_new_bindings,
            "Import Bindings",
            "Import Bindings",
            SubtableIdentifier::new(binding_predicate, current_step_number),
        );
    }

    if imported_tables.is_empty() {
        return input_node;
    }

    // Join imported tables
    let join_markers = join_markers(&markers_input, &imported_tables);
    let mut node_join = subtable_plan.plan_mut().join_empty(join_markers.clone());

    node_join.add_subnode(input_node.clone());
    for node_import in imported_tables {
        node_join.add_subnode(node_import);
    }

    let mut node_join_old = subtable_plan.plan_mut().join_empty(join_markers.clone());
    node_join_old.add_subnode(input_node);
    for node_import in old_imported_tables {
        node_join_old.add_subnode(node_import);
    }

    // Union of matches for old and new tables
    let node_union = subtable_plan
        .plan_mut()
        .union(join_markers, vec![node_join, node_join_old]);

    // Finally, apply operations, filters, and negation

    let node_import_functions = node_functions(
        subtable_plan.plan_mut(),
        variable_translation,
        node_union,
        import_operations,
    );

    let node_import_filter = node_filter(
        subtable_plan.plan_mut(),
        variable_translation,
        node_import_functions,
        import_filters,
    );

    node_negation(
        subtable_plan.plan_mut(),
        table_manager,
        variable_translation,
        node_import_filter,
        current_step_number,
        import_subtracted_atoms,
        import_subtracted_filters,
    )
}

/// Compute the column markers for the binding table.
fn binding_table_markers(
    input_markers: &OperationTable,
    import_markers: &OperationTable,
) -> OperationTable {
    let mut result = OperationTable::default();

    for &marker in import_markers.iter() {
        if input_markers.contains(&marker) {
            result.push(marker);
        }
    }

    result
}

/// Compute the predicate name and arity for the binding table.
pub(crate) fn binding_table_predicate_name<T>(
    predicate: &Tag,
    input_markers: &[T],
    import_markers: &[T],
) -> (Tag, usize)
where
    T: Eq + std::fmt::Debug,
{
    let mut name = format!("__IMPORT_{}_", predicate.name());
    let mut arity = 0;

    for marker in import_markers {
        if input_markers.contains(marker) {
            name.push('b');
            arity += 1;
        } else {
            name.push('f');
        }
    }

    (Tag::new(name), arity)
}

/// Extend the initial column markers with markers used in imports.
fn join_markers(input_markers: &OperationTable, imports: &Vec<ExecutionNodeRef>) -> OperationTable {
    let mut result = input_markers.clone();

    for import in imports {
        let import_markers = import.markers_cloned();

        for marker in import_markers {
            if !input_markers.contains(&marker) {
                result.push(marker);
            }
        }
    }

    result
}
