//! This module contains helper functions for implementing incremental imports

use nemo_physical::{
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
    tabular::operations::OperationTable,
};

use crate::{
    chase_model::{analysis::variable_order::VariableOrder, components::import::ChaseImportClause},
    execution::{planning::operations::union::subplan_union, rule_execution::VariableTranslation},
    io::ImportManager,
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
    table_manager::TableManager,
};

/// Compute an exeuction plan for on-demand importing of tables.
pub(crate) fn node_imports(
    plan: &mut ExecutionPlan,
    table_manager: &TableManager,
    import_manager: &ImportManager,
    variable_translation: &VariableTranslation,
    current_step_number: usize,
    variable_order: &mut VariableOrder,
    input_node: ExecutionNodeRef,
    imports: &Vec<ChaseImportClause>,
) -> ExecutionNodeRef {
    // plan.import(marked_columns, subnode, provider);
    let mut imported_tables = Vec::<ExecutionNodeRef>::default();

    // Perform the import operations
    for import in imports {
        // First we project the content of the input node to the required columns
        let markers_projection =
            binding_table_markers(variable_translation, variable_order, import.bindings());
        let node_import_bindings =
            plan.projectreorder(markers_projection.clone(), input_node.clone());

        // To only request new information, we subtract the old bindings
        let binding_predicate = binding_table_predicate_name(variable_order, import);
        let old_bindings = subplan_union(
            plan,
            table_manager,
            &binding_predicate,
            0..current_step_number,
            markers_projection.clone(),
        );
        let node_new_bindings = plan.subtract(node_import_bindings, vec![old_bindings]);

        // Now we can add the import
        let markers_import = variable_translation.operation_table(import.bindings().iter());
        let provider = import_manager
            .table_provider_from_handler(import.handler())
            .expect("invalid import");

        let node_import = plan.import(markers_import, node_new_bindings.clone(), provider);
        imported_tables.push(node_import);

        // We also save the input binding table
        plan.write_permanent(
            node_new_bindings,
            "Import Bindings",
            binding_predicate.name(),
        );
    }

    // Extend variable order
    extend_variable_order(variable_order, imports);

    // Join imported tables
    let join_markers = variable_translation.operation_table(variable_order.iter());
    let mut node_join = plan.join_empty(join_markers);

    node_join.add_subnode(input_node);
    for node_import in imported_tables {
        node_join.add_subnode(node_import);
    }

    node_join
}

/// Compute the column markers for the binding table.
fn binding_table_markers(
    variable_translation: &VariableTranslation,
    variable_order: &VariableOrder,
    bindings: &Vec<Variable>,
) -> OperationTable {
    let mut input_variables = Vec::default();
    for variable in bindings {
        if variable_order.contains(variable) {
            input_variables.push(variable.clone());
        }
    }

    variable_translation.operation_table(input_variables.iter())
}

/// Compute the predicate name for the binding table.
fn binding_table_predicate_name(variable_order: &VariableOrder, import: &ChaseImportClause) -> Tag {
    let mut name = format!("__IMPORT_{}_", import.predicate().name());

    for variable in import.bindings() {
        if variable_order.contains(variable) {
            name.push('b');
        } else {
            name.push('f');
        }
    }

    Tag::new(name)
}

/// Extend the initial variable order with variables used in imports.
fn extend_variable_order(variable_order: &mut VariableOrder, imports: &Vec<ChaseImportClause>) {
    for import in imports {
        for variable in import.bindings() {
            if !variable_order.contains(variable) {
                variable_order.push(variable.clone());
            }
        }
    }
}
