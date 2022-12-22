//! Module which collects functions for logging events.

use num::ToPrimitive;

use crate::{
    logical::{
        model::{DataSource, Identifier, Program},
        program_analysis::analysis::NormalRuleAnalysis,
        table_manager::ColumnOrder,
    },
    physical::{
        management::{database::TableKeyType, execution_plan::ExecutionTree, ByteSized},
        tabular::{table_types::trie::Trie, traits::table::Table},
    },
};

/// Log: Rule application.
pub fn log_apply_rule(_program: &Program, step: usize, rule_index: usize) {
    // TOOO: Maybe add a to_string() method to rules so
    log::info!("<<< {step}: APPLYING RULE {rule_index} >>>");
}

/// Log: Choose variable order.
pub fn log_choose_variable_order(chosen_index: usize) {
    log::info!("Selected variable order: {chosen_index}");
}

/// Log: Combine tables to prevent fragmentation
pub fn log_fragmentation_combine(predicate: Identifier, trie_opt: Option<&Trie>) {
    if let Some(trie) = trie_opt {
        log::info!(
            "Combined multiple single-step tables for predicate {}: {} elements ({})",
            predicate.0,
            trie.row_num(),
            trie.size_bytes()
        );
    } else {
        log::warn!(
            "Combined multiple single-step tables for predicate {}: Empty table",
            predicate.0
        );
    }
}

/// Log: Print all available variable orders.
pub fn log_avaiable_variable_order(program: &Program, analysis: &NormalRuleAnalysis) {
    log::info!("Available orders:");
    for (index, promising_order) in analysis.promising_variable_orders.iter().enumerate() {
        log::info!(
            "   ({}) {}",
            index,
            promising_order.debug(program.get_dict_names())
        );
    }
}

/// Log: Loading a table from disk.
pub fn log_load_table(source: &DataSource) {
    match source {
        DataSource::CsvFile(file) => {
            log::info!("Loaded CSV file: {:?}", file);
        }
        DataSource::RdfFile(file) => {
            log::info!("Loaded RDF file: {:?}", file);
        }
        DataSource::SparqlQuery(_query) => {
            // TODO: This is not very insightful...
            log::info!("Loaded SparqlQuery");
        }
    }
}

/// Log: Add reference.
pub fn log_add_reference(from: Identifier, to: Identifier, reorder: &ColumnOrder) {
    log::info!("Add reference {} -> {} ({:?})", from.0, to.0, reorder.0);
}

/// Log: Executing plan title.
pub fn log_execution_title<TableKey: TableKeyType>(tree: &ExecutionTree<TableKey>) {
    log::info!("Executing plan \"{}\":", tree.name());
}

/// Log: Executing plan.
pub fn log_execution_tree(tree: &str) {
    log::info!("   -> {tree}");
}

/// Log: Materialize.
pub fn log_materialize(trie_elements: usize, next_count: usize) {
    log::info!(
        "Materialize: Next: {next_count}, Elements: {}, Quotient: {}",
        trie_elements,
        next_count.to_f64().unwrap() / trie_elements.to_f64().unwrap()
    );
}

/// Log: Save temporary table.
pub fn log_save_trie_temp(trie: &Trie) {
    log::info!(
        "Saved temporary table: {} entries ({})",
        trie.row_num(),
        trie.size_bytes().to_string()
    );
}

/// Log: Save table permanently.
pub fn log_save_trie_perm(trie: &Trie) {
    log::info!(
        "Saved permanent table: {} entries ({})",
        trie.row_num(),
        trie.size_bytes().to_string()
    );
}

/// Log: Trie is empty.
pub fn log_empty_trie() {
    log::info!("Trie does not contain any elements");
}
