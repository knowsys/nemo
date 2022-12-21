//! Module which collects functions for logging events.

use crate::{
    logical::{
        model::{DataSource, Program},
        program_analysis::analysis::NormalRuleAnalysis,
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

/// Log: Print all available variable orders.
pub fn log_avaiable_variable_order(program: &Program, analysis: &NormalRuleAnalysis) {
    log::info!("Available orders:");
    for promising_order in &analysis.promising_variable_orders {
        log::info!("   * {}", promising_order.debug(program.get_dict_names()));
    }
}

/// Log: Loading a table from disk.
pub fn log_load_table(source: &DataSource) {
    match source {
        DataSource::CsvFile(file) => {
            log::info!("Added CSV file as source: {:?}", file);
        }
        DataSource::RdfFile(file) => {
            log::info!("Added RDF file as source: {:?}", file);
        }
        DataSource::SparqlQuery(_query) => {
            // TODO: This is not very insightful...
            log::info!("Added source: SparqlQuery");
        }
    }
}

/// Log: Executing plan.
pub fn log_executing_plan<TableKey: TableKeyType>(tree: &ExecutionTree<TableKey>) {
    log::info!("Executing plan {}:", tree.name());
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

/// Log: Empty trie iterator.
pub fn log_empty_iter() {
    log::info!("Plan is empty");
}
