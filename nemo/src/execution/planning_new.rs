//! This module contains functions and data structures related to
//! converting rules into execution plans executed by nemo-physical

use crate::{
    chase_model::analysis::variable_order::VariableOrder,
    execution::rule_execution::VariableTranslation, io::ImportManager, table_manager::TableManager,
};

pub(crate) mod normalization;
pub(crate) mod operations;
pub(crate) mod strategy;

/// Collects runtime information for generating an execution plan
pub(crate) struct RuntimeInformation<'a> {
    /// Last step in which the current rule was applied
    step_last_application: usize,
    /// Current rule execution step
    step_current: usize,

    /// Table manager
    table_manager: &'a TableManager,
    /// Import mamager
    import_manager: &'a ImportManager,

    /// Variable order
    order: VariableOrder,
    /// Variable translation
    translation: VariableTranslation,
}
