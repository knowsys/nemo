//! This module defines intermediate datastructures
//! that are used to generate execution plans for the physical layer.

use crate::{
    chase_model::analysis::variable_order::VariableOrder,
    execution::rule_execution::VariableTranslation, io::ImportManager, table_manager::TableManager,
};

pub mod aggregation;
pub mod filter;
pub mod function;
pub mod import;
pub mod join;
pub mod join_seminaive;
pub mod negation;
pub mod projection_head;
pub mod union;

/// Collects runtime information for generating an execution plan
pub struct RuntimeInformation<'a> {
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
