//! This module contains functions and data structures related to
//! converting rules into execution plans executed by nemo-physical

use nemo_physical::tabular::operations::OperationTableGenerator;

use crate::{
    io::ImportManager, rule_model::components::term::primitive::variable::Variable,
    table_manager::TableManager,
};

pub mod normalization;

pub(crate) mod analysis;
pub(crate) mod operations;
pub(crate) mod strategy;

/// Translation of a [Variable] into an [OperationTable][nemo_physical::tabular::operations::OperationColumnMarker],
/// which is used for constructing [ExecutionPlan][nemo_physical::management::execution_plan::ExecutionPlan]s
pub(crate) type VariableTranslation = OperationTableGenerator<Variable>;

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

    /// Variable translation
    translation: VariableTranslation,
}
