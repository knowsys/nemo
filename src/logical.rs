//! This module defines logical data structures and operations.

pub mod permutator;
pub use permutator::Permutator;

pub mod model;

pub mod execution_engine;
pub use execution_engine::RuleExecutionEngine;

pub mod execution_plan;
pub use execution_plan::ExecutionNode;
pub use execution_plan::ExecutionPlan;
pub use execution_plan::ExecutionSeries;

pub mod table_manager;
pub use table_manager::TableManager;
