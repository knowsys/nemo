//! Contains the trait that defines what constitutes a rule execution strategy.

use thiserror::Error;

use crate::{
    chase_model::{analysis::program_analysis::RuleAnalysis, components::rule::ChaseRule},
    execution::rule_execution::RuleExecution,
};

/// Errors that can occur while creating a strategy.
#[derive(Error, Debug, Copy, Clone)]
pub enum SelectionStrategyError {
    /// Rules of the program cannot be stratified
    #[error("The rules of the program are not stratified.")]
    NonStratifiedProgram,
}

/// Trait that defines a strategy for rule execution,
/// namely the order in which the rules are applied in.
pub trait RuleSelectionStrategy: std::fmt::Debug + Sized {
    /// Create a new [RuleSelectionStrategy] object.
    fn new(rule_analyses: Vec<&RuleAnalysis>) -> Result<Self, SelectionStrategyError>;

    /// Return the index of the next rule that should be executed.
    /// Returns `None` if there are no more rules to be applied
    /// and the execution should therefore stop.
    fn next_rule(&mut self, new_derivations: Option<bool>) -> Option<usize>;
}

pub trait MetaStrategy: RuleSelectionStrategy {
    fn current_scc(&self) -> &[usize];
}
