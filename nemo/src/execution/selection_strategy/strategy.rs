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
    fn new(
        rules: Vec<&ChaseRule>,
        rule_analyses: Vec<&RuleAnalysis>,
    ) -> Result<Self, SelectionStrategyError>;

    /// Return the index of the next rule that should be executed.
    /// Returns `None` if there are no more rules to be applied
    /// and the execution should therefore stop.
    fn next_rule(&mut self, new_derivations: Option<bool>) -> Option<usize>;
}

/// Strategy for executing a set of rules,
/// which might involve different types of [`ExecutionStep`]s
pub trait ExecutionStrategy: std::fmt::Debug + Sized {
    /// Create a new [ExecutionStrategy] object.
    fn new(
        rules: Vec<&ChaseRule>,
        rule_analyses: Vec<&RuleAnalysis>,
    ) -> Result<Self, SelectionStrategyError>;

    /// Return the next step that should be executed.
    /// Returns `None` if there are no more rules to be applied
    /// and the execution should therefore stop.
    fn next_step(&mut self, new_derivations: Option<bool>) -> Option<ExecutionStep<'_>>;
}

/// Step that can be taken in the execution of a set of rules.
#[derive(Copy, Clone, Debug)]
pub enum ExecutionStep<'a> {
    /// Execute a single rule via the trie-join
    ExecuteRule {
        /// Index of the rule that shall be executed
        index: usize,
        /// Strategy for the rule execution
        execution: &'a RuleExecution,
    },
}

/// A strategy executing one rule at a time
#[derive(Debug)]
pub struct SingleStepStrategy<T> {
    inner: T,
    rule_execution: Box<[RuleExecution]>,
}

impl<T: RuleSelectionStrategy> ExecutionStrategy for SingleStepStrategy<T> {
    fn new(
        rules: Vec<&ChaseRule>,
        rule_analyses: Vec<&RuleAnalysis>,
    ) -> Result<Self, SelectionStrategyError> {
        let rule_execution = rules
            .iter()
            .zip(&rule_analyses)
            .map(|(r, a)| RuleExecution::initialize(r, a))
            .collect();

        Ok(SingleStepStrategy {
            inner: T::new(rules, rule_analyses)?,
            rule_execution,
        })
    }

    fn next_step(&mut self, new_derivations: Option<bool>) -> Option<ExecutionStep<'_>> {
        let index = self.inner.next_rule(new_derivations)?;
        Some(ExecutionStep::ExecuteRule {
            execution: &self.rule_execution[index],
            index,
        })
    }
}
