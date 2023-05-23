//! Functionality for evaluating an existential rule program.

pub mod execution_engine;
pub use execution_engine::ExecutionEngine;

use self::selection_strategy::{
    dependency_graph::graph_positive::GraphConstructorPositive,
    strategy_graph::StrategyDependencyGraph, strategy_round_robin::StrategyRoundRobin,
};

pub mod planning;

pub mod rule_execution;

pub mod selection_strategy;

/// The default strategy that will be used for reasoning
pub type DefaultExecutionStrategy =
    StrategyDependencyGraph<GraphConstructorPositive, StrategyRoundRobin>;

/// Shorthand for an execution engine using the default strategy
pub type DefaultExecutionEngine = ExecutionEngine<DefaultExecutionStrategy>;
