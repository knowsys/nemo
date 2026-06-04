//! Functionality for evaluating an existential rule program.

pub mod execution_engine;
pub use execution_engine::ExecutionEngine;

use self::selection_strategy::{
    dependency_graph::graph_positive::GraphConstructorPositive,
    //strategy_stratified_negation::StrategyStratifiedNegation,
    strategy_full_chain_stratification::StrategyFullChainStratification,
    strategy_graph::StrategyDependencyGraph,
    strategy_round_robin::StrategyRoundRobin,
};

pub mod execution_parameters;
pub mod planning;
pub mod selection_strategy;
pub mod tracing;

/// The default strategy that will be used for reasoning
//pub type DefaultExecutionStrategy = StrategyStratifiedNegation<
pub type DefaultExecutionStrategy = StrategyFullChainStratification<
    StrategyDependencyGraph<GraphConstructorPositive, StrategyRoundRobin>,
>;

/// Shorthand for an execution engine using the default strategy
pub type DefaultExecutionEngine = ExecutionEngine<DefaultExecutionStrategy>;
