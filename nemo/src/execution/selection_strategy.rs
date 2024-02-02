//! Module containing different strategies for when to apply which rule

pub mod strategy;

pub(crate) mod strategy_graph;
pub mod strategy_random;
pub mod strategy_round_robin;
pub mod strategy_stratified_negation;

pub(crate) mod dependency_graph;
