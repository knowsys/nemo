//! Module containing different strategies for when to apply which rule

pub mod strategy;

pub(crate) mod strategy_graph;
pub(crate) mod strategy_random;
pub(crate) mod strategy_round_robin;
pub(crate) mod strategy_stratified_negation;

pub(crate) mod dependency_graph;
