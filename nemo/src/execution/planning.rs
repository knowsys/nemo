//! This module implements the functionality for planing the execution of a rule.
mod aggregates;

pub(crate) mod operations;

pub(crate) mod plan_body_seminaive;
pub(crate) mod plan_head_datalog;
pub(crate) mod plan_head_restricted;
pub(crate) mod plan_tracing;

pub(crate) mod strategy_head;
pub(crate) use strategy_head::HeadStrategy;

pub(crate) mod strategy_body;
pub(crate) use strategy_body::BodyStrategy;
