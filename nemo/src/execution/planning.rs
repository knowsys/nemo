//! This module implements the functionality for planing the execution of a rule.

pub mod plan_body_seminaive;

pub mod plan_head_datalog;

pub mod plan_head_restricted;

pub mod strategy_head;
pub use strategy_head::HeadStrategy;

pub mod strategy_body;
pub use strategy_body::BodyStrategy;

pub mod seminaive_join;
pub use seminaive_join::SeminaiveJoinGenerator;

pub mod plan_util;

pub mod negation;

pub mod arithmetic;
