//! This module implements the functionality for planing the execution of a rule.

pub(crate) mod plan_body_seminaive;

pub(crate) mod plan_head_datalog;

pub(crate) mod plan_head_restricted;

pub(crate) mod strategy_head;
pub(crate) use strategy_head::HeadStrategy;

pub(crate) mod strategy_body;
pub(crate) use strategy_body::BodyStrategy;

pub(crate) mod seminaive_join;
pub(crate) use seminaive_join::SeminaiveJoinGenerator;

pub(crate) mod plan_util;

pub(crate) mod negation;

mod aggregates;

pub(crate) mod arithmetic;
