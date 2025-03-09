//! This module contains data structures and implementations
//! for realizing the evaluation of functions on columnar data.

pub mod evaluation;
pub mod tree;

pub(crate) mod definitions;
#[macro_use]
pub(crate) mod new_definitions;
pub(crate) mod new_tree;
pub(crate) mod program;
pub(crate) mod stack;
