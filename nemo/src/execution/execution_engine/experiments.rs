//! This module contains code relating to the experiments of the AAAI submission.

use crate::execution::{ExecutionEngine, selection_strategy::strategy::RuleSelectionStrategy};

pub mod collect;
pub mod node;

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {}
