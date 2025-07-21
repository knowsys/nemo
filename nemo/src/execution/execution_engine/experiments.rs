//! This module contains code relating to the experiments of the AAAI submission.

use crate::execution::{selection_strategy::strategy::RuleSelectionStrategy, ExecutionEngine};

pub mod collect;

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {}
