//! Computes the minimum of all input values.

use std::cmp::Ordering;

use crate::datatypes::StorageValueT;

use super::processor::{AggregateGroupProcessor, AggregateProcessor, select_extremum};

#[derive(Debug)]
pub(crate) struct MinAggregateProcessor {}

impl MinAggregateProcessor {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl AggregateProcessor for MinAggregateProcessor {
    fn idempotent(&self) -> bool {
        true
    }

    fn group(&self) -> Box<dyn AggregateGroupProcessor> {
        Box::new(MinAggregateGroupProcessor::new())
    }
}

#[derive(Debug)]
pub(crate) struct MinAggregateGroupProcessor {
    current_min_value: Option<StorageValueT>,
}

impl MinAggregateGroupProcessor {
    pub(crate) fn new() -> Self {
        Self {
            current_min_value: None,
        }
    }
}

impl AggregateGroupProcessor for MinAggregateGroupProcessor {
    fn write_aggregate_input_value(&mut self, value: StorageValueT) {
        self.current_min_value = Some(match self.current_min_value {
            Some(current) => select_extremum(current, value, Ordering::Less),
            None => value,
        });
    }

    fn finish(&self) -> Option<StorageValueT> {
        self.current_min_value.as_ref().copied()
    }
}
