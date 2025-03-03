//! Computes the maximum of all input values.

use crate::storagevalues::StorageValueT;

use super::processor::{AggregateGroupProcessor, AggregateProcessor};

#[derive(Debug)]
pub(crate) struct MaxAggregateProcessor {}

impl MaxAggregateProcessor {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl AggregateProcessor for MaxAggregateProcessor {
    fn idempotent(&self) -> bool {
        true
    }

    fn group(&self) -> Box<dyn AggregateGroupProcessor> {
        Box::new(MaxAggregateGroupProcessor::new())
    }
}

#[derive(Debug)]
pub(crate) struct MaxAggregateGroupProcessor {
    current_max_value: Option<StorageValueT>,
}

impl MaxAggregateGroupProcessor {
    pub(crate) fn new() -> Self {
        Self {
            current_max_value: None,
        }
    }
}

impl AggregateGroupProcessor for MaxAggregateGroupProcessor {
    fn write_aggregate_input_value(&mut self, value: StorageValueT) {
        match &self.current_max_value {
            Some(current_max_value) => {
                if value > *current_max_value {
                    self.current_max_value = Some(value);
                }
            }
            None => self.current_max_value = Some(value),
        }
    }

    fn finish(&self) -> Option<StorageValueT> {
        self.current_max_value.as_ref().copied()
    }
}
