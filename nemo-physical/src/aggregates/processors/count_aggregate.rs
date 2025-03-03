//! Count the input values. Always returns an [i64], independent of the input value type.

use crate::storagevalues::storagevalue::StorageValueT;

use super::processor::{AggregateGroupProcessor, AggregateProcessor};

#[derive(Debug)]
pub(crate) struct CountAggregateProcessor {}

impl CountAggregateProcessor {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl AggregateProcessor for CountAggregateProcessor {
    fn idempotent(&self) -> bool {
        false
    }

    fn group(&self) -> Box<dyn AggregateGroupProcessor> {
        Box::new(CountAggregateGroupProcessor::new())
    }
}

#[derive(Debug)]
pub(crate) struct CountAggregateGroupProcessor {
    current_count: i64,
}

impl CountAggregateGroupProcessor {
    pub(crate) fn new() -> Self {
        Self { current_count: 0 }
    }
}

impl AggregateGroupProcessor for CountAggregateGroupProcessor {
    fn write_aggregate_input_value(&mut self, _value: StorageValueT) {
        self.current_count += 1;
    }

    fn finish(&self) -> Option<StorageValueT> {
        Some(self.current_count.into())
    }
}
