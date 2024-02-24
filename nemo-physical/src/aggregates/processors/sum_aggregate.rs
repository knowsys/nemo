//! Computes the sum of all input values.

use crate::datatypes::{Double, StorageValueT};

use super::processor::{AggregateGroupProcessor, AggregateProcessor};
use num::CheckedAdd;

#[derive(Debug)]
pub(crate) struct SumAggregateProcessor {}

impl SumAggregateProcessor {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl AggregateProcessor for SumAggregateProcessor {
    fn idempotent(&self) -> bool {
        false
    }

    fn group(&self) -> Box<dyn AggregateGroupProcessor> {
        Box::new(SumAggregateGroupProcessor::new())
    }
}

#[derive(Debug)]
pub(crate) struct SumAggregateGroupProcessor {
    current_sum: Double,
}

impl SumAggregateGroupProcessor {
    pub(crate) fn new() -> Self {
        Self {
            current_sum: Double::default(),
        }
    }
}

impl AggregateGroupProcessor for SumAggregateGroupProcessor {
    fn write_aggregate_input_value(&mut self, value: StorageValueT) {
        match value {
            StorageValueT::Double(value) => {
                self.current_sum = self
                    .current_sum
                    .checked_add(&value)
                    // TODO: Improve error handling
                    .expect("overflow in sum aggregate operation")
            }
            StorageValueT::Float(value) => {
                self.current_sum = self
                    .current_sum
                    .checked_add(&Double::from_number(Into::<f32>::into(value) as f64))
                    // TODO: Improve error handling
                    .expect("overflow in sum aggregate operation")
            }
            StorageValueT::Int64(value) => {
                self.current_sum = self
                    .current_sum
                    // Lossy conversion to f64
                    .checked_add(&Double::from_number(Into::<i64>::into(value) as f64))
                    // TODO: Improve error handling
                    .expect("overflow in sum aggregate operation")
            }
            _ => (),
        }
    }

    fn finish(&self) -> Option<StorageValueT> {
        Some(self.current_sum.into())
    }
}
