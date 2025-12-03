//! Computes the sum of all input values.

use crate::datatypes::{Double, Float, StorageValueT};

use super::processor::{AggregateGroupProcessor, AggregateProcessor};
use num::{CheckedAdd, Zero};

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
    current_sum_f32: Option<Float>,
    current_sum_i64: Option<i64>,
    current_sum_f64: Option<Double>,
}

impl SumAggregateGroupProcessor {
    pub(crate) fn new() -> Self {
        Self {
            current_sum_f32: None,
            current_sum_i64: None,
            current_sum_f64: None,
        }
    }
}

impl AggregateGroupProcessor for SumAggregateGroupProcessor {
    fn write_aggregate_input_value(&mut self, value: StorageValueT) {
        match value {
            StorageValueT::Double(value) => {
                if let Some(current_value) = self.current_sum_f64 {
                    self.current_sum_f64 = Some(
                        current_value
                            .checked_add(&value)
                            // TODO: Improve error handling
                            .expect("overflow in sum aggregate operation"),
                    )
                } else {
                    self.current_sum_f64 = Some(value)
                }
            }
            StorageValueT::Float(value) => {
                if let Some(current_value) = self.current_sum_f32 {
                    self.current_sum_f32 = Some(
                        current_value
                            .checked_add(&value)
                            // TODO: Improve error handling
                            .expect("overflow in sum aggregate operation"),
                    );
                } else {
                    self.current_sum_f32 = Some(value)
                }
            }
            StorageValueT::Int64(value) => {
                if let Some(current_value) = self.current_sum_i64 {
                    self.current_sum_i64 = Some(
                        current_value
                            .checked_add(value)
                            // TODO: Improve error handling
                            .expect("overflow in sum aggregate operation"),
                    );
                } else {
                    self.current_sum_i64 = Some(value)
                }
            }
            _ => (),
        }
    }

    fn finish(&self) -> Option<StorageValueT> {
        if self.current_sum_f64.is_some()
            || (self.current_sum_i64.is_some() && self.current_sum_f64.is_some())
        {
            let mut overall_sum = self.current_sum_f64.unwrap_or(Double::zero());

            // Lossy conversion
            if let Some(current_sum_i64) = self.current_sum_i64 {
                overall_sum = overall_sum
                    .checked_add(&Double::from_number(
                        Into::<i64>::into(current_sum_i64) as f64
                    ))
                    .expect("overflow in sum aggregate operation");
            }

            if let Some(current_sum_f32) = self.current_sum_f32 {
                overall_sum = overall_sum
                    .checked_add(&Double::from_number(
                        Into::<f32>::into(current_sum_f32) as f64
                    ))
                    .expect("overflow in sum aggregate operation");
            }

            Some(overall_sum.into())
        } else {
            self.current_sum_i64
                .map(Into::into)
                .or_else(|| self.current_sum_f32.map(Into::into))
        }
    }
}
