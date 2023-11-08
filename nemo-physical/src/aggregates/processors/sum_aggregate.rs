//! Computes the sum of all input values.

use std::marker::PhantomData;

use crate::datatypes::StorageValueT;

use super::{
    aggregate::Aggregate,
    processor::{AggregateGroupProcessor, AggregateProcessor},
};

#[derive(Debug)]
pub(crate) struct SumAggregateProcessor<A>
where
    A: Aggregate,
{
    phantom_data: PhantomData<A>,
}

impl<A: Aggregate> SumAggregateProcessor<A> {
    pub fn new() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<A: Aggregate> AggregateProcessor<A> for SumAggregateProcessor<A> {
    fn idempotent(&self) -> bool {
        false
    }

    fn group(&self) -> Box<dyn AggregateGroupProcessor<A>> {
        Box::new(SumAggregateGroupProcessor::new())
    }
}

#[derive(Debug)]
pub(crate) struct SumAggregateGroupProcessor<A>
where
    A: Aggregate,
{
    current_sum: A,
}

impl<A: Aggregate> SumAggregateGroupProcessor<A> {
    pub fn new() -> Self {
        Self {
            current_sum: A::default(),
        }
    }
}

impl<A: Aggregate> AggregateGroupProcessor<A> for SumAggregateGroupProcessor<A> {
    fn write_aggregate_input_value(&mut self, value: A) {
        self.current_sum = self
            .current_sum
            .checked_add(&value)
            .expect("overflow in sum aggregate operation"); // TODO: Improve error handling
    }

    fn finish(&self) -> Option<StorageValueT> {
        Some(self.current_sum.clone().into())
    }
}
