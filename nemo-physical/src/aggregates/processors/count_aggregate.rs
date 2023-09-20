//! Count the input values. Always returns an [i64], independent of the input value type.

use std::marker::PhantomData;

use crate::datatypes::StorageValueT;

use super::{
    aggregate::Aggregate,
    processor::{AggregateGroupProcessor, AggregateProcessor},
};

pub(crate) struct CountAggregateProcessor<A>
where
    A: Aggregate,
{
    phantom_data: PhantomData<A>,
}

impl<A: Aggregate> CountAggregateProcessor<A> {
    pub fn new() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<A: Aggregate> AggregateProcessor<A> for CountAggregateProcessor<A> {
    fn idempotent(&self) -> bool {
        false
    }

    fn group(&self) -> Box<dyn AggregateGroupProcessor<A>> {
        Box::new(CountAggregateGroupProcessor::new())
    }
}

pub(crate) struct CountAggregateGroupProcessor<A>
where
    A: Aggregate,
{
    current_count: i64,
    phantom_data: PhantomData<A>,
}

impl<A: Aggregate> CountAggregateGroupProcessor<A> {
    pub fn new() -> Self {
        Self {
            current_count: 0,
            phantom_data: PhantomData,
        }
    }
}

impl<A: Aggregate> AggregateGroupProcessor<A> for CountAggregateGroupProcessor<A> {
    fn write_aggregate_input_value(&mut self, _value: A) {
        self.current_count = self
            .current_count
            .checked_add(1)
            .expect("integer overflow in count aggregate operation"); // TODO: Improve error handling
    }

    fn finish(&self) -> Option<StorageValueT> {
        Some(self.current_count.into())
    }
}
