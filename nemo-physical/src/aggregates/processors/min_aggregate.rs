//! Computes the minimum of all input values.

use std::marker::PhantomData;

use crate::datatypes::StorageValueT;

use super::{
    aggregate::Aggregate,
    processor::{AggregateGroupProcessor, AggregateProcessor},
};

pub(crate) struct MinAggregateProcessor<A>
where
    A: Aggregate,
{
    phantom_data: PhantomData<A>,
}

impl<A: Aggregate> MinAggregateProcessor<A> {
    pub fn new() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<A: Aggregate> AggregateProcessor<A> for MinAggregateProcessor<A> {
    fn idempotent(&self) -> bool {
        true
    }

    fn group(&self) -> Box<dyn AggregateGroupProcessor<A>> {
        Box::new(MinAggregateGroupProcessor::new())
    }
}

pub(crate) struct MinAggregateGroupProcessor<A>
where
    A: Aggregate,
{
    current_min_value: Option<A>,
}

impl<A: Aggregate> MinAggregateGroupProcessor<A> {
    pub fn new() -> Self {
        Self {
            current_min_value: None,
        }
    }
}

impl<A: Aggregate> AggregateGroupProcessor<A> for MinAggregateGroupProcessor<A> {
    fn write_aggregate_input_value(&mut self, value: A) {
        match &self.current_min_value {
            Some(current_min_value) => {
                if value < *current_min_value {
                    self.current_min_value = Some(value);
                }
            }
            None => self.current_min_value = Some(value),
        }
    }

    fn finish(&self) -> Option<StorageValueT> {
        self.current_min_value
            .as_ref()
            .map(|value| value.clone().into())
    }
}
