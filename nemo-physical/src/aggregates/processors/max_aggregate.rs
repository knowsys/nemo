//! Computes the maximum of all input values.

use std::marker::PhantomData;

use crate::datatypes::StorageValueT;

use super::{
    aggregate::Aggregate,
    processor::{AggregateGroupProcessor, AggregateProcessor},
};

#[derive(Debug)]
pub(crate) struct MaxAggregateProcessor<A>
where
    A: PartialEq + PartialOrd + 'static,
{
    phantom_data: PhantomData<A>,
}

impl<A: Aggregate> MaxAggregateProcessor<A> {
    pub(crate) fn new() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<A: Aggregate> AggregateProcessor<A> for MaxAggregateProcessor<A> {
    fn idempotent(&self) -> bool {
        true
    }

    fn group(&self) -> Box<dyn AggregateGroupProcessor<A>> {
        Box::new(MaxAggregateGroupProcessor::new())
    }
}

#[derive(Debug)]
pub(crate) struct MaxAggregateGroupProcessor<A>
where
    A: Aggregate,
{
    current_max_value: Option<A>,
}

impl<A: Aggregate> MaxAggregateGroupProcessor<A> {
    pub(crate) fn new() -> Self {
        Self {
            current_max_value: None,
        }
    }
}

impl<A: Aggregate> AggregateGroupProcessor<A> for MaxAggregateGroupProcessor<A> {
    fn write_aggregate_input_value(&mut self, value: A) {
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
        self.current_max_value
            .as_ref()
            .map(|value| value.clone().into())
    }
}
