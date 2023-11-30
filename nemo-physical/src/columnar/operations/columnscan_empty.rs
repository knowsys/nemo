use super::super::traits::columnscan::ColumnScan;
use crate::datatypes::ColumnDataType;
use std::{fmt::Debug, marker::PhantomData, ops::Range};

/// Dummy Iterator that defers everything to its sub iterator
#[derive(Debug)]
pub struct ColumnScanEmpty<T> {
    _phantom: PhantomData<T>,
}

impl<T> ColumnScanEmpty<T> {
    /// Constructs a new [`ColumnScanEmpty`].
    pub fn new() -> Self {
        ColumnScanEmpty {
            _phantom: PhantomData::default(),
        }
    }
}

impl<T> Iterator for ColumnScanEmpty<T>
where
    T: ColumnDataType + PartialOrd + Eq,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl<T> ColumnScan for ColumnScanEmpty<T>
where
    T: ColumnDataType + PartialOrd + Eq,
{
    fn seek(&mut self, _value: T) -> Option<T> {
        None
    }

    fn current(&self) -> Option<T> {
        None
    }

    fn reset(&mut self) {}

    fn pos(&self) -> Option<usize> {
        None
    }
    fn narrow(&mut self, _interval: Range<usize>) {}
}
