//! This module defines a column builder for run length encoded columns.

use std::num::NonZeroUsize;

use crate::columnar::{
    column::rle::ColumnRle,
    columntype::{rle::RunLengthEncodable, ColumnType},
};

use super::ColumnBuilder;

/// Compact representation of multiple values of type `T`
/// that differ by a constant amount
#[derive(Debug, PartialEq)]
pub(crate) struct RleElement<T: RunLengthEncodable> {
    /// Start value
    pub value: T,
    /// Number of values represented by this element
    pub length: NonZeroUsize,
    /// Increment between consecutive values represented by this element
    pub increment: T::Step,
}

/// Implementation of [ColumnBuilder] that allows the use of incremental run length encoding.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ColumnBuilderRle<T: RunLengthEncodable> {
    elements: Vec<RleElement<T>>,
    previous_value_opt: Option<T>,
    count: usize,
}

impl<T> ColumnBuilderRle<T>
where
    T: ColumnType,
{
    /// Constructor.
    pub(crate) fn new() -> ColumnBuilderRle<T> {
        ColumnBuilderRle {
            elements: Vec::new(),
            previous_value_opt: None,
            count: 0,
        }
    }
}

impl<T> ColumnBuilderRle<T>
where
    T: ColumnType,
{
    /// Get the average length of RleElements to get a feeling for how much memory the encoding will take.
    pub(crate) fn avg_length_of_rle_elements(&self) -> usize {
        if self.elements.is_empty() {
            return 0;
        }

        self.count / self.elements.len()
    }

    /// Get number of RleElements in builder.
    pub(crate) fn number_of_rle_elements(&self) -> usize {
        self.elements.len()
    }
}

impl<'a, T> ColumnBuilder<'a, T> for ColumnBuilderRle<T>
where
    T: 'a + ColumnType,
{
    type Col = ColumnRle<T>;

    fn add(&mut self, value: T) {
        self.count += 1;
        let current_value = value;

        if self.elements.is_empty() {
            self.elements.push(RleElement {
                value: current_value,
                length: NonZeroUsize::new(1).expect("1 is non-zero"),
                increment: T::zero_step(),
            });

            self.previous_value_opt = Some(current_value);

            return;
        }

        let previous_value = self
            .previous_value_opt
            .expect("if the elements are not empty, then there is also a previous value");
        let last_element = self
            .elements
            .last_mut()
            .expect("if the elements are not empty, then there is a last one");
        let current_increment = T::diff_step(previous_value, current_value);

        match current_increment {
            Some(cur_inc) => {
                if last_element.length == NonZeroUsize::new(1).expect("1 is non-zero") {
                    last_element.length = NonZeroUsize::new(2).expect("2 is non-zero");
                    last_element.increment = cur_inc;
                } else if last_element.increment == cur_inc {
                    let last_length = last_element.length.get();

                    last_element.length =
                        NonZeroUsize::new(last_length + 1).expect("usize + 1 is non-zero");
                } else {
                    self.elements.push(RleElement {
                        value: current_value,
                        length: NonZeroUsize::new(1).expect("1 is non-zero"),
                        increment: T::zero_step(),
                    });
                }
            }
            None => {
                self.elements.push(RleElement {
                    value: current_value,
                    length: NonZeroUsize::new(1).expect("1 is non-zero"),
                    increment: T::zero_step(),
                });
            }
        }

        self.previous_value_opt = Some(current_value);
    }

    fn finalize(self) -> Self::Col {
        ColumnRle::from_rle_elements(self.elements)
    }

    fn count(&self) -> usize {
        self.count
    }
}
