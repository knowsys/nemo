use super::{Column, ColumnBuilder, ColumnScan};
use num::Zero;
use std::{
    fmt::Debug,
    iter::{repeat, Sum},
    num::NonZeroUsize,
    ops::{Add, Mul, Sub},
};

#[derive(Debug, PartialEq)]
struct RleElement<T> {
    value: T,
    length: NonZeroUsize,
    increment: T,
    is_negative_increment: bool,
}

impl<
        T: Copy + TryFrom<usize> + Add<Output = T> + Sub<Output = T> + Mul<Output = T> + Sum + Zero,
    > RleElement<T>
{
    fn get(&self, index: usize) -> T {
        if index >= self.length.get() {
            panic!("IndexOutOfBounds in rle value computation.");
        }

        if self.increment.is_zero() {
            return self.value;
        }

        let total_increment = if let Ok(t_index) = T::try_from(index) {
            t_index * self.increment
        } else {
            repeat(self.increment).take(index).sum()
        };

        if self.is_negative_increment {
            self.value - total_increment
        } else {
            self.value + total_increment
        }
    }
}

/// Implementation of [`ColumnBuilder`] that allows the use of incremental run length encoding.
#[derive(Debug, Default, PartialEq)]
pub struct RleColumnBuilder<T> {
    elements: Vec<RleElement<T>>,
    previous_value_opt: Option<T>,
}

impl<T> RleColumnBuilder<T> {
    /// Constructor.
    pub fn new() -> RleColumnBuilder<T> {
        RleColumnBuilder {
            elements: Vec::new(),
            previous_value_opt: None,
        }
    }
}

impl<
        T: Debug
            + Copy
            + TryFrom<usize>
            + PartialOrd
            + Add<Output = T>
            + Sub<Output = T>
            + Mul<Output = T>
            + PartialEq
            + Default
            + Sum
            + Zero,
    > RleColumnBuilder<T>
{
    /// Get the average length of RleElements to get a feeling for how much memory the encoding will take.
    pub fn avg_length_of_rle_elements(&self) -> usize {
        if self.elements.is_empty() {
            return 0;
        }

        self.elements.iter().map(|e| e.length.get()).sum::<usize>() / self.elements.len()
    }

    /// Get number of RleElements in builder.
    pub fn number_of_rle_elements(&self) -> usize {
        self.elements.len()
    }

    fn finalize_raw(self) -> RleColumn<T> {
        RleColumn::from_rle_elements(self.elements)
    }
}

impl<
        'a,
        T: 'a
            + Copy
            + TryFrom<usize>
            + Debug
            + PartialOrd
            + Add<Output = T>
            + Sub<Output = T>
            + Mul<Output = T>
            + PartialEq
            + Default
            + Sum
            + Zero,
    > ColumnBuilder<'a, T> for RleColumnBuilder<T>
{
    fn add(&mut self, value: T) {
        let current_value = value;

        if self.elements.is_empty() {
            self.elements.push(RleElement {
                value: current_value,
                length: NonZeroUsize::new(1).unwrap(),
                increment: Default::default(),
                is_negative_increment: Default::default(),
            });

            self.previous_value_opt = Some(current_value);

            return;
        }

        let previous_value = self.previous_value_opt.unwrap();
        let last_element = self.elements.last_mut().unwrap();
        let is_current_increment_negative = current_value < previous_value;
        let current_increment = if is_current_increment_negative {
            previous_value - current_value
        } else {
            current_value - previous_value
        };

        if last_element.length == NonZeroUsize::new(1).unwrap() {
            last_element.length = NonZeroUsize::new(2).unwrap();
            last_element.increment = current_increment;
            last_element.is_negative_increment = is_current_increment_negative;
        } else if last_element.increment == current_increment
            && last_element.is_negative_increment == is_current_increment_negative
        {
            last_element.length = NonZeroUsize::new(last_element.length.get() + 1).unwrap();
        } else {
            self.elements.push(RleElement {
                value: current_value,
                length: NonZeroUsize::new(1).unwrap(),
                increment: Default::default(),
                is_negative_increment: Default::default(),
            });
        }

        self.previous_value_opt = Some(current_value);
    }

    fn finalize(self) -> Box<dyn Column<T> + 'a> {
        Box::new(self.finalize_raw())
    }
}

/// Implementation of [`Column`] that allows the use of incremental run length encoding.
#[derive(Debug, PartialEq)]
pub struct RleColumn<T> {
    elements: Vec<RleElement<T>>,
}

impl<
        T: Debug
            + Copy
            + TryFrom<usize>
            + PartialOrd
            + Add<Output = T>
            + Sub<Output = T>
            + Mul<Output = T>
            + PartialEq
            + Default
            + Sum
            + Zero,
    > RleColumn<T>
{
    /// Constructs a new RleColumn from a vector of RleElements.
    fn from_rle_elements(elements: Vec<RleElement<T>>) -> RleColumn<T> {
        RleColumn { elements }
    }

    /// Constructs a new RleColumn from a vector of the suitable type.
    pub fn new(data: Vec<T>) -> RleColumn<T> {
        let mut builder = RleColumnBuilder::new();
        for value in data {
            builder.add(value);
        }

        builder.finalize_raw()
    }
}

impl<
        T: Debug
            + Copy
            + TryFrom<usize>
            + PartialOrd
            + Add<Output = T>
            + Sub<Output = T>
            + Mul<Output = T>
            + Sum
            + Zero,
    > Column<T> for RleColumn<T>
{
    fn len(&self) -> usize {
        self.elements.iter().map(|e| e.length.get()).sum()
    }

    fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    fn get(&self, index: usize) -> T {
        let mut target_index = index;
        let mut element_index = 0;
        while target_index >= self.elements[element_index].length.get() {
            target_index -= self.elements[element_index].length.get();
            element_index += 1;
        }
        self.elements[element_index].get(target_index)
    }

    fn iter<'a>(&'a self) -> Box<dyn ColumnScan<Item = T> + 'a> {
        Box::new(RleColumnScan::new(self))
    }
}

#[derive(Debug)]
struct RleColumnScan<'a, T> {
    column: &'a RleColumn<T>,
    element_index: Option<usize>,
    increment_index: Option<usize>,
    current: Option<T>,
}

impl<'a, T> RleColumnScan<'a, T> {
    pub fn new(column: &'a RleColumn<T>) -> RleColumnScan<'a, T> {
        RleColumnScan {
            column,
            element_index: None,
            increment_index: None,
            current: None,
        }
    }
}

impl<'a, T: Copy + TryFrom<usize> + Add<Output = T> + Sub<Output = T> + Mul<Output = T>> Iterator
    for RleColumnScan<'a, T>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut element_index = self.element_index.unwrap_or_default();
        let mut increment_index = self
            .increment_index
            .map_or_else(Default::default, |i| i + 1);

        if element_index < self.column.elements.len()
            && increment_index >= self.column.elements[element_index].length.get()
        {
            element_index += 1;
            increment_index = 0;
        }

        self.element_index = Some(element_index);
        self.increment_index = Some(increment_index);

        self.current = (element_index < self.column.elements.len()).then(|| {
            if increment_index == 0 {
                self.column.elements[element_index].value
            } else {
                let current = self
                    .current
                    .expect("after the first iteration the current value is always set");

                if self.column.elements[element_index].is_negative_increment {
                    current - self.column.elements[element_index].increment
                } else {
                    current + self.column.elements[element_index].increment
                }
            }
        });

        self.current
    }
}

impl<
        'a,
        T: Debug
            + Copy
            + TryFrom<usize>
            + PartialOrd
            + Add<Output = T>
            + Sub<Output = T>
            + Mul<Output = T>,
    > ColumnScan for RleColumnScan<'a, T>
{
    /// Find the next value that is at least as large as the given value,
    /// advance the iterator to this position, and return the value.
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        // could we assume a sorted column here?
        // for now we don't
        for next in self {
            if next >= value {
                return Some(next);
            }
        }
        None
    }

    fn current(&mut self) -> Option<Self::Item> {
        self.current
    }
}

#[cfg(test)]
mod test {
    use super::{Column, RleColumn, RleElement};
    use std::iter::repeat;
    use std::num::NonZeroUsize;
    use test_log::test;

    fn get_control_data() -> Vec<i64> {
        vec![2, 5, 6, 7, 8, 42, 4, 7, 10, 13, 16]
    }

    fn get_test_column_i64() -> RleColumn<i64> {
        RleColumn {
            elements: vec![
                RleElement {
                    value: 2,
                    length: NonZeroUsize::new(2).unwrap(),
                    increment: 3,
                    is_negative_increment: false,
                },
                RleElement {
                    value: 6,
                    length: NonZeroUsize::new(3).unwrap(),
                    increment: 1,
                    is_negative_increment: false,
                },
                RleElement {
                    value: 42,
                    length: NonZeroUsize::new(2).unwrap(),
                    increment: 38,
                    is_negative_increment: true,
                },
                RleElement {
                    value: 7,
                    length: NonZeroUsize::new(4).unwrap(),
                    increment: 3,
                    is_negative_increment: false,
                },
            ],
        }
    }

    fn get_test_column_u32() -> RleColumn<u32> {
        RleColumn {
            elements: vec![
                RleElement {
                    value: 2,
                    length: NonZeroUsize::new(2).unwrap(),
                    increment: 3,
                    is_negative_increment: false,
                },
                RleElement {
                    value: 6,
                    length: NonZeroUsize::new(3).unwrap(),
                    increment: 1,
                    is_negative_increment: false,
                },
                RleElement {
                    value: 42,
                    length: NonZeroUsize::new(2).unwrap(),
                    increment: 38,
                    is_negative_increment: true,
                },
                RleElement {
                    value: 7,
                    length: NonZeroUsize::new(4).unwrap(),
                    increment: 3,
                    is_negative_increment: false,
                },
            ],
        }
    }

    fn get_control_data_with_inc_zero() -> Vec<u8> {
        repeat(1).take(1000000).collect()
    }

    fn get_test_column_with_inc_zero() -> RleColumn<u8> {
        RleColumn {
            elements: vec![RleElement {
                value: 1,
                length: NonZeroUsize::new(1000000).unwrap(),
                increment: 0,
                is_negative_increment: false,
            }],
        }
    }

    #[test]
    fn i64_construction() {
        let raw_data = get_control_data();
        let expected: RleColumn<i64> = get_test_column_i64();

        let constructed: RleColumn<i64> = RleColumn::new(raw_data);
        assert_eq!(expected, constructed);
    }

    #[test]
    fn u32_construction() {
        let raw_data: Vec<u32> = get_control_data().iter().map(|x| *x as u32).collect();
        let expected: RleColumn<u32> = get_test_column_u32();

        let constructed: RleColumn<u32> = RleColumn::new(raw_data);
        assert_eq!(expected, constructed);
    }

    #[test]
    fn col_with_zero_increment_construction() {
        let raw_data: Vec<u8> = get_control_data_with_inc_zero();
        let expected: RleColumn<u8> = get_test_column_with_inc_zero();

        let constructed: RleColumn<u8> = RleColumn::new(raw_data);
        assert_eq!(expected, constructed);
    }

    #[test]
    fn is_empty() {
        let c: RleColumn<i64> = get_test_column_i64();
        assert!(!c.is_empty());

        let c_empty: RleColumn<i64> = RleColumn { elements: vec![] };
        assert!(c_empty.is_empty());
    }

    #[test]
    fn len() {
        let control_data = get_control_data();
        let c: RleColumn<i64> = get_test_column_i64();
        assert_eq!(c.len(), control_data.len());
    }

    #[test]
    fn get() {
        let control_data = get_control_data();
        let c: RleColumn<i64> = get_test_column_i64();

        control_data
            .iter()
            .enumerate()
            .for_each(|(i, control_item)| assert_eq!(c.get(i), *control_item))
    }

    #[test]
    fn get_with_large_index_on_inc_zero() {
        let control_data = get_control_data_with_inc_zero();
        let c: RleColumn<u8> = get_test_column_with_inc_zero();

        control_data
            .iter()
            .enumerate()
            .for_each(|(i, control_item)| assert_eq!(c.get(i), *control_item))
    }

    #[test]
    fn iter() {
        let control_data: Vec<i64> = get_control_data();
        let c: RleColumn<i64> = get_test_column_i64();

        let iterated_c: Vec<i64> = c.iter().collect();
        assert_eq!(iterated_c, control_data);
    }
}
