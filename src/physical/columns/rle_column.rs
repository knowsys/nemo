use super::{Column, ColumnBuilder, ColumnScan};
use std::{
    fmt::Debug,
    num::NonZeroUsize,
    ops::{Add, Mul, Sub},
};

#[derive(Debug, PartialEq)]
struct RleElement<T, I = i64> {
    value: T,
    length: NonZeroUsize,
    increment: I,
}

impl<
        T: Copy + TryFrom<I>,
        I: Copy + TryFrom<T> + TryFrom<usize> + Add<Output = I> + Mul<Output = I>,
    > RleElement<T, I>
{
    fn get(&self, index: usize) -> T {
        let i_value = I::try_from(self.value)
            .or(Err(
                "should not happen if construction of RleColumn works correctly",
            ))
            .unwrap();
        let i_index = I::try_from(index)
            .or(Err(
                "should not happen if construction of RleColumn works correctly",
            ))
            .unwrap();

        let i_result = i_value + i_index * self.increment;

        T::try_from(i_result)
            .or(Err(
                "should not happen if construction of RleColumn works correctly",
            ))
            .unwrap()
    }
}

/// Implementation of [`ColumnBuilder`] that allows the use of incremental run length encoding.
#[derive(Debug, Default, PartialEq)]
pub struct RleColumnBuilder<T, I = i64> {
    elements: Vec<RleElement<T, I>>,
    previous_value_opt: Option<T>,
}

impl<
        T: Debug + Copy + TryFrom<I> + PartialOrd,
        I: Debug
            + Copy
            + TryFrom<T>
            + TryFrom<usize>
            + Add<Output = I>
            + Sub<Output = I>
            + Mul<Output = I>
            + PartialEq
            + Default,
    > RleColumnBuilder<T, I>
{
    /// Constructor.
    pub fn new() -> RleColumnBuilder<T, I> {
        RleColumnBuilder {
            elements: Vec::new(),
            previous_value_opt: None,
        }
    }

    /// Get the number of RleElements to get a feeling for how much memory the encoding will take.
    pub fn number_of_rle_elements(&self) -> usize {
        self.elements.len()
    }

    fn finalize_raw(self) -> RleColumn<T, I> {
        RleColumn::from_rle_elements(self.elements)
    }
}

impl<
        'a,
        T: 'a + Copy + Debug + TryFrom<I> + PartialOrd,
        I: 'a
            + Copy
            + Debug
            + TryFrom<T>
            + TryFrom<usize>
            + Add<Output = I>
            + Sub<Output = I>
            + Mul<Output = I>
            + PartialEq
            + Default,
    > ColumnBuilder<'a, T> for RleColumnBuilder<T, I>
{
    fn add(&mut self, value: T) {
        let current_value = value;

        if self.elements.is_empty() {
            self.elements.push(RleElement {
                value: current_value,
                length: NonZeroUsize::new(1).unwrap(),
                increment: I::default(),
            });

            self.previous_value_opt = Some(current_value);

            return;
        }

        let previous_value = self.previous_value_opt.unwrap();
        let last_element = self.elements.last_mut().unwrap();
        let current_increment = I::try_from(current_value)
            .or(Err("overflow when building RleColumn"))
            .unwrap()
            - I::try_from(previous_value)
                .or(Err("overflow when building RleColumn"))
                .unwrap();

        if last_element.length == NonZeroUsize::new(1).unwrap() {
            last_element.length = NonZeroUsize::new(2).unwrap();
            last_element.increment = current_increment;
        } else if last_element.increment == current_increment {
            last_element.length = NonZeroUsize::new(last_element.length.get() + 1).unwrap();
        } else {
            self.elements.push(RleElement {
                value: current_value,
                length: NonZeroUsize::new(1).unwrap(),
                increment: I::default(),
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
pub struct RleColumn<T, I = i64> {
    elements: Vec<RleElement<T, I>>,
}

impl<
        T: Debug + Copy + TryFrom<I> + PartialOrd,
        I: Debug
            + Copy
            + TryFrom<T>
            + TryFrom<usize>
            + Add<Output = I>
            + Sub<Output = I>
            + Mul<Output = I>
            + PartialEq
            + Default,
    > RleColumn<T, I>
{
    /// Constructs a new RleColumn from a vector of RleElements.
    fn from_rle_elements(elements: Vec<RleElement<T, I>>) -> RleColumn<T, I> {
        RleColumn { elements }
    }

    /// Constructs a new RleColumn from a vector of the suitable type.
    pub fn new(data: Vec<T>) -> RleColumn<T, I> {
        let mut builder = RleColumnBuilder::new();
        for value in data {
            builder.add(value);
        }

        builder.finalize_raw()
    }
}

impl<
        T: Debug + Copy + PartialOrd + TryFrom<I>,
        I: Debug + Copy + TryFrom<T> + TryFrom<usize> + Add<Output = I> + Mul<Output = I>,
    > Column<T> for RleColumn<T, I>
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
struct RleColumnScan<'a, T, I> {
    column: &'a RleColumn<T, I>,
    element_index: Option<usize>,
    increment_index: Option<usize>,
    current: Option<T>,
}

impl<'a, T, I> RleColumnScan<'a, T, I> {
    pub fn new(column: &'a RleColumn<T, I>) -> RleColumnScan<'a, T, I> {
        RleColumnScan {
            column,
            element_index: None,
            increment_index: None,
            current: None,
        }
    }
}

impl<'a, T: Copy + TryFrom<I>, I: Copy + TryFrom<T> + Add<Output = I>> Iterator
    for RleColumnScan<'a, T, I>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut element_index = self.element_index.unwrap_or_default();
        let mut increment_index = self
            .increment_index
            .map_or_else(Default::default, |i| i + 1);

        if increment_index >= self.column.elements[element_index].length.get() {
            element_index += 1;
            increment_index = 0;
        }

        self.element_index = Some(element_index);
        self.increment_index = Some(increment_index);

        self.current = (element_index < self.column.elements.len()).then(|| {
            if increment_index == 0 {
                self.column.elements[element_index].value
            } else {
                // self.current should always contain a value here
                let i_current = I::try_from(self.current.unwrap())
                    .or(Err("should work if construction works"))
                    .unwrap();
                T::try_from(i_current + self.column.elements[element_index].increment)
                    .or(Err("should work if construction works"))
                    .unwrap()
            }
        });

        self.current
    }
}

impl<
        'a,
        T: Debug + Copy + PartialOrd + TryFrom<I>,
        I: Debug + Copy + TryFrom<T> + Add<Output = I>,
    > ColumnScan for RleColumnScan<'a, T, I>
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
                },
                RleElement {
                    value: 6,
                    length: NonZeroUsize::new(3).unwrap(),
                    increment: 1,
                },
                RleElement {
                    value: 42,
                    length: NonZeroUsize::new(2).unwrap(),
                    increment: -38,
                },
                RleElement {
                    value: 7,
                    length: NonZeroUsize::new(4).unwrap(),
                    increment: 3,
                },
            ],
        }
    }

    fn get_test_column_u32_with_i64_inc() -> RleColumn<u32, i64> {
        RleColumn {
            elements: vec![
                RleElement {
                    value: 2,
                    length: NonZeroUsize::new(2).unwrap(),
                    increment: 3,
                },
                RleElement {
                    value: 6,
                    length: NonZeroUsize::new(3).unwrap(),
                    increment: 1,
                },
                RleElement {
                    value: 42,
                    length: NonZeroUsize::new(2).unwrap(),
                    increment: -38,
                },
                RleElement {
                    value: 7,
                    length: NonZeroUsize::new(4).unwrap(),
                    increment: 3,
                },
            ],
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
        let expected: RleColumn<u32> = get_test_column_u32_with_i64_inc();

        let constructed: RleColumn<u32> = RleColumn::new(raw_data);
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

        #[allow(clippy::needless_range_loop)]
        for i in 0..control_data.len() {
            assert_eq!(c.get(i), control_data[i]);
        }
    }

    #[test]
    fn iter() {
        let control_data: Vec<i64> = get_control_data();
        let c: RleColumn<i64> = get_test_column_i64();

        let iterated_c: Vec<i64> = c.iter().collect();
        assert_eq!(iterated_c, control_data);
    }
}
