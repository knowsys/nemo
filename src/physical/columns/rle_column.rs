use super::{Column, ColumnBuilder, ColumnScan, RangedColumnScan};
use crate::physical::datatypes::{Field, FloorToUsize, Ring};
use num::Zero;
use std::{
    fmt::Debug,
    iter::repeat,
    num::NonZeroUsize,
    ops::{Add, Mul, Range},
};

#[derive(Debug, Clone, Copy, PartialEq)]
enum Step<T> {
    Increment(T),
    Decrement(T),
}

impl<T> Eq for Step<T> where T: PartialEq {}

impl<T> Default for Step<T>
where
    T: Default,
{
    fn default() -> Self {
        Self::Increment(Default::default())
    }
}

impl<T> Step<T>
where
    T: Copy + Ord + Ring,
{
    // returns None if the computed diff does not allow the computation of the original value
    // (may happen for floating point numbers)
    fn from_two_values(previous_value: T, next_value: T) -> Option<Self> {
        if next_value < previous_value {
            let diff = previous_value - next_value;

            (next_value == previous_value - diff).then(|| Self::Decrement(diff))
        } else {
            let diff = next_value - previous_value;

            (next_value == previous_value + diff).then(|| Self::Increment(diff))
        }
    }
}

impl<T> Add<T> for Step<T>
where
    T: Ring,
{
    type Output = T;

    fn add(self, rhs: T) -> T {
        match self {
            Self::Increment(inc) => rhs + inc,
            Self::Decrement(dec) => rhs - dec,
        }
    }
}

impl<T> Add for Step<T>
where
    T: Ord + Ring,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        match self {
            Self::Increment(l_inc) => match rhs {
                Self::Increment(r_inc) => Self::Increment(l_inc + r_inc),
                Self::Decrement(r_dec) => {
                    if l_inc < r_dec {
                        Self::Decrement(r_dec - l_inc)
                    } else {
                        Self::Increment(l_inc - r_dec)
                    }
                }
            },
            Self::Decrement(l_dec) => match rhs {
                Self::Decrement(r_dec) => Self::Decrement(l_dec + r_dec),
                Self::Increment(r_inc) => {
                    if r_inc < l_dec {
                        Self::Decrement(l_dec - r_inc)
                    } else {
                        Self::Increment(r_inc - l_dec)
                    }
                }
            },
        }
    }
}

impl<T> Mul<usize> for Step<T>
where
    T: Copy + TryFrom<usize> + Ring,
{
    type Output = Self;

    fn mul(self, rhs: usize) -> Self {
        let raw_increment = match self {
            Self::Increment(inc) => inc,
            Self::Decrement(dec) => dec,
        };

        let total_increment = if let Ok(t_rhs) = T::try_from(rhs) {
            t_rhs * raw_increment
        } else {
            repeat(raw_increment).take(rhs).sum()
        };

        match self {
            Self::Increment(_) => Self::Increment(total_increment),
            Self::Decrement(_) => Self::Decrement(total_increment),
        }
    }
}

impl<T> Zero for Step<T>
where
    T: Zero + Ord + Ring,
{
    fn zero() -> Self {
        Self::Increment(T::zero())
    }

    fn is_zero(&self) -> bool {
        match self {
            Self::Increment(inc) => inc.is_zero(),
            Self::Decrement(dec) => dec.is_zero(),
        }
    }
}

#[derive(Debug, PartialEq)]
struct RleElement<T> {
    value: T,
    length: NonZeroUsize,
    increment: Step<T>,
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

impl<T> RleColumnBuilder<T>
where
    T: Debug + Copy + Ord + TryFrom<usize> + Default + Field + FloorToUsize,
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

impl<'a, T> ColumnBuilder<'a, T> for RleColumnBuilder<T>
where
    T: 'a + Copy + Ord + TryFrom<usize> + Debug + Default + Field + FloorToUsize,
{
    fn add(&mut self, value: T) {
        let current_value = value;

        if self.elements.is_empty() {
            self.elements.push(RleElement {
                value: current_value,
                length: NonZeroUsize::new(1).expect("1 is non-zero"),
                increment: Default::default(),
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
        let current_increment = Step::from_two_values(previous_value, current_value);

        match current_increment {
            Some(cur_inc) => {
                if last_element.length == NonZeroUsize::new(1).expect("1 is non-zero") {
                    last_element.length = NonZeroUsize::new(2).expect("2 is non-zero");
                    last_element.increment = cur_inc;
                } else if last_element.increment == cur_inc {
                    last_element.length = NonZeroUsize::new(last_element.length.get() + 1)
                        .expect("usize + 1 is non-zero");
                } else {
                    self.elements.push(RleElement {
                        value: current_value,
                        length: NonZeroUsize::new(1).expect("1 is non-zero"),
                        increment: Default::default(),
                    });
                }
            }
            None => {
                self.elements.push(RleElement {
                    value: current_value,
                    length: NonZeroUsize::new(1).expect("1 is non-zero"),
                    increment: Default::default(),
                });
            }
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
    values: Vec<T>,
    end_indices: Vec<NonZeroUsize>,
    increments: Vec<Step<T>>,
}

impl<T> RleColumn<T>
where
    T: Debug + Copy + Ord + TryFrom<usize> + Default + Field + FloorToUsize,
{
    /// Constructs a new RleColumn from a vector of RleElements.
    fn from_rle_elements(elements: Vec<RleElement<T>>) -> RleColumn<T> {
        let mut values: Vec<T> = vec![];
        let mut end_indices: Vec<NonZeroUsize> = vec![];
        let mut increments: Vec<Step<T>> = vec![];

        elements.iter().for_each(|e| {
            values.push(e.value);
            end_indices.push(
                NonZeroUsize::new(
                    end_indices.last().map(|nzusize| nzusize.get()).unwrap_or(0) + e.length.get(),
                )
                .expect("usize is >= 0 and e.length is always > 0"),
            );
            increments.push(e.increment);
        });

        RleColumn {
            values,
            end_indices,
            increments,
        }
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

impl<T> Column<T> for RleColumn<T>
where
    T: Debug + Copy + Ord + TryFrom<usize> + Field + FloorToUsize,
{
    fn len(&self) -> usize {
        self.end_indices
            .last()
            .map(|nzusize| nzusize.get())
            .unwrap_or(0)
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn get(&self, index: usize) -> T {
        let target_index_in_datastructure = if index == 0 {
            0
        } else {
            self.end_indices
                .binary_search(
                    &NonZeroUsize::new(index)
                        .expect("index is > 0 in this branch of the condition"),
                )
                .map(|i| i + 1)
                .unwrap_or_else(|i| i)
        };

        let remainder = index
            - target_index_in_datastructure
                .checked_sub(1)
                .map(|i| self.end_indices[i].get())
                .unwrap_or(0);
        let value = self.values[target_index_in_datastructure];
        let increment = self.increments[target_index_in_datastructure];

        if increment.is_zero() {
            return value;
        }

        // explicit check for zero since increment may be Infinity for floating point types
        // (and Infinity * 0 produces NaN)
        if remainder == 0 {
            value
        } else {
            increment * remainder + value
        }
    }

    fn iter<'a>(&'a self) -> Box<dyn RangedColumnScan<Item = T> + 'a> {
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

impl<'a, T> Iterator for RleColumnScan<'a, T>
where
    T: Copy + Ord + TryFrom<usize> + Ring,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut element_index = self.element_index.unwrap_or_default();
        let mut increment_index = self
            .increment_index
            .map_or_else(Default::default, |i| i + 1);

        if element_index < self.column.values.len()
            && increment_index
                >= self.column.end_indices[element_index].get()
                    - element_index
                        .checked_sub(1)
                        .map(|i| self.column.end_indices[i].get())
                        .unwrap_or(0)
        {
            element_index += 1;
            increment_index = 0;
        }

        self.element_index = Some(element_index);
        self.increment_index = Some(increment_index);

        self.current = (element_index < self.column.values.len()).then(|| {
            if increment_index == 0 {
                self.column.values[element_index]
            } else {
                let current = self
                    .current
                    .expect("after the first iteration the current value is always set");

                self.column.increments[element_index] + current
            }
        });

        self.current
    }
}

impl<'a, T> ColumnScan for RleColumnScan<'a, T>
where
    T: Debug + Copy + Ord + TryFrom<usize> + Field + FloorToUsize,
{
    /// Find the next value that is at least as large as the given value,
    /// advance the iterator to this position, and return the value.
    /// If multiple candidates exist, the iterator should be advanced to the first such value.
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        // seek only works correctly if column is sorted; we are just checking this here
        debug_assert!(
            self.column.is_empty()
                || self
                    .column
                    .iter()
                    .take(self.column.len() - 1)
                    .zip(self.column.iter().skip(1))
                    .all(|pair| pair.0 <= pair.1)
        );

        if self.column.values.is_empty() {
            return None;
        }

        let current_element_index = self.element_index.unwrap_or_default();
        let current_increment_index = self.increment_index.unwrap_or_default();

        // it is possible that the last element of the matching_element_index - 1 -th element
        // holds the same value as the matching element; in this case, we want to position the
        // iterator on that element;
        // hence we subtract 1 in any case
        let bin_search_element_index = self.column.values[current_element_index..]
            .binary_search(&value)
            .unwrap_or_else(|err| err)
            .saturating_sub(1)
            + current_element_index;

        let mut seek_element_index: usize = bin_search_element_index;
        let mut seek_increment_index: usize;

        let start_value = self.column.values[bin_search_element_index];
        let inc = self.column.increments[bin_search_element_index];

        if let Step::Increment(inc) = inc {
            if value <= start_value {
                seek_element_index = bin_search_element_index;
                seek_increment_index = 0;
            } else if inc.is_zero() {
                seek_element_index = bin_search_element_index + 1;
                seek_increment_index = 0;
            } else {
                seek_increment_index = ((value - start_value) / inc)
                    .floor_to_usize()
                    .unwrap_or_default();

                if bin_search_element_index
                    .checked_sub(1)
                    .and_then(|el_idx| self.column.end_indices.get(el_idx))
                    .map(|nzu| nzu.get())
                    .unwrap_or(0)
                    + seek_increment_index
                    >= self.column.end_indices[bin_search_element_index].get()
                {
                    seek_element_index = bin_search_element_index + 1;
                    seek_increment_index = 0;
                }
            }
        } else {
            panic!("if the column is sorted all increments are positive")
        };

        if seek_element_index > current_element_index
            || (seek_element_index == current_increment_index
                && seek_increment_index > current_increment_index)
        {
            self.element_index = Some(seek_element_index);
            self.increment_index = Some(seek_increment_index);
        } else {
            self.element_index = Some(current_element_index);
            self.increment_index = Some(current_increment_index);
        }

        self.current = if self
            .element_index
            .expect("This is just set a few lines above.")
            >= self.column.values.len()
        {
            None
        } else {
            Some(
                self.column.get(
                    self.element_index
                        .expect("This is just set a few lines above.")
                        .checked_sub(1)
                        .and_then(|el_idx| self.column.end_indices.get(el_idx))
                        .map(|nzu| nzu.get())
                        .unwrap_or(0)
                        + self
                            .increment_index
                            .expect("This is just set a few lines above."),
                ),
            )
        };

        if let Some(cur) = self.current {
            if cur >= value {
                return Some(cur);
            }
        }

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

    fn reset(&mut self) {
        self.element_index = None;
        self.increment_index = None;
        self.current = None;
    }
}

impl<'a, T> RangedColumnScan for RleColumnScan<'a, T>
where
    T: Debug + Copy + Ord + TryFrom<usize> + Field + FloorToUsize,
{
    fn pos(&self) -> Option<usize> {
        unimplemented!("RleColumnScan does not support intervals for now");
    }

    fn narrow(&mut self, _interval: Range<usize>) {
        unimplemented!("RleColumnScan does not support intervals for now");
    }
}

#[cfg(test)]
mod test {
    use super::{Column, RleColumn, Step};
    use crate::physical::datatypes::{Double, Float};
    use num::Zero;
    use quickcheck_macros::quickcheck;
    use std::iter::repeat;
    use std::num::NonZeroUsize;
    use test_log::test;

    fn get_control_data() -> Vec<i64> {
        vec![2, 5, 6, 7, 8, 42, 4, 7, 10, 13, 16]
    }

    fn get_test_column_i64() -> RleColumn<i64> {
        RleColumn {
            values: vec![2, 6, 42, 7],
            end_indices: vec![
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(5).unwrap(),
                NonZeroUsize::new(7).unwrap(),
                NonZeroUsize::new(11).unwrap(),
            ],
            increments: vec![
                Step::Increment(3),
                Step::Increment(1),
                Step::Decrement(38),
                Step::Increment(3),
            ],
        }
    }

    fn get_test_column_u32() -> RleColumn<u32> {
        RleColumn {
            values: vec![2, 6, 42, 7],
            end_indices: vec![
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(5).unwrap(),
                NonZeroUsize::new(7).unwrap(),
                NonZeroUsize::new(11).unwrap(),
            ],
            increments: vec![
                Step::Increment(3),
                Step::Increment(1),
                Step::Decrement(38),
                Step::Increment(3),
            ],
        }
    }

    fn get_control_data_with_inc_zero() -> Vec<u8> {
        repeat(1).take(1000000).collect()
    }

    fn get_test_column_with_inc_zero() -> RleColumn<u8> {
        RleColumn {
            values: vec![1],
            end_indices: vec![NonZeroUsize::new(1000000).unwrap()],
            increments: vec![Step::zero()],
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

        let c_empty: RleColumn<i64> = RleColumn {
            values: vec![],
            end_indices: vec![],
            increments: vec![],
        };
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

    #[quickcheck]
    fn get_quickcheck_u64(raw_data: Vec<u64>) -> bool {
        let col = RleColumn::new(raw_data.clone());

        raw_data
            .iter()
            .enumerate()
            .all(|(i, control_item)| col.get(i) == *control_item)
    }

    #[quickcheck]
    fn get_quickcheck_float(raw_data: Vec<Float>) -> bool {
        let col = RleColumn::new(raw_data.clone());

        raw_data
            .iter()
            .enumerate()
            .all(|(i, control_item)| col.get(i) == *control_item)
    }

    #[quickcheck]
    fn get_quickcheck_double(raw_data: Vec<Double>) -> bool {
        let col = RleColumn::new(raw_data.clone());

        raw_data
            .iter()
            .enumerate()
            .all(|(i, control_item)| col.get(i) == *control_item)
    }

    #[test]
    fn iter() {
        let control_data: Vec<i64> = get_control_data();
        let c: RleColumn<i64> = get_test_column_i64();

        let iterated_c: Vec<i64> = c.iter().collect();
        assert_eq!(iterated_c, control_data);
    }

    #[quickcheck]
    fn iter_quickcheck_u64(raw_data: Vec<u64>) -> bool {
        let col = RleColumn::new(raw_data.clone());

        let iterated_col: Vec<u64> = col.iter().collect();
        iterated_col == raw_data
    }

    #[quickcheck]
    fn iter_quickcheck_float(raw_data: Vec<Float>) -> bool {
        let col = RleColumn::new(raw_data.clone());

        let iterated_col: Vec<Float> = col.iter().collect();
        iterated_col == raw_data
    }

    #[quickcheck]
    fn iter_quickcheck_double(raw_data: Vec<Double>) -> bool {
        let col = RleColumn::new(raw_data.clone());

        let iterated_col: Vec<Double> = col.iter().collect();
        iterated_col == raw_data
    }

    #[test]
    fn seek() {
        let seek_test_col = RleColumn {
            values: vec![2, 22],
            end_indices: vec![NonZeroUsize::new(3).unwrap(), NonZeroUsize::new(9).unwrap()],
            increments: vec![Step::Increment(5), Step::Increment(1)],
        };

        let mut iter = seek_test_col.iter();
        iter.seek(10);

        assert_eq!(iter.current().unwrap(), 12);

        let mut iter = seek_test_col.iter();
        iter.seek(22);

        assert_eq!(iter.current().unwrap(), 22);

        let mut iter = seek_test_col.iter();
        iter.seek(28);

        assert_eq!(iter.current(), None);
    }

    #[quickcheck]
    fn seek_quickcheck_u64(mut raw_data: Vec<u64>, target: u64) -> bool {
        raw_data.sort_unstable();

        let expected_output = raw_data
            .get(raw_data.binary_search(&target).unwrap_or_else(|err| err))
            .copied();

        let col = RleColumn::new(raw_data);
        let seek_result = col.iter().seek(target);

        seek_result == expected_output
    }

    #[quickcheck]
    fn seek_quickcheck_float(mut raw_data: Vec<Float>, target: Float) -> bool {
        raw_data.sort();

        let expected_output = raw_data
            .get(raw_data.binary_search(&target).unwrap_or_else(|err| err))
            .copied();

        let col = RleColumn::new(raw_data);
        let seek_result = col.iter().seek(target);

        seek_result == expected_output
    }

    #[quickcheck]
    fn seek_quickcheck_double(mut raw_data: Vec<Double>, target: Double) -> bool {
        raw_data.sort();

        let expected_output = raw_data
            .get(raw_data.binary_search(&target).unwrap_or_else(|err| err))
            .copied();

        let col = RleColumn::new(raw_data);
        let seek_result = col.iter().seek(target);

        seek_result == expected_output
    }
}
