use super::{Column, ColumnBuilder, ColumnScan, RangedColumnScan};
use crate::physical::datatypes::{ColumnDataType, Field, Ring};
use num::Zero;
use std::{
    fmt::Debug,
    iter::repeat,
    num::NonZeroUsize,
    ops::{Add, Mul, Range},
};

trait ReversibleMul {
    fn reversible_mul(self, rhs: Self) -> Option<Self>
    where
        Self: Sized;
}

impl<T> ReversibleMul for T
where
    T: Copy + Ord + Field + std::panic::RefUnwindSafe,
{
    fn reversible_mul(self, rhs: Self) -> Option<Self> {
        // NOTE: we use this to catch overflow in multiplication
        std::panic::catch_unwind(|| self * rhs)
            .ok()
            // NOTE: we use this to rule out that product is not something like f32::INFINITY
            .and_then(|product| (product / rhs == self).then_some(product))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Step<T> {
    Increment(T),
    Decrement(T),
    // the following two are used as alternative to multiplication
    // when multiplication would overflow T
    RepeatedIncrement(T, usize),
    RepeatedDecrement(T, usize),
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

            (next_value == previous_value - diff).then_some(Self::Decrement(diff))
        } else {
            let diff = next_value - previous_value;

            (next_value == previous_value + diff).then_some(Self::Increment(diff))
        }
    }

    // just a special case of from_two_values with the first being zero and without the checks
    fn from_t(value: T) -> Self {
        if value < T::zero() {
            Self::Decrement(T::zero() - value)
        } else {
            Self::Increment(value)
        }
    }
}

impl<T> Add<T> for Step<T>
where
    T: Copy + Ring,
{
    type Output = T;

    fn add(self, rhs: T) -> T {
        match self {
            Self::Increment(inc) => rhs + inc,
            Self::Decrement(dec) => rhs - dec,
            Self::RepeatedIncrement(inc, mul) => {
                repeat(inc).take(mul).fold(rhs, |sum, inc| sum + inc)
            }
            Self::RepeatedDecrement(dec, mul) => {
                repeat(dec).take(mul).fold(rhs, |sum, dec| sum - dec)
            }
        }
    }
}

impl<T> Add for Step<T>
where
    T: Copy + Ord + Ring,
{
    type Output = Self;

    // NOTE: this may overflow but it should be fine for our use cases
    fn add(self, rhs: Self) -> Self {
        Self::from_t(self + (rhs + T::zero()))
    }
}

impl<T> Mul<usize> for Step<T>
where
    T: Copy + Ord + TryFrom<usize> + Field + std::panic::RefUnwindSafe,
{
    type Output = Self;

    fn mul(self, rhs: usize) -> Self {
        match self {
            Self::Increment(inc) => T::try_from(rhs)
                .ok()
                .and_then(|t_rhs| inc.reversible_mul(t_rhs))
                .map(|prod| Self::Increment(prod))
                .unwrap_or(Self::RepeatedIncrement(inc, rhs)),
            Self::Decrement(dec) => T::try_from(rhs)
                .ok()
                .and_then(|t_rhs| dec.reversible_mul(t_rhs))
                .map(|prod| Self::Decrement(prod))
                .unwrap_or(Self::RepeatedDecrement(dec, rhs)),
            Self::RepeatedIncrement(inc, mul) => Self::RepeatedIncrement(inc, rhs * mul),
            Self::RepeatedDecrement(dec, mul) => Self::RepeatedDecrement(dec, rhs * mul),
        }
    }
}

impl<T> Zero for Step<T>
where
    T: Copy + Zero + Ord + Ring,
{
    fn zero() -> Self {
        Self::Increment(T::zero())
    }

    fn is_zero(&self) -> bool {
        match self {
            Self::Increment(inc) => inc.is_zero(),
            Self::Decrement(dec) => dec.is_zero(),
            Self::RepeatedIncrement(inc, mul) => inc.is_zero() || mul.is_zero(),
            Self::RepeatedDecrement(dec, mul) => dec.is_zero() || mul.is_zero(),
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
    count: usize,
}

impl<T> RleColumnBuilder<T> {
    /// Constructor.
    pub fn new() -> RleColumnBuilder<T> {
        RleColumnBuilder {
            elements: Vec::new(),
            previous_value_opt: None,
            count: 0,
        }
    }
}

impl<T> RleColumnBuilder<T>
where
    T: ColumnDataType + Default,
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
    T: 'a + ColumnDataType + Default,
{
    type Col = RleColumn<T>;

    fn add(&mut self, value: T) {
        self.count += 1;
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

    fn finalize(self) -> Self::Col {
        self.finalize_raw()
    }

    fn count(&self) -> usize {
        self.count
    }
}

/// Implementation of [`Column`] that allows the use of incremental run length encoding.
#[derive(Debug, PartialEq, Clone)]
pub struct RleColumn<T> {
    values: Vec<T>,
    end_indices: Vec<NonZeroUsize>,
    increments: Vec<Step<T>>,
}

impl<T> RleColumn<T>
where
    T: ColumnDataType + Default,
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

impl<T> RleColumn<T>
where
    T: ColumnDataType,
{
    fn get_element_and_increment_index_from_global_index(&self, index: usize) -> (usize, usize) {
        let element_index = if index == 0 {
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

        let increment_index = index
            - element_index
                .checked_sub(1)
                .map(|i| self.end_indices[i].get())
                .unwrap_or(0);

        (element_index, increment_index)
    }

    fn get_internal(&self, element_index: usize, increment_index: usize) -> T {
        let value = self.values[element_index];
        let increment = self.increments[element_index];

        if increment.is_zero() {
            return value;
        }

        // explicit check for zero since increment may be Infinity for floating point types
        // (and Infinity * 0 produces NaN)
        if increment_index == 0 {
            value
        } else {
            increment * increment_index + value
        }
    }
}

impl<'a, T> Column<'a, T> for RleColumn<T>
where
    T: 'a + ColumnDataType,
{
    type ColScan = RleColumnScan<'a, T>;

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
        let (element_index, increment_index) =
            self.get_element_and_increment_index_from_global_index(index);

        self.get_internal(element_index, increment_index)
    }

    fn iter(&'a self) -> Self::ColScan {
        RleColumnScan::new(self)
    }
}

/// Column Scan tailored towards RleColumns
#[derive(Debug)]
pub struct RleColumnScan<'a, T> {
    column: &'a RleColumn<T>,
    element_index: Option<usize>,
    increment_index: Option<usize>,
    current: Option<T>,
    lower_bound_inclusive: (usize, usize), // element_index and increment index
    upper_bound_exclusive: (usize, usize), // element_index and increment index
}

impl<'a, T> RleColumnScan<'a, T>
where
    T: ColumnDataType,
{
    /// Constructor for RleColumnScan
    pub fn new(column: &'a RleColumn<T>) -> RleColumnScan<'a, T> {
        RleColumnScan {
            column,
            element_index: None,
            increment_index: None,
            current: None,
            lower_bound_inclusive: (0, 0),
            upper_bound_exclusive: column.get_element_and_increment_index_from_global_index(
                column.end_indices.last().map(|nzu| nzu.get()).unwrap_or(0),
            ),
        }
    }

    fn pos_for_element_and_increment_index(&self, el_idx: usize, inc_idx: usize) -> Option<usize> {
        self.are_indices_in_range(el_idx, inc_idx).then(|| {
            el_idx
                .checked_sub(1)
                .map(|el_idx| self.column.end_indices[el_idx].get())
                .unwrap_or(0)
                + inc_idx
        })
    }

    fn are_indices_in_range(&self, el_idx: usize, inc_idx: usize) -> bool {
        let (lower_el, lower_inc) = self.lower_bound_inclusive;
        let (upper_el, upper_inc) = self.upper_bound_exclusive;

        let lower_bound_ok = el_idx > lower_el || (el_idx == lower_el && inc_idx >= lower_inc);
        let upper_bound_ok = el_idx < upper_el || (el_idx == upper_el && inc_idx < upper_inc);

        lower_bound_ok && upper_bound_ok
    }

    fn is_self_in_range(&self) -> bool {
        self.element_index
            .and_then(|el_idx| {
                self.increment_index
                    .map(|inc_idx| self.are_indices_in_range(el_idx, inc_idx))
            })
            .unwrap_or(false)
    }
}

impl<'a, T> Iterator for RleColumnScan<'a, T>
where
    T: ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut element_index = self.element_index.unwrap_or(self.lower_bound_inclusive.0);
        let mut increment_index = self
            .increment_index
            .map_or(self.lower_bound_inclusive.1, |i| i + 1);

        if self.are_indices_in_range(element_index, increment_index)
            && increment_index
                >= self.column.end_indices[element_index].get()
                    - element_index
                        .checked_sub(1)
                        .map(|el_idx| self.column.end_indices[el_idx].get())
                        .unwrap_or(0)
        {
            element_index += 1;
            increment_index = 0;
        }

        self.element_index = Some(element_index);
        self.increment_index = Some(increment_index);

        self.current = self.is_self_in_range().then(|| {
            if increment_index == 0 {
                self.column.values[element_index]
            } else if let Some(current) = self.current {
                self.column.increments[element_index] + current
            } else {
                self.column.get_internal(element_index, increment_index)
            }
        });

        self.current
    }
}

impl<'a, T> ColumnScan for RleColumnScan<'a, T>
where
    T: ColumnDataType,
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
        let bin_search_element_index = self.column.values[current_element_index
            ..(if self.upper_bound_exclusive.1 > 0 {
                self.upper_bound_exclusive.0 + 1
            } else {
                self.upper_bound_exclusive.0
            })]
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

                if self
                    .pos_for_element_and_increment_index(
                        bin_search_element_index,
                        seek_increment_index,
                    )
                    .map(|pos| pos >= self.column.end_indices[bin_search_element_index].get())
                    .unwrap_or(false)
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

        self.current = self.is_self_in_range().then(|| {
            self.column.get_internal(
                self.element_index
                    .expect("This is just set a few lines above."),
                self.increment_index
                    .expect("This is just set a few lines above."),
            )
        });

        if let Some(cur) = self.current {
            if cur >= value {
                return Some(cur);
            }
        }

        self.find(|&next| next >= value)
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
    T: ColumnDataType,
{
    fn pos(&self) -> Option<usize> {
        self.pos_for_element_and_increment_index(self.element_index?, self.increment_index?)
    }

    fn narrow(&mut self, interval: Range<usize>) {
        self.lower_bound_inclusive = self
            .column
            .get_element_and_increment_index_from_global_index(interval.start);
        self.upper_bound_exclusive = self
            .column
            .get_element_and_increment_index_from_global_index(interval.end);
    }
}

#[cfg(test)]
mod test {
    use super::{Column, ColumnScan, RangedColumnScan, RleColumn, Step};
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
    #[cfg_attr(miri, ignore)]
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
    #[cfg_attr(miri, ignore)]
    fn get_with_large_index_on_inc_zero() {
        let control_data = get_control_data_with_inc_zero();
        let c: RleColumn<u8> = get_test_column_with_inc_zero();

        control_data
            .iter()
            .enumerate()
            .for_each(|(i, control_item)| assert_eq!(c.get(i), *control_item))
    }

    #[test]
    fn get_large_increment_overflowing_value_space_i64() {
        let control_data = vec![-i64::MAX, 0, i64::MAX];
        let c = RleColumn::new(control_data.clone());
        control_data
            .iter()
            .enumerate()
            .for_each(|(i, control_item)| assert_eq!(c.get(i), *control_item))
    }

    #[test]
    fn get_large_increment_overflowing_value_space_float() {
        let control_data = vec![
            Float::new(f32::MIN).unwrap(),
            Float::new(0.0).unwrap(),
            Float::new(f32::MAX).unwrap(),
        ];
        let c = RleColumn::new(control_data.clone());
        control_data
            .iter()
            .enumerate()
            .for_each(|(i, control_item)| assert_eq!(c.get(i), *control_item))
    }

    #[test]
    fn get_large_increment_overflowing_value_space_double() {
        let control_data = vec![
            Double::new(f64::MIN).unwrap(),
            Double::new(0.0).unwrap(),
            Double::new(f64::MAX).unwrap(),
        ];
        let c = RleColumn::new(control_data.clone());
        control_data
            .iter()
            .enumerate()
            .for_each(|(i, control_item)| assert_eq!(c.get(i), *control_item))
    }

    #[quickcheck]
    #[cfg_attr(miri, ignore)]
    fn get_quickcheck_u64(raw_data: Vec<u64>) -> bool {
        let col = RleColumn::new(raw_data.clone());

        raw_data
            .iter()
            .enumerate()
            .all(|(i, control_item)| col.get(i) == *control_item)
    }

    #[quickcheck]
    #[cfg_attr(miri, ignore)]
    fn get_quickcheck_float(raw_data: Vec<Float>) -> bool {
        let col = RleColumn::new(raw_data.clone());

        raw_data
            .iter()
            .enumerate()
            .all(|(i, control_item)| col.get(i) == *control_item)
    }

    #[quickcheck]
    #[cfg_attr(miri, ignore)]
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
    #[cfg_attr(miri, ignore)]
    fn iter_quickcheck_u64(raw_data: Vec<u64>) -> bool {
        let col = RleColumn::new(raw_data.clone());

        let iterated_col: Vec<u64> = col.iter().collect();
        iterated_col == raw_data
    }

    #[quickcheck]
    #[cfg_attr(miri, ignore)]
    fn iter_quickcheck_float(raw_data: Vec<Float>) -> bool {
        let col = RleColumn::new(raw_data.clone());

        let iterated_col: Vec<Float> = col.iter().collect();
        iterated_col == raw_data
    }

    #[quickcheck]
    #[cfg_attr(miri, ignore)]
    fn iter_quickcheck_double(raw_data: Vec<Double>) -> bool {
        let col = RleColumn::new(raw_data.clone());

        let iterated_col: Vec<Double> = col.iter().collect();
        iterated_col == raw_data
    }

    #[test]
    fn seek_and_get_pos() {
        let seek_test_col = RleColumn {
            values: vec![2, 22],
            end_indices: vec![NonZeroUsize::new(3).unwrap(), NonZeroUsize::new(9).unwrap()],
            increments: vec![Step::Increment(5), Step::Increment(1)],
        };

        let mut iter = seek_test_col.iter();

        assert_eq!(iter.current(), None);
        assert_eq!(iter.pos(), None);

        iter.seek(10);

        assert_eq!(iter.current().unwrap(), 12);
        assert_eq!(iter.pos().unwrap(), 2);

        iter.seek(22);

        assert_eq!(iter.current().unwrap(), 22);
        assert_eq!(iter.pos().unwrap(), 3);

        iter.seek(25);

        assert_eq!(iter.current().unwrap(), 25);
        assert_eq!(iter.pos().unwrap(), 6);

        iter.seek(28);

        assert_eq!(iter.current(), None);
        assert_eq!(iter.pos(), None);
    }

    #[test]
    fn next_and_seek_and_get_pos_on_narrowed_column() {
        let seek_test_col = RleColumn {
            values: vec![2, 22],
            end_indices: vec![NonZeroUsize::new(3).unwrap(), NonZeroUsize::new(9).unwrap()],
            increments: vec![Step::Increment(5), Step::Increment(1)],
        };

        let mut iter = seek_test_col.iter();

        assert_eq!(iter.current(), None);
        assert_eq!(iter.pos(), None);

        iter.narrow(1..6);

        assert_eq!(iter.current(), None);
        assert_eq!(iter.pos(), None);

        iter.next();

        assert_eq!(iter.current().unwrap(), 7);
        assert_eq!(iter.pos().unwrap(), 1);

        iter.seek(10);

        assert_eq!(iter.current().unwrap(), 12);
        assert_eq!(iter.pos().unwrap(), 2);

        iter.seek(22);

        assert_eq!(iter.current().unwrap(), 22);
        assert_eq!(iter.pos().unwrap(), 3);

        iter.seek(25);

        assert_eq!(iter.current(), None);
        assert_eq!(iter.pos(), None);

        iter.next();

        assert_eq!(iter.current(), None);
        assert_eq!(iter.pos(), None);
    }

    #[quickcheck]
    #[cfg_attr(miri, ignore)]
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
    #[cfg_attr(miri, ignore)]
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
    #[cfg_attr(miri, ignore)]
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
