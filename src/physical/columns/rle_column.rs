use super::{Column, ColumnBuilder, ColumnScan, RangedColumnScan};
use num::Zero;
use std::{
    fmt::Debug,
    iter::{repeat, Sum},
    num::NonZeroUsize,
    ops::{Add, Mul, Range, Sub},
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
    T: Copy + Ord + Add<Output = T> + Sub<Output = T>,
{
    fn from_two_values(previous_value: T, next_value: T) -> Self {
        if next_value < previous_value {
            Self::Decrement(previous_value - next_value)
        } else {
            Self::Increment(next_value - previous_value)
        }
    }
}

impl<T> Add<T> for Step<T>
where
    T: Add<Output = T> + Sub<Output = T>,
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
    T: Ord + Add<Output = T> + Sub<Output = T>,
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
    T: Copy + TryFrom<usize> + Sum + Mul<Output = T>,
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
    T: Zero + Ord + Sub<Output = T>,
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

impl<T> RleElement<T>
where
    T: Copy
        + Ord
        + TryFrom<usize>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Sum
        + Zero,
{
    fn get(&self, index: usize) -> T {
        if index >= self.length.get() {
            panic!("IndexOutOfBounds in rle value computation.");
        }

        if self.increment.is_zero() {
            return self.value;
        }

        let total_increment = self.increment * index;

        total_increment + self.value
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

impl<T> RleColumnBuilder<T>
where
    T: Debug
        + Copy
        + Ord
        + TryFrom<usize>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Default
        + Sum
        + Zero,
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
    T: 'a
        + Copy
        + Ord
        + TryFrom<usize>
        + Debug
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Default
        + Sum
        + Zero,
{
    fn add(&mut self, value: T) {
        let current_value = value;

        if self.elements.is_empty() {
            self.elements.push(RleElement {
                value: current_value,
                length: NonZeroUsize::new(1).unwrap(),
                increment: Default::default(),
            });

            self.previous_value_opt = Some(current_value);

            return;
        }

        let previous_value = self.previous_value_opt.unwrap();
        let last_element = self.elements.last_mut().unwrap();
        let current_increment = Step::from_two_values(previous_value, current_value);

        if last_element.length == NonZeroUsize::new(1).unwrap() {
            last_element.length = NonZeroUsize::new(2).unwrap();
            last_element.increment = current_increment;
        } else if last_element.increment == current_increment {
            last_element.length = NonZeroUsize::new(last_element.length.get() + 1).unwrap();
        } else {
            self.elements.push(RleElement {
                value: current_value,
                length: NonZeroUsize::new(1).unwrap(),
                increment: Default::default(),
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

impl<T> RleColumn<T>
where
    T: Debug
        + Copy
        + Ord
        + TryFrom<usize>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Default
        + Sum
        + Zero,
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

impl<T> Column<T> for RleColumn<T>
where
    T: Debug
        + Copy
        + Ord
        + TryFrom<usize>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Sum
        + Zero,
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
    T: Copy + Ord + TryFrom<usize> + Add<Output = T> + Sub<Output = T> + Mul<Output = T>,
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

                self.column.elements[element_index].increment + current
            }
        });

        self.current
    }
}

impl<'a, T> ColumnScan for RleColumnScan<'a, T>
where
    T: Debug + Copy + Ord + TryFrom<usize> + Add<Output = T> + Sub<Output = T> + Mul<Output = T>,
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

impl<'a, T> RangedColumnScan for RleColumnScan<'a, T>
where
    T: Debug + Copy + Ord + TryFrom<usize> + Add<Output = T> + Sub<Output = T> + Mul<Output = T>,
{
    fn pos(&self) -> Option<usize> {
        self.element_index
    }

    fn narrow(&mut self, _interval: Range<usize>) {
        unimplemented!("RleColumnScan does not support intervals for now");
    }
}

#[cfg(test)]
mod test {
    use super::{Column, RleColumn, RleElement, Step};
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
                    increment: Step::Increment(3),
                },
                RleElement {
                    value: 6,
                    length: NonZeroUsize::new(3).unwrap(),
                    increment: Step::Increment(1),
                },
                RleElement {
                    value: 42,
                    length: NonZeroUsize::new(2).unwrap(),
                    increment: Step::Decrement(38),
                },
                RleElement {
                    value: 7,
                    length: NonZeroUsize::new(4).unwrap(),
                    increment: Step::Increment(3),
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
                    increment: Step::Increment(3),
                },
                RleElement {
                    value: 6,
                    length: NonZeroUsize::new(3).unwrap(),
                    increment: Step::Increment(1),
                },
                RleElement {
                    value: 42,
                    length: NonZeroUsize::new(2).unwrap(),
                    increment: Step::Decrement(38),
                },
                RleElement {
                    value: 7,
                    length: NonZeroUsize::new(4).unwrap(),
                    increment: Step::Increment(3),
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
                increment: Step::Increment(0),
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
