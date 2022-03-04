use super::{Column, ColumnScan};
use std::{
    fmt::Debug,
    num::NonZeroUsize,
    ops::{Add, Mul, Sub},
};

// TODO: is it useful to have I as extra type parameter? (guess it's hard to use...)
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
        // TODO: better error handling
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

        let t_result = T::try_from(i_result)
            .or(Err(
                "should not happen if construction of RleColumn works correctly",
            ))
            .unwrap();

        t_result
    }
}

/// Implementation of [`Column`] that allows the use of incremental run length encoding.
#[derive(Debug, PartialEq)]
pub struct RleColumn<T, I = i64> {
    elements: Vec<RleElement<T, I>>,
}

const MINIMUM_RLE_ELEMENT_LENGTH: usize = 4;

impl<
        T: Copy + TryFrom<I>,
        I: Copy
            + TryFrom<T>
            + TryFrom<usize>
            + Add<Output = I>
            + Sub<Output = I>
            + Mul<Output = I>
            + PartialEq
            + Default,
    > RleColumn<T, I>
{
    /// Constructs a new RleColumn from a vector of the suitable type.
    pub fn new(data: Vec<T>) -> RleColumn<T, I> {
        let mut rle_element_candidate_start: usize = 0;
        let mut rle_element_candidate_length: NonZeroUsize = NonZeroUsize::new(1).unwrap();

        let mut rle_elements: Vec<RleElement<T, I>> = vec![];

        let mut previous_increment_opt: Option<I> = None;

        for index in 1..data.len() {
            let current_element = data[index];
            let previous_element = data[index - 1];

            // TODO: better error handling
            let current_increment = I::try_from(current_element)
                .or(Err("overflow when building RleColumn"))
                .unwrap()
                - I::try_from(previous_element)
                    .or(Err("overflow when building RleColumn"))
                    .unwrap();
            previous_increment_opt = (index >= 2).then(|| {
                I::try_from(previous_element)
                    .or(Err("overflow when building RleColumn"))
                    .unwrap()
                    - I::try_from(data[index - 2])
                        .or(Err("overflow when building RleColumn"))
                        .unwrap()
            });

            // we want to add the current item to the rle_element_candidate if the increment stays
            // the same or if we just started a new candidate
            // (previous_increment_opt is None in this case)
            let should_add_to_current_candidate = previous_increment_opt
                .map_or(true, |previous_increment| {
                    current_increment == previous_increment
                });

            if should_add_to_current_candidate {
                rle_element_candidate_length =
                    NonZeroUsize::new(rle_element_candidate_length.get() + 1).unwrap();
            } else {
                // if the current candidate is finished, we transform it
                // into one or multiple rle elements
                if rle_element_candidate_length.get() >= MINIMUM_RLE_ELEMENT_LENGTH {
                    // this is done again after the loop
                    rle_elements.push(RleElement {
                        value: data[rle_element_candidate_start],
                        length: rle_element_candidate_length,
                        increment: previous_increment_opt.unwrap(), // we know here that this exists
                    });
                    rle_element_candidate_start = index;
                    rle_element_candidate_length = NonZeroUsize::new(1).unwrap();
                } else if index > 0 {
                    for value in data
                        .iter()
                        .skip(rle_element_candidate_start)
                        .take(rle_element_candidate_length.get() - 1)
                    {
                        rle_elements.push(RleElement {
                            value: *value,
                            length: NonZeroUsize::new(1).unwrap(),
                            increment: Default::default(),
                        })
                    }
                    rle_element_candidate_start = index - 1;
                    rle_element_candidate_length = NonZeroUsize::new(2).unwrap();
                }
            }
        }

        // add last candidate to elements
        rle_elements.push(RleElement {
            value: data[rle_element_candidate_start],
            length: rle_element_candidate_length,
            increment: previous_increment_opt.unwrap_or_default(), // will be None iff rle_element_candidate_length is 1
        });

        RleColumn {
            elements: rle_elements,
        }
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
                    length: NonZeroUsize::new(1).unwrap(),
                    increment: 0,
                },
                RleElement {
                    value: 5,
                    length: NonZeroUsize::new(4).unwrap(),
                    increment: 1,
                },
                RleElement {
                    value: 42,
                    length: NonZeroUsize::new(1).unwrap(),
                    increment: 0,
                },
                RleElement {
                    value: 4,
                    length: NonZeroUsize::new(5).unwrap(),
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
                    length: NonZeroUsize::new(1).unwrap(),
                    increment: 0,
                },
                RleElement {
                    value: 5,
                    length: NonZeroUsize::new(4).unwrap(),
                    increment: 1,
                },
                RleElement {
                    value: 42,
                    length: NonZeroUsize::new(1).unwrap(),
                    increment: 0,
                },
                RleElement {
                    value: 4,
                    length: NonZeroUsize::new(5).unwrap(),
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
        assert_eq!(c.is_empty(), false);

        let c_empty: RleColumn<i64> = RleColumn { elements: vec![] };
        assert_eq!(c_empty.is_empty(), true);
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

    // TODO: extra tests for column scan?
}
