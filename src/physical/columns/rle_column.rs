use super::{Column, ColumnScan};
use std::{
    fmt::Debug,
    num::NonZeroUsize,
    ops::{Add, Mul, Sub},
};

// TODO: is it useful to have I as extra type parameter? (guess it's hard to use...)
#[derive(Debug, PartialEq)]
struct RleElement<T, I = T> {
    value: T,
    length: NonZeroUsize,
    increment: I,
}

impl<T: Add<I, Output = T> + Debug + Copy, I: TryFrom<usize> + Mul<Output = I> + Copy>
    RleElement<T, I>
{
    fn get(&self, index: usize) -> T {
        self.value
            + self.increment
                * I::try_from(index)
                    .or(Err(
                        "should not happen if construction of RleColumn works correctly",
                    ))
                    .unwrap()
    }
}

/// Implementation of [`Column`] that allows the use of incremental run length encoding.
#[derive(Debug, PartialEq)]
pub struct RleColumn<T, I = T> {
    elements: Vec<RleElement<T, I>>,
}

impl<
        T: Add<I, Output = T> + Sub<T, Output = I> + Debug + Ord + Copy + Default,
        I: Mul<Output = I> + PartialEq + Default + Copy,
    > RleColumn<T, I>
{
    /// Constructs a new RleColumn from a vector of the suitable type.
    pub fn new(data: Vec<T>) -> RleColumn<T, I> {
        let mut previous_increment: Option<I> = None;
        let mut current_increment: Option<I> = None;
        let mut previous_element: Option<T> = None;

        let mut rle_element_candidate: Vec<T> = vec![]; //TODO: could be vector slice probably
        let mut rle_elements: Vec<RleElement<T, I>> = vec![];

        for current_element in data {
            if let Some(prev) = previous_element {
                // FIXME: this may overflow depending on T and I
                current_increment = Some(current_element - prev);
            }

            // FIXME: this structure of conditionals is prone to errors...
            let push_current_element_to_candidate: bool = match current_increment {
                None => false,
                Some(cur_inc) => match previous_increment {
                    None => true,
                    Some(prev_inc) => cur_inc == prev_inc,
                },
            };

            if push_current_element_to_candidate {
                rle_element_candidate.push(current_element);
                previous_increment = current_increment;
            } else {
                // TODO: all of this is done after loop again; unify!

                // TODO: get rid of magic number
                if rle_element_candidate.len() >= 4 {
                    rle_elements.push(RleElement {
                        value: rle_element_candidate[0],
                        length: NonZeroUsize::new(rle_element_candidate.len()).unwrap(), // we know that this is not 0
                        increment: previous_increment.unwrap(), // we know here that this exists
                    });
                    rle_element_candidate = vec![current_element];
                } else if !rle_element_candidate.is_empty() {
                    for candidate_element in &rle_element_candidate[..rle_element_candidate.len() - 1] {
                        rle_elements.push(RleElement {
                            value: *candidate_element,
                            length: NonZeroUsize::new(1).unwrap(),
                            increment: Default::default(),
                        })
                    }
                    rle_element_candidate = vec![rle_element_candidate[rle_element_candidate.len() - 1], current_element];
                } else {
                    rle_element_candidate = vec![current_element];
                }
                previous_increment = None;
            }

            previous_element = Some(current_element);
        }

        // TODO: all of this is done in loop as well; unify!

        // TODO: get rid of magic number
        if rle_element_candidate.len() >= 4 {
            rle_elements.push(RleElement {
                value: rle_element_candidate[0],
                length: NonZeroUsize::new(rle_element_candidate.len()).unwrap(), // we know that this is not 0
                increment: previous_increment.unwrap(), // we know here that this exists
            })
        } else {
            for candidate_element in rle_element_candidate {
                rle_elements.push(RleElement {
                    value: candidate_element,
                    length: NonZeroUsize::new(1).unwrap(),
                    increment: Default::default(),
                })
            }
        }

        RleColumn {
            elements: rle_elements,
        }

        // TODO: actually use RLE
        //RleColumn {
        //elements: data
        //.iter()
        //.map(|e| RleElement {
        //value: *e,
        //length: NonZeroUsize::new(1).unwrap(),
        //increment: Default::default(),
        //})
        //.collect(),
        //}
    }
}

impl<
        T: Add<I, Output = T> + Debug + Ord + Copy,
        I: TryFrom<usize> + Mul<Output = I> + Debug + Copy,
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

impl<'a, T: Add<I, Output = T> + Ord + Copy, I: Mul<Output = I>> RleColumnScan<'a, T, I> {
    pub fn new(column: &'a RleColumn<T, I>) -> RleColumnScan<'a, T, I> {
        RleColumnScan {
            column,
            element_index: None,
            increment_index: None,
            current: None,
        }
    }
}

impl<'a, T: Add<I, Output = T> + Ord + Copy, I: Mul<Output = I> + Copy> Iterator
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
                self.current.unwrap() + self.column.elements[element_index].increment
            }
        });

        self.current
    }
}

impl<'a, T: Add<I, Output = T> + Debug + Ord + Copy, I: Mul<Output = I> + Debug + Copy> ColumnScan
    for RleColumnScan<'a, T, I>
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
    use test_env_log::test;

    fn get_control_data() -> Vec<i64> {
        vec![2, 5, 6, 7, 8, 42, 4, 7, 10, 13, 16]
    }

    fn get_test_column() -> RleColumn<i64> {
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
    fn construction() {
        let raw_data = get_control_data();
        let expected: RleColumn<i64> = get_test_column();

        let constructed: RleColumn<i64> = RleColumn::new(raw_data);
        assert_eq!(expected, constructed);
    }

    #[test]
    fn is_empty() {
        let c: RleColumn<i64> = get_test_column();
        assert_eq!(c.is_empty(), false);

        let c_empty: RleColumn<i64> = RleColumn { elements: vec![] };
        assert_eq!(c_empty.is_empty(), true);
    }

    #[test]
    fn len() {
        let control_data = get_control_data();
        let c: RleColumn<i64> = get_test_column();
        assert_eq!(c.len(), control_data.len());
    }

    #[test]
    fn get() {
        let control_data = get_control_data();
        let c: RleColumn<i64> = get_test_column();

        for i in 0..control_data.len() {
            assert_eq!(c.get(i), control_data[i]);
        }
    }

    #[test]
    fn iter() {
        let control_data: Vec<i64> = get_control_data();
        let c: RleColumn<i64> = get_test_column();

        let iterated_c: Vec<i64> = c.iter().collect();
        assert_eq!(iterated_c, control_data);
    }

    // TODO: tests for column scan; we should split this file up anyway...
}
