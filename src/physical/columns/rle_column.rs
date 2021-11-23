use super::{Column, ColumnScan};
use std::{
    fmt::Debug,
    num::NonZeroUsize,
    ops::{Add, Mul},
};

#[derive(Debug, PartialEq)]
struct RleElement<T> {
    value: T,
    length: NonZeroUsize,
    increment: T,
}

impl<T: Add<Output = T> + Mul<Output = T> + TryFrom<usize> + Debug + Copy> RleElement<T> {
    fn get(&self, index: usize) -> T {
        self.value
            + self.increment
                * T::try_from(index)
                    .or(Err(
                        "should not happen if construction of RleColumn works correctly",
                    ))
                    .unwrap()
    }
}

/// Implementation of [`Column`] that allows the use of incremental run length encoding.
#[derive(Debug, PartialEq)]
pub struct RleColumn<T> {
    elements: Vec<RleElement<T>>,
}

impl<T: Add<Output = T> + Debug + Ord + Copy + Default> RleColumn<T> {
    /// Constructs a new RleColumn from a vector of the suitable type.
    pub fn new(data: Vec<T>) -> RleColumn<T> {
        // TODO: actually use RLE
        RleColumn {
            elements: data
                .iter()
                .map(|e| RleElement {
                    value: *e,
                    length: NonZeroUsize::new(1).unwrap(),
                    increment: Default::default(),
                })
                .collect(),
        }
    }
}

impl<T: Add<Output = T> + Mul<Output = T> + TryFrom<usize> + Debug + Ord + Copy> Column<T>
    for RleColumn<T>
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

impl<'a, T: Add<Output = T> + Ord + Copy> RleColumnScan<'a, T> {
    pub fn new(column: &'a RleColumn<T>) -> RleColumnScan<'a, T> {
        RleColumnScan {
            column,
            element_index: None,
            increment_index: None,
            current: None,
        }
    }
}

impl<'a, T: Add<Output = T> + Ord + Copy> Iterator for RleColumnScan<'a, T> {
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

impl<'a, T: Add<Output = T> + Debug + Ord + Copy> ColumnScan for RleColumnScan<'a, T> {
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

    fn get_control_data() -> Vec<u64> {
        vec![2, 5, 6, 7, 8, 42, 4, 7, 10, 13, 16]
    }

    fn get_test_column() -> RleColumn<u64> {
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
        let expected: RleColumn<u64> = get_test_column();

        let constructed = RleColumn::new(raw_data);
        assert_eq!(expected, constructed);
    }

    #[test]
    fn is_empty() {
        let c: RleColumn<u64> = get_test_column();
        assert_eq!(c.is_empty(), false);

        let c_empty: RleColumn<u64> = RleColumn { elements: vec![] };
        assert_eq!(c_empty.is_empty(), true);
    }

    #[test]
    fn len() {
        let control_data = get_control_data();
        let c: RleColumn<u64> = get_test_column();
        assert_eq!(c.len(), control_data.len());
    }

    #[test]
    fn get() {
        let control_data = get_control_data();
        let c: RleColumn<u64> = get_test_column();

        for i in 0..8 {
            assert_eq!(c.get(i), control_data[i]);
        }
    }

    #[test]
    fn iter() {
        let control_data: Vec<u64> = get_control_data();
        let c: RleColumn<u64> = get_test_column();

        let iterated_c: Vec<u64> = c.iter().collect();
        assert_eq!(iterated_c, control_data);
    }

    // TODO: tests for column scan; we should split this file up anyway...
}
