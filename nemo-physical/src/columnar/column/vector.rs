//! This module defines [ColumnVector] and [ColumnScanVector].

use std::{
    cell::UnsafeCell,
    fmt::Debug,
    mem::size_of,
    ops::{Index, Range},
};

use crate::{
    columnar::columnscan::ColumnScan, datatypes::column_data_type::DeletedValue,
    management::bytesized::ByteSized,
};

use super::Column;

/// Simple implementation of [Column] that uses Vec to store data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnVector<T> {
    data: Vec<T>,
}

impl<T: Debug + Copy + Ord> ColumnVector<T> {
    /// Constructs a new ColumnVector from a vector of the suitable type.
    pub(crate) fn new(data: Vec<T>) -> ColumnVector<T> {
        let mut data = data;
        data.shrink_to_fit();

        ColumnVector { data }
    }

    pub(crate) fn iter_mut(&self) -> ColumnScanVectorMut<T> {
        ColumnScanVectorMut::new(&self.data)
    }
}

impl<'a, T: 'a + Debug + Copy + Ord + DeletedValue> Column<'a, T> for ColumnVector<T> {
    type Scan = ColumnScanVector<'a, T>;

    fn len(&self) -> usize {
        self.data.len()
    }

    fn get(&self, index: usize) -> T {
        self.data[index]
    }

    fn iter(&'a self) -> Self::Scan {
        ColumnScanVector::new(self)
    }
}

impl<T: Debug + Copy + Ord> Index<usize> for ColumnVector<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl<T> ByteSized for ColumnVector<T> {
    fn size_bytes(&self) -> u64 {
        // cast everything to u64 separately to avoid overflows
        size_of::<Self>() as u64 + self.data.capacity() as u64 * size_of::<T>() as u64
    }
}

/// Simple implementation of [ColumnScan] for a [ColumnVector].
#[derive(Debug)]
pub(crate) struct ColumnScanVector<'a, T> {
    column: &'a ColumnVector<T>,
    pos: Option<usize>,
    interval: Range<usize>,
}

impl<'a, T> ColumnScanVector<'a, T>
where
    T: 'a + Debug + Copy + Ord + DeletedValue,
{
    /// Defines the lower limit of elements in the interval where a binary search is used instead of a vector-scan
    const SEEK_BINARY_SEARCH: usize = 10;

    /// Constructs a new [ColumnScanVector] for a Column.
    pub(crate) fn new(column: &'a ColumnVector<T>) -> Self {
        Self {
            column,
            pos: None,
            interval: 0..column.len(),
        }
    }

    fn validate_interval(&self) {
        assert!(
            self.interval.end <= self.column.len(),
            "Cannot narrow to an interval larger than the column."
        );
    }
}

impl<'a, T> Iterator for ColumnScanVector<'a, T>
where
    T: 'a + Debug + Copy + Ord + DeletedValue,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let pos = self.pos.map_or_else(|| self.interval.start, |pos| pos + 1);
            self.pos = Some(pos);
            if pos >= self.interval.end {
                return None;
            }

            let current_element = self.column.get(pos);
            if current_element != T::deleted_value() {
                return Some(current_element);
            }
        }
    }
}

impl<'a, T> ColumnScan for ColumnScanVector<'a, T>
where
    T: 'a + Debug + Copy + Ord + DeletedValue,
{
    fn seek(&mut self, value: T) -> Option<T> {
        if self.interval.end == 0 {
            return None;
        }

        let pos = self.pos.get_or_insert(self.interval.start);
        let mut lower = *pos;
        let mut upper = self.interval.end - 1;

        // check if position is out of bounds
        if *pos > upper {
            return None;
        }
        // check if value exceeds the greatest element in column
        if self.column.get(upper) < value {
            *pos = self.column.len();
            return None;
        }
        // check if lower bound is already the target value
        if self.column.get(lower) >= value {
            *pos = lower;
            return Some(self.column.get(*pos));
        }

        // do binary search till interval is small enough to be scanned
        while upper - lower >= Self::SEEK_BINARY_SEARCH {
            let mid = (lower + upper) / 2;
            if self.column.get(mid) < value {
                lower = mid + 1;
            } else {
                upper = mid;
            }
        }

        *pos = lower;
        // scan the interval
        while *pos <= upper && self.column.get(*pos) < value {
            *pos += 1;
        }
        // if it is out of bounds
        if *pos > upper {
            None
        } else {
            Some(self.column.get(*pos))
        }
    }

    fn current(&self) -> Option<T> {
        self.pos
            .and_then(|pos| (pos < self.interval.end).then(|| self.column.get(pos)))
    }

    fn reset(&mut self) {
        self.pos = None;
    }

    fn pos(&self) -> Option<usize> {
        self.pos
            .and_then(|pos| (pos < self.interval.end).then_some(pos))
    }

    fn narrow(&mut self, interval: Range<usize>) {
        self.interval = interval;
        self.pos = None;
        self.validate_interval();
    }
}

/// Mutable iterator over a [ColumnScanVector]
#[derive(Debug)]
pub(crate) struct ColumnScanVectorMut<'a, T> {
    /// The referenced column
    column: &'a [UnsafeCell<T>],
    /// Current position
    pos: Option<usize>,
    /// Range that is currently active within the column
    interval: Range<usize>,
}

impl<'a, T> ColumnScanVectorMut<'a, T> {
    pub(crate) fn new(column: &'a [T]) -> Self {
        let column_len = column.len();
        let slice = unsafe {
            let ptr: *const [T] = column;
            &*(ptr as *const [UnsafeCell<T>])
        };

        Self {
            column: slice,
            pos: None,
            interval: 0..column_len,
        }
    }

    pub(crate) fn current(&self) -> Option<&T> {
        unsafe { self.column.get(self.pos?).map(|cell| &*cell.get()) }
    }

    pub(crate) fn current_mut(&self) -> Option<&mut T> {
        unsafe { self.column.get(self.pos?).map(|cell| &mut *cell.get()) }
    }

    pub(crate) fn reset(&mut self) {
        self.pos = None;
    }

    pub(crate) fn pos(&self) -> Option<usize> {
        self.pos
    }

    pub(crate) fn narrow(&mut self, interval: Range<usize>) {
        self.interval = interval;
        self.pos = None;
    }

    pub(crate) fn next(&mut self) {
        let pos = self.pos.map_or_else(|| self.interval.start, |pos| pos + 1);
        self.pos = Some(pos);
    }
}

impl<'a, T: DeletedValue> ColumnScanVectorMut<'a, T> {
    pub(crate) fn delete(&mut self) {
        if let Some(current) = self.current_mut() {
            *current = T::deleted_value();
        }
    }
}

// impl<'a, T: Debug + Copy + Ord> Iterator for ColumnScanVectorMut<'a, T> {
//     type Item = &'a mut T;

//     fn next(&mut self) -> Option<Self::Item> {
//         let pos = self.pos.map_or_else(|| self.interval.start, |pos| pos + 1);
//         self.pos = Some(pos);
//         if pos < self.interval.end {
//             // https://users.rust-lang.org/t/how-to-implement-a-safe-mutable-iterator-for-vec-t/33297/5
//             self.column.take().and_then(|v| {
//                 let (head, tail) = v.split_first_mut()?;
//                 self.column = Some(tail);
//                 Some(head)
//             })
//         } else {
//             self.pos = None;
//             None
//         }
//     }
// }

#[cfg(test)]
mod test {
    use crate::columnar::{column::Column, columnscan::ColumnScan};

    use super::{ColumnScanVector, ColumnVector};
    use test_log::test;

    fn get_test_column() -> ColumnVector<u64> {
        let data: Vec<u64> = vec![1, 2, 5];
        ColumnVector::new(data)
    }

    fn get_test_column_large() -> ColumnVector<u64> {
        let data: Vec<u64> = vec![
            1, 2, 5, 9, 12, 14, 16, 18, 21, 25, 28, 29, 30, 35, 37, 39, 40, 45, 47, 49,
        ];
        ColumnVector::new(data)
    }

    #[test]
    fn test_u64_column() {
        let data: Vec<u64> = vec![1, 2, 3];

        let vc: ColumnVector<u64> = ColumnVector::new(data);
        assert_eq!(vc.len(), 3);
        assert_eq!(vc[0], 1);
        assert_eq!(vc[1], 2);
        assert_eq!(vc[2], 3);
    }

    #[test]
    fn u64_iterate_column() {
        let test_column = get_test_column();
        let mut gcs: ColumnScanVector<u64> = ColumnScanVector::new(&test_column);
        assert_eq!(gcs.pos(), None);
        assert_eq!(gcs.current(), None);
        assert_eq!(gcs.next(), Some(1));
        assert_eq!(gcs.current(), Some(1));
        assert_eq!(gcs.pos(), Some(0));
        assert_eq!(gcs.next(), Some(2));
        assert_eq!(gcs.current(), Some(2));
        assert_eq!(gcs.pos(), Some(1));
        assert_eq!(gcs.next(), Some(5));
        assert_eq!(gcs.current(), Some(5));
        assert_eq!(gcs.pos(), Some(2));
        assert_eq!(gcs.next(), None);
        assert_eq!(gcs.pos(), None);
        assert_eq!(gcs.current(), None);
    }

    #[test]
    fn u64_seek_column() {
        let test_column = get_test_column();
        let mut gcs: ColumnScanVector<u64> = ColumnScanVector::new(&test_column);
        assert_eq!(gcs.pos(), None);
        assert_eq!(gcs.seek(2), Some(2));
        assert_eq!(gcs.pos(), Some(1));
        assert_eq!(gcs.seek(2), Some(2));
        assert_eq!(gcs.pos(), Some(1));
        assert_eq!(gcs.seek(3), Some(5));
        assert_eq!(gcs.pos(), Some(2));
        assert_eq!(gcs.seek(6), None);
        assert_eq!(gcs.pos(), None);
        assert_eq!(gcs.seek(3), None);
        assert_eq!(gcs.pos(), None);
    }

    #[test]
    fn u64_narrow() {
        let test_column = get_test_column();
        let mut gcs = ColumnScanVector::new(&test_column);
        gcs.narrow(0..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2]);
        let mut gcs = ColumnScanVector::new(&test_column);
        gcs.narrow(1..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2]);
        let mut gcs = ColumnScanVector::new(&test_column);
        gcs.narrow(1..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2, 5]);
        let mut gcs = ColumnScanVector::new(&test_column);
        gcs.narrow(0..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
    }

    #[test]
    #[should_panic(expected = "Cannot narrow to an interval larger than the column.")]
    fn u64_narrow_to_invalid() {
        let test_column = get_test_column();
        let mut gcs = ColumnScanVector::new(&test_column);
        gcs.narrow(1..23);
    }

    #[test]
    fn u64_narrow_after_use() {
        let test_column = get_test_column();
        let mut gcs = ColumnScanVector::new(&test_column);
        assert_eq!(gcs.next(), Some(1));
        gcs.narrow(0..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn u64_seek_interval() {
        let test_column = get_test_column_large();
        let mut gcs = ColumnScanVector::new(&test_column);

        gcs.narrow(4..16);
        assert_eq!(gcs.seek(2), Some(12));
        assert_eq!(gcs.current(), Some(12));
        assert_eq!(gcs.seek(17), Some(18));
        assert_eq!(gcs.current(), Some(18));
        assert_eq!(gcs.seek(25), Some(25));
        assert_eq!(gcs.current(), Some(25));
        assert_eq!(gcs.seek(39), Some(39));
        assert_eq!(gcs.seek(40), None);
        assert_eq!(gcs.current(), None);
    }
}
