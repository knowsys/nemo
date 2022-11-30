use std::{fmt::Debug, ops::Index};

use crate::physical::columnar::columnscans::ColumnScanGeneric;

use super::Column;

/// Simple implementation of [`Column`] that uses Vec to store data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnVector<T> {
    data: Vec<T>,
}

impl<T: Debug + Copy + Ord> ColumnVector<T> {
    /// Constructs a new ColumnVector from a vector of the suitable type.
    pub fn new(data: Vec<T>) -> ColumnVector<T> {
        ColumnVector { data }
    }
}

impl<'a, T: 'a + Debug + Copy + Ord> Column<'a, T> for ColumnVector<T> {
    type Scan = ColumnScanGeneric<'a, T, ColumnVector<T>>;

    fn len(&self) -> usize {
        self.data.len()
    }

    fn get(&self, index: usize) -> T {
        self.data[index]
    }

    fn iter(&'a self) -> Self::Scan {
        ColumnScanGeneric::new(self)
    }
}

impl<T: Debug + Copy + Ord> Index<usize> for ColumnVector<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

#[cfg(test)]
mod test {
    use super::{Column, ColumnVector};
    use test_log::test;

    #[test]
    fn test_u64_column() {
        let data: Vec<u64> = vec![1, 2, 3];

        let vc: ColumnVector<u64> = ColumnVector::new(data);
        assert_eq!(vc.len(), 3);
        assert_eq!(vc[0], 1);
        assert_eq!(vc[1], 2);
        assert_eq!(vc[2], 3);
    }
}
