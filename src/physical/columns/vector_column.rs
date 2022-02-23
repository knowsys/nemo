use super::{Column, ColumnScan, GenericColumnScan};
use std::{fmt::Debug, ops::Index};

/// Simple implementation of [`Column`] that uses Vec to store data.
#[derive(Debug)]
pub struct VectorColumn<T> {
    data: Vec<T>,
}

impl<T: Debug + Copy + Ord> VectorColumn<T> {
    /// Constructs a new VectorColumn from a vector of the suitable type.
    pub fn new(data: Vec<T>) -> VectorColumn<T> {
        VectorColumn { data }
    }
}

impl<T: Debug + Copy + Ord> Column<T> for VectorColumn<T> {
    fn len(&self) -> usize {
        self.data.len()
    }

    fn get(&self, index: usize) -> &T {
        &self.data[index]
    }

    fn iter<'a>(&'a self) -> Box<dyn ColumnScan<Item = T> + 'a> {
        Box::new(GenericColumnScan::new(self))
    }
}

impl<T: Debug + Copy + Ord> Index<usize> for VectorColumn<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index)
    }
}

#[cfg(test)]
mod test {
    use super::{Column, VectorColumn};
    use test_env_log::test;

    #[test]
    fn test_u64_column() {
        let data: Vec<u64> = vec![1, 2, 3];

        let vc: VectorColumn<u64> = VectorColumn::new(data);
        assert_eq!(vc.len(), 3);
        assert_eq!(vc[0], 1);
        assert_eq!(vc[1], 2);
        assert_eq!(vc[2], 3);
    }
}
