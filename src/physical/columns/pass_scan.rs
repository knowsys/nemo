use super::{ColumnScan, RangedColumnScan, RangedColumnScanCell};
use crate::physical::datatypes::{Field, FloorToUsize};
use std::{fmt::Debug, ops::Range};

/// Simple implementation of [`ColumnScan`] for an arbitrary [`Column`].
#[derive(Debug)]
pub struct PassScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    reference_scan: &'a RangedColumnScanCell<'a, T>,
}
impl<'a, T> PassScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Constructs a new PassScan for a Column.
    pub fn new(reference_scan: &'a RangedColumnScanCell<'a, T>) -> PassScan<'a, T> {
        PassScan { reference_scan }
    }
}

impl<'a, T> Iterator for PassScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field + PartialOrd + Eq,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.reference_scan.next()
    }
}

impl<'a, T> ColumnScan for PassScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field + PartialOrd + Eq,
{
    fn seek(&mut self, value: T) -> Option<T> {
        self.reference_scan.seek(value)
    }

    fn current(&mut self) -> Option<T> {
        self.reference_scan.current()
    }

    fn reset(&mut self) {
        self.reference_scan.reset()
    }
}

impl<'a, T> RangedColumnScan for PassScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field + PartialOrd + Eq,
{
    fn pos(&self) -> Option<usize> {
        self.reference_scan.pos()
    }
    fn narrow(&mut self, interval: Range<usize>) {
        self.reference_scan.narrow(interval);
    }
}

#[cfg(test)]
mod test {
    use super::PassScan;
    use crate::physical::columns::{
        Column, ColumnScan, GenericColumnScanEnum, RangedColumnScanCell, RangedColumnScanEnum,
        VectorColumn,
    };
    use test_log::test;

    #[test]
    fn test_u64() {
        let ref_col = VectorColumn::new(vec![0, 4, 7]);
        let mut ref_col_iter = RangedColumnScanCell::new(RangedColumnScanEnum::GenericColumnScan(
            GenericColumnScanEnum::VectorColumn(ref_col.iter()),
        ));

        let mut pass_scan = PassScan::new(&mut ref_col_iter);

        assert_eq!(pass_scan.current(), None);
        assert_eq!(pass_scan.next(), Some(0));
        assert_eq!(pass_scan.current(), Some(0));
        assert_eq!(pass_scan.seek(6), Some(7));
        assert_eq!(pass_scan.current(), Some(7));
        assert_eq!(pass_scan.next(), None);
        assert_eq!(pass_scan.current(), None);
    }
}
