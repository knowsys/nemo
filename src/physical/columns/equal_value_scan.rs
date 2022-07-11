use super::{ColumnScan, RangedColumnScan, RangedColumnScanCell};
use crate::physical::datatypes::{Field, FloorToUsize};
use std::{fmt::Debug, ops::Range};

/// Simple implementation of [`ColumnScan`] for an arbitrary [`Column`].
#[derive(Debug)]
pub struct EqualValueScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    scan: &'a RangedColumnScanCell<'a, T>,
    value: T,
    current_value: Option<T>,
}
impl<'a, T> EqualValueScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Constructs a new EqualValueScan for a Column.
    pub fn new(scan: &'a RangedColumnScanCell<'a, T>, value: T) -> EqualValueScan<'a, T> {
        EqualValueScan {
            scan,
            value,
            current_value: None,
        }
    }
}

impl<'a, T> Iterator for EqualValueScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_value.is_some() {
            self.current_value = None;
            return None;
        }
        let reference_value = self.value;
        let next_value_opt = self.scan.seek(reference_value);

        if let Some(next_value) = next_value_opt {
            if next_value == reference_value {
                self.current_value = next_value_opt;
            } else {
                self.current_value = None;
            }
        } else {
            self.current_value = None;
        }
        self.current_value
    }
}

impl<'a, T> ColumnScan for EqualValueScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn seek(&mut self, value: T) -> Option<T> {
        let reference_value = self.value;
        if value > reference_value {
            self.current_value = None;
            None
        } else {
            self.next()
        }
    }

    fn current(&mut self) -> Option<T> {
        self.current_value
    }

    fn reset(&mut self) {
        self.current_value = None;
    }
}

impl<'a, T> RangedColumnScan for EqualValueScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn pos(&self) -> Option<usize> {
        unimplemented!(
            "This function only exists because RangedColumnScans cannnot be ColumnScans"
        );
    }
    fn narrow(&mut self, _interval: Range<usize>) {
        unimplemented!(
            "This function only exists because RangedColumnScans cannnot be ColumnScans"
        );
    }
}

#[cfg(test)]
mod test {
    use super::EqualValueScan;
    use crate::physical::columns::{
        Column, ColumnScan, GenericColumnScanEnum, RangedColumnScanCell, RangedColumnScanEnum,
        VectorColumn,
    };
    use test_log::test;

    #[test]
    fn test_u64() {
        let col = VectorColumn::new(vec![1u64, 4, 8]);
        let col_iter = RangedColumnScanCell::new(RangedColumnScanEnum::GenericColumnScan(
            GenericColumnScanEnum::VectorColumn(col.iter()),
        ));

        let mut equal_scan = EqualValueScan::new(&col_iter, 4);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), Some(4));
        assert_eq!(equal_scan.current(), Some(4));
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);

        let col_iter = RangedColumnScanCell::new(RangedColumnScanEnum::GenericColumnScan(
            GenericColumnScanEnum::VectorColumn(col.iter()),
        ));
        let mut equal_scan = EqualValueScan::new(&col_iter, 7);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);
    }
}
