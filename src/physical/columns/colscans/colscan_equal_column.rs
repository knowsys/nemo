use super::{colscan::ColScan, ColScanCell};
use crate::physical::datatypes::ColumnDataType;
use std::{fmt::Debug, ops::Range};

/// Iterator which allows its sub iterator to only jump to the value pointed to by a reference iterator
#[derive(Debug)]
pub struct EqualColumnScan<'a, T>
where
    T: 'a + ColumnDataType,
{
    reference_scan: &'a ColScanCell<'a, T>,
    value_scan: &'a ColScanCell<'a, T>,
    current_value: Option<T>,
}
impl<'a, T> EqualColumnScan<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new EqualColumnScan for a Column.
    pub fn new(
        reference_scan: &'a ColScanCell<'a, T>,
        value_scan: &'a ColScanCell<'a, T>,
    ) -> EqualColumnScan<'a, T> {
        EqualColumnScan {
            reference_scan,
            value_scan,
            current_value: None,
        }
    }
}

impl<'a, T> Iterator for EqualColumnScan<'a, T>
where
    T: 'a + ColumnDataType + Eq,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_value.is_some() {
            self.current_value = None;
            return None;
        }
        let reference_value = self.reference_scan.current()?;
        let next_value_opt = self.value_scan.seek(reference_value);

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

impl<'a, T> ColScan for EqualColumnScan<'a, T>
where
    T: 'a + ColumnDataType + Eq,
{
    fn seek(&mut self, value: T) -> Option<T> {
        let reference_value = self.reference_scan.current()?;
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

    fn pos(&self) -> Option<usize> {
        unimplemented!("This functions is not implemented for column operators");
    }
    fn narrow(&mut self, _interval: Range<usize>) {
        unimplemented!("This functions is not implemented for column operators");
    }
}

#[cfg(test)]
mod test {
    use crate::physical::columns::{
        colscans::{ColScan, ColScanCell, ColScanEnum, GenericColumnScanEnum},
        columns::{Column, VectorColumn},
    };

    use super::EqualColumnScan;
    use test_log::test;

    #[test]
    fn test_u64() {
        let ref_col = VectorColumn::new(vec![0u64, 4, 7]);
        let val_col = VectorColumn::new(vec![1u64, 4, 8]);

        let ref_iter = ColScanCell::new(ColScanEnum::GenericColumnScan(
            GenericColumnScanEnum::VectorColumn(ref_col.iter()),
        ));
        let val_iter = ColScanCell::new(ColScanEnum::GenericColumnScan(
            GenericColumnScanEnum::VectorColumn(val_col.iter()),
        ));

        ref_iter.seek(4);

        let mut equal_scan = EqualColumnScan::new(&ref_iter, &val_iter);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), Some(4));
        assert_eq!(equal_scan.current(), Some(4));
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);

        let ref_iter = ColScanCell::new(ColScanEnum::GenericColumnScan(
            GenericColumnScanEnum::VectorColumn(ref_col.iter()),
        ));
        let val_iter = ColScanCell::new(ColScanEnum::GenericColumnScan(
            GenericColumnScanEnum::VectorColumn(val_col.iter()),
        ));

        ref_iter.seek(7);

        let mut equal_scan = EqualColumnScan::new(&ref_iter, &val_iter);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);
    }
}
