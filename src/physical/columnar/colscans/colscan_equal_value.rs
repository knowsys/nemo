use super::{colscan::ColScan, ColScanCell};
use crate::physical::datatypes::ColumnDataType;
use std::{fmt::Debug, ops::Range};

/// Iterator which allows its sub iterator to only jump to a certain value
#[derive(Debug)]
pub struct ColScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    scan: &'a ColScanCell<'a, T>,
    value: T,
    current_value: Option<T>,
}
impl<'a, T> ColScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new ColScanEqualValue for a Column.
    pub fn new(scan: &'a ColScanCell<'a, T>, value: T) -> ColScanEqualValue<'a, T> {
        ColScanEqualValue {
            scan,
            value,
            current_value: None,
        }
    }
}

impl<'a, T> Iterator for ColScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_value.is_some() {
            self.current_value = None;
            return None;
        }
        let reference_value = self.value;
        let next_value_opt = self.scan.seek(reference_value);

        self.current_value = next_value_opt.and_then(|next_value| {
            if next_value == reference_value {
                next_value_opt
            } else {
                None
            }
        });

        self.current_value
    }
}

impl<'a, T> ColScan for ColScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
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
        colscans::{ColScan, ColScanCell, ColScanEnum, ColScanGenericEnum},
        columns::{Column, ColumnVector},
    };

    use super::ColScanEqualValue;

    use test_log::test;

    #[test]
    fn test_u64() {
        let col = ColumnVector::new(vec![1u64, 4, 8]);
        let col_iter = ColScanCell::new(ColScanEnum::ColScanGeneric(
            ColScanGenericEnum::ColumnVector(col.iter()),
        ));

        let mut equal_scan = ColScanEqualValue::new(&col_iter, 4);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), Some(4));
        assert_eq!(equal_scan.current(), Some(4));
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);

        let col_iter = ColScanCell::new(ColScanEnum::ColScanGeneric(
            ColScanGenericEnum::ColumnVector(col.iter()),
        ));
        let mut equal_scan = ColScanEqualValue::new(&col_iter, 7);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);
    }
}
