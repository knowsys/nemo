use super::{columnscan::ColumnScan, ColumnScanCell};
use crate::physical::datatypes::ColumnDataType;
use std::{fmt::Debug, ops::Range};

/// Iterator which allows its sub iterator to only jump to a certain value
#[derive(Debug)]
pub struct ColumnScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    scan: &'a ColumnScanCell<'a, T>,
    value: T,
    current_value: Option<T>,
}
impl<'a, T> ColumnScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new ColumnScanEqualValue for a Column.
    pub fn new(scan: &'a ColumnScanCell<'a, T>, value: T) -> ColumnScanEqualValue<'a, T> {
        ColumnScanEqualValue {
            scan,
            value,
            current_value: None,
        }
    }
}

impl<'a, T> Iterator for ColumnScanEqualValue<'a, T>
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

impl<'a, T> ColumnScan for ColumnScanEqualValue<'a, T>
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
    use crate::physical::columnar::{
        columns::{Column, ColumnVector},
        columnscans::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanGenericEnum},
    };

    use super::ColumnScanEqualValue;

    use test_log::test;

    #[test]
    fn test_u64() {
        let col = ColumnVector::new(vec![1u64, 4, 8]);
        let col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanGeneric(
            ColumnScanGenericEnum::ColumnVector(col.iter()),
        ));

        let mut equal_scan = ColumnScanEqualValue::new(&col_iter, 4);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), Some(4));
        assert_eq!(equal_scan.current(), Some(4));
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);

        let col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanGeneric(
            ColumnScanGenericEnum::ColumnVector(col.iter()),
        ));
        let mut equal_scan = ColumnScanEqualValue::new(&col_iter, 7);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);
    }
}
