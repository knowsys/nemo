use super::{ColumnScan, RangedColumnScan};
use std::{fmt::Debug, ops::Range};

/// Simple implementation of [`ColumnScan`] for an arbitrary [`Column`].
#[derive(Debug)]
pub struct EqualColumnScan<'a, T, Scan: RangedColumnScan<Item = T>> {
    reference_scan: &'a mut Scan,
    value_scan: &'a mut Scan,
    current_value: Option<T>,
}
impl<'a, T, Scan: RangedColumnScan<Item = T>> EqualColumnScan<'a, T, Scan> {
    /// Constructs a new EqualColumnScan for a Column.
    pub fn new(
        reference_scan: &'a mut Scan,
        value_scan: &'a mut Scan,
    ) -> EqualColumnScan<'a, T, Scan> {
        EqualColumnScan {
            reference_scan,
            value_scan,
            current_value: None,
        }
    }
}

impl<'a, T: Eq + Debug + Copy + PartialOrd, Scan: RangedColumnScan<Item = T>> Iterator
    for EqualColumnScan<'a, T, Scan>
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

impl<'a, T: Ord + Copy + Debug + PartialOrd, Scan: RangedColumnScan<Item = T>> ColumnScan
    for EqualColumnScan<'a, T, Scan>
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
}

impl<'a, T: Ord + Copy + Debug, Scan: RangedColumnScan<Item = T>> RangedColumnScan
    for EqualColumnScan<'a, T, Scan>
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
    use super::EqualColumnScan;
    use crate::physical::columns::{Column, ColumnScan, VectorColumn};
    use test_log::test;

    #[test]
    fn test_u64() {
        let ref_col = VectorColumn::new(vec![0, 4, 7]);
        let val_col = VectorColumn::new(vec![1, 4, 8]);

        let mut ref_iter = ref_col.iter();
        let mut val_iter = val_col.iter();

        ref_iter.seek(4);

        let mut equal_scan = EqualColumnScan::new(&mut ref_iter, &mut val_iter);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), Some(4));
        assert_eq!(equal_scan.current(), Some(4));
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);

        let mut ref_iter = ref_col.iter();
        ref_iter.seek(7);

        let mut equal_scan = EqualColumnScan::new(&mut ref_iter, &mut val_iter);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);
    }
}
