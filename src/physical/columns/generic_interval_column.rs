use super::{Column, ColumnEnum, GenericColumnScan, IntervalColumn, RleColumn, VectorColumn};
use crate::generate_forwarder;
use crate::physical::datatypes::{Field, FloorToUsize};
use std::marker::PhantomData;
use std::{fmt::Debug, ops::Range};

/// Simple implementation of [`IntervalColumn`] that uses a second column to manage interval bounds.
#[derive(Debug)]
pub struct GenericIntervalColumn<'a, T, Col: Column<'a, T>, Starts: Column<'a, usize>> {
    _at: &'a PhantomData<T>,
    data: Col,
    int_starts: Starts,
}

/// Enum encapsulating implementations of GenericIntervalColumns
#[derive(Debug)]
pub enum GenericIntervalColumnEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Case VectorColumn with VectorColumn representing start indices
    VectorColumnWithVecStarts(GenericIntervalColumn<'a, T, VectorColumn<T>, VectorColumn<usize>>),
    /// Case VectorColumn with RleColumn representing start indices
    VectorColumnWithRleStarts(GenericIntervalColumn<'a, T, VectorColumn<T>, RleColumn<usize>>),
    /// Case VectorColumn with VectorColumn representing start indices
    RleColumnWithVecStarts(GenericIntervalColumn<'a, T, RleColumn<T>, VectorColumn<usize>>),
    /// Case RleColumn with RleColumn representing start indices
    RleColumnWithRleStarts(GenericIntervalColumn<'a, T, RleColumn<T>, RleColumn<usize>>),
    /// Case ColumnEnum with VectorColumn representing start indices
    ColumnEnumWithVecStarts(GenericIntervalColumn<'a, T, ColumnEnum<T>, VectorColumn<usize>>),
    /// Case ColumnEnum with RleColumn representing start indices
    ColumnEnumWithRleStarts(GenericIntervalColumn<'a, T, ColumnEnum<T>, RleColumn<usize>>),
}

generate_forwarder!(forward_to_interval_column;
                    VectorColumnWithVecStarts,
                    VectorColumnWithRleStarts,
                    RleColumnWithVecStarts,
                    RleColumnWithRleStarts,
                    ColumnEnumWithVecStarts,
                    ColumnEnumWithRleStarts);

impl<'a, T, Col: Column<'a, T>, Starts: Column<'a, usize>>
    GenericIntervalColumn<'a, T, Col, Starts>
{
    /// Constructs a new VectorColumn from a vector of the suitable type.
    pub fn new(data: Col, int_starts: Starts) -> GenericIntervalColumn<'a, T, Col, Starts> {
        GenericIntervalColumn {
            _at: &PhantomData,
            data,
            int_starts,
        }
    }
}

impl<'a, T: 'a + Debug + Copy + Ord, Col: 'a + Column<'a, T>, Starts: 'a + Column<'a, usize>>
    Column<'a, T> for GenericIntervalColumn<'a, T, Col, Starts>
{
    type ColScan = GenericColumnScan<'a, T, GenericIntervalColumn<'a, T, Col, Starts>>;

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn get(&self, index: usize) -> T {
        self.data.get(index)
    }

    fn iter(&'a self) -> Self::ColScan {
        GenericColumnScan::new(self)
    }
}

impl<'a, T: 'a + Debug + Copy + Ord, Col: 'a + Column<'a, T>, Starts: 'a + Column<'a, usize>>
    IntervalColumn<'a, T> for GenericIntervalColumn<'a, T, Col, Starts>
{
    fn int_len(&self) -> usize {
        self.int_starts.len()
    }

    fn int_bounds(&self, int_idx: usize) -> Range<usize> {
        let start_idx = self.int_starts.get(int_idx);
        if int_idx + 1 < self.int_starts.len() {
            start_idx..self.int_starts.get(int_idx + 1)
        } else {
            start_idx..self.data.len()
        }
    }
}

impl<'a, T> Column<'a, T> for GenericIntervalColumnEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type ColScan = GenericColumnScan<'a, T, GenericIntervalColumnEnum<'a, T>>;

    fn len(&self) -> usize {
        forward_to_interval_column!(self, len)
    }

    fn is_empty(&self) -> bool {
        forward_to_interval_column!(self, is_empty)
    }

    fn get(&self, index: usize) -> T {
        forward_to_interval_column!(self, get(index))
    }

    fn iter(&'a self) -> Self::ColScan {
        GenericColumnScan::new(self)
    }
}

impl<'a, T> IntervalColumn<'a, T> for GenericIntervalColumnEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn int_len(&self) -> usize {
        forward_to_interval_column!(self, int_len)
    }

    fn int_bounds(&self, int_idx: usize) -> Range<usize> {
        forward_to_interval_column!(self, int_bounds(int_idx))
    }
}

#[cfg(test)]
mod test {
    use super::super::VectorColumn;
    use super::{Column, GenericIntervalColumn, IntervalColumn};
    use test_log::test;

    #[test]
    fn test_u64_column() {
        let data: Vec<u64> = vec![1, 2, 3, 10, 11, 12, 20, 30, 31];
        let int_starts: Vec<usize> = vec![0, 3, 6, 7];

        let v_data: VectorColumn<u64> = VectorColumn::new(data);
        let v_int_starts: VectorColumn<usize> = VectorColumn::new(int_starts);
        let gic = GenericIntervalColumn::new(v_data, v_int_starts);
        assert_eq!(gic.len(), 9);
        assert_eq!(gic.get(0), 1);
        assert_eq!(gic.get(1), 2);
        assert_eq!(gic.get(2), 3);
        assert_eq!(gic.get(3), 10);

        assert_eq!(gic.int_len(), 4);
        assert_eq!(gic.int_bounds(0), 0..3);
        assert_eq!(gic.int_bounds(1), 3..6);
        assert_eq!(gic.int_bounds(2), 6..7);
        assert_eq!(gic.int_bounds(3), 7..9);
    }
}
