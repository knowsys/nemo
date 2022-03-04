use super::Column;
use super::ColumnBuilder;
use super::RleColumnBuilder;
use super::VectorColumn;
use std::fmt::Debug;

// Number of elements added after which to decide which column to use.
const COLUMN_IMPL_DECISION_THRESHOLD: usize = 20;

// Each rle elements needs to store three values
// hence for essentially the same space requirement as a plain vector, the ratio would be 3
const TARGET_RATIO_FOR_RLE: usize = 3;

#[derive(Debug, PartialEq)]
enum ColumnType {
    Undecided,
    VectorColumn,
    RleColumn,
}

impl Default for ColumnType {
    fn default() -> Self {
        ColumnType::Undecided
    }
}

/// Implementation of [`ColumnBuilder`] that may adaptively decide for the
/// best possible column implementation for the given data.
#[derive(Debug, Default)]
pub struct AdaptiveColumnBuilder<T> {
    data: Vec<T>,
    rle_column_builder: RleColumnBuilder<T>,
    column_type: ColumnType,
}

impl<T: Debug + Copy + TryFrom<i64> + PartialOrd> AdaptiveColumnBuilder<T>
where
    i64: TryFrom<T>,
{
    /// Constructor.
    pub fn new() -> AdaptiveColumnBuilder<T> {
        AdaptiveColumnBuilder {
            data: Vec::new(),
            rle_column_builder: RleColumnBuilder::new(),
            column_type: ColumnType::Undecided,
        }
    }

    fn decide_column_type(&mut self) {
        if self.column_type != ColumnType::Undecided {
            return;
        }

        self.column_type = if self.rle_column_builder.number_of_rle_elements()
            * TARGET_RATIO_FOR_RLE
            < self.data.len()
        {
            ColumnType::RleColumn
        } else {
            ColumnType::VectorColumn
        }
    }
}

impl<'a, T: 'a + Debug + Copy + Ord + TryFrom<i64>> ColumnBuilder<'a, T>
    for AdaptiveColumnBuilder<T>
where
    i64: TryFrom<T>,
{
    fn add(&mut self, value: T) {
        if self.data.len() > COLUMN_IMPL_DECISION_THRESHOLD {
            self.decide_column_type();
        }

        match self.column_type {
            ColumnType::VectorColumn => self.data.push(value),
            ColumnType::RleColumn => self.rle_column_builder.add(value),
            ColumnType::Undecided => {
                self.data.push(value);
                self.rle_column_builder.add(value);
            }
        }
    }

    fn finalize(mut self) -> Box<dyn Column<T> + 'a> {
        self.decide_column_type();

        match self.column_type {
            ColumnType::VectorColumn => Box::new(VectorColumn::new(self.data)),
            ColumnType::RleColumn => self.rle_column_builder.finalize(),
            ColumnType::Undecided => panic!("column type should have been decided here"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{AdaptiveColumnBuilder, ColumnBuilder, ColumnType};
    use test_log::test;

    fn construct_presumable_vector_column() -> AdaptiveColumnBuilder<u64> {
        let mut acb: AdaptiveColumnBuilder<u64> = AdaptiveColumnBuilder::new();
        acb.add(1);
        acb.add(2);
        acb.add(3);

        acb
    }

    fn construct_presumable_rle_column() -> AdaptiveColumnBuilder<u64> {
        let mut acb: AdaptiveColumnBuilder<u64> = AdaptiveColumnBuilder::new();
        acb.add(1);
        acb.add(2);
        acb.add(3);
        acb.add(4);

        acb
    }

    #[test]
    fn test_column_type_decision_for_vector_column() {
        let mut acb: AdaptiveColumnBuilder<u64> = construct_presumable_vector_column();

        acb.decide_column_type();
        let ct = acb.column_type;

        assert_eq!(ct, ColumnType::VectorColumn);
    }

    #[test]
    fn test_column_type_decision_for_rle_column() {
        let mut acb: AdaptiveColumnBuilder<u64> = construct_presumable_rle_column();

        acb.decide_column_type();
        let ct = acb.column_type;

        assert_eq!(ct, ColumnType::RleColumn);
    }

    #[test]
    fn test_build_u64_vector_column() {
        let acb: AdaptiveColumnBuilder<u64> = construct_presumable_vector_column();

        let vc = acb.finalize();
        assert_eq!(vc.len(), 3);
        assert_eq!(vc.get(0), 1);
        assert_eq!(vc.get(1), 2);
        assert_eq!(vc.get(2), 3);
    }

    #[test]
    fn test_build_u64_rle_column() {
        let acb: AdaptiveColumnBuilder<u64> = construct_presumable_rle_column();

        let vc = acb.finalize();
        assert_eq!(vc.len(), 4);
        assert_eq!(vc.get(0), 1);
        assert_eq!(vc.get(1), 2);
        assert_eq!(vc.get(2), 3);
        assert_eq!(vc.get(3), 4);
    }
}
