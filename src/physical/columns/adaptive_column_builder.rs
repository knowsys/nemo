// TODO: get rid of unwraps

use super::Column;
use super::ColumnBuilder;
use super::RleColumnBuilder;
use super::VectorColumn;
use std::fmt::Debug;

// Number of rle elements in rle column builder after which to decide which column type to use.
const COLUMN_IMPL_DECISION_THRESHOLD: usize = 5;

// The average minimum length of the elements in the incremental RLE
const TARGET_MIN_LENGTH_FOR_RLE_ELEMENTS: usize = 3;

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
    vector_column_data: Option<Vec<T>>,
    rle_column_builder: Option<RleColumnBuilder<T>>,
    column_type: ColumnType,
}

impl<T: Debug + Copy + TryFrom<i64> + PartialOrd> AdaptiveColumnBuilder<T>
where
    i64: From<T>,
{
    /// Constructor.
    pub fn new() -> AdaptiveColumnBuilder<T> {
        AdaptiveColumnBuilder {
            vector_column_data: None,
            rle_column_builder: Some(RleColumnBuilder::new()),
            column_type: ColumnType::Undecided,
        }
    }

    fn decide_column_type(&mut self) {
        if self.column_type != ColumnType::Undecided {
            return;
        }

        if self
            .rle_column_builder
            .as_ref()
            .unwrap()
            .avg_length_of_rle_elements()
            > TARGET_MIN_LENGTH_FOR_RLE_ELEMENTS
        {
            self.column_type = ColumnType::RleColumn;
        } else {
            self.column_type = ColumnType::VectorColumn;
            let rle_column = self.rle_column_builder.take().unwrap().finalize();
            self.vector_column_data = Some(rle_column.iter().collect());
        }
    }
}

impl<'a, T: 'a + Debug + Copy + Ord + TryFrom<i64>> ColumnBuilder<'a, T>
    for AdaptiveColumnBuilder<T>
where
    i64: From<T>,
{
    fn add(&mut self, value: T) {
        if let Some(builder) = &self.rle_column_builder {
            if builder.number_of_rle_elements() > COLUMN_IMPL_DECISION_THRESHOLD {
                self.decide_column_type();
            }
        }

        match self.column_type {
            ColumnType::VectorColumn => self.vector_column_data.as_mut().unwrap().push(value),
            ColumnType::RleColumn => self.rle_column_builder.as_mut().unwrap().add(value),

            // we only build the rle if still undecided and then evaluate the compression ration by
            // the length of the rle elements
            ColumnType::Undecided => self.rle_column_builder.as_mut().unwrap().add(value),
        }
    }

    fn finalize(mut self) -> Box<dyn Column<T> + 'a> {
        self.decide_column_type();

        match self.column_type {
            ColumnType::VectorColumn => {
                Box::new(VectorColumn::new(self.vector_column_data.unwrap()))
            }
            ColumnType::RleColumn => self.rle_column_builder.unwrap().finalize(),
            ColumnType::Undecided => panic!("column type should have been decided here"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{AdaptiveColumnBuilder, ColumnBuilder, ColumnType};
    use test_log::test;

    fn construct_presumable_vector_column() -> AdaptiveColumnBuilder<u32> {
        let mut acb: AdaptiveColumnBuilder<u32> = AdaptiveColumnBuilder::new();
        acb.add(1);
        acb.add(2);
        acb.add(3);

        acb
    }

    fn construct_presumable_rle_column() -> AdaptiveColumnBuilder<u32> {
        let mut acb: AdaptiveColumnBuilder<u32> = AdaptiveColumnBuilder::new();
        acb.add(1);
        acb.add(2);
        acb.add(3);
        acb.add(4);

        acb
    }

    #[test]
    fn test_column_type_decision_for_vector_column() {
        let mut acb: AdaptiveColumnBuilder<u32> = construct_presumable_vector_column();

        acb.decide_column_type();
        let ct = acb.column_type;

        assert_eq!(ct, ColumnType::VectorColumn);
    }

    #[test]
    fn test_column_type_decision_for_rle_column() {
        let mut acb: AdaptiveColumnBuilder<u32> = construct_presumable_rle_column();

        acb.decide_column_type();
        let ct = acb.column_type;

        assert_eq!(ct, ColumnType::RleColumn);
    }

    #[test]
    fn test_build_u32_vector_column() {
        let acb: AdaptiveColumnBuilder<u32> = construct_presumable_vector_column();

        let vc = acb.finalize();
        assert_eq!(vc.len(), 3);
        assert_eq!(vc.get(0), 1);
        assert_eq!(vc.get(1), 2);
        assert_eq!(vc.get(2), 3);
    }

    #[test]
    fn test_build_u32_rle_column() {
        let acb: AdaptiveColumnBuilder<u32> = construct_presumable_rle_column();

        let vc = acb.finalize();
        assert_eq!(vc.len(), 4);
        assert_eq!(vc.get(0), 1);
        assert_eq!(vc.get(1), 2);
        assert_eq!(vc.get(2), 3);
        assert_eq!(vc.get(3), 4);
    }
}
