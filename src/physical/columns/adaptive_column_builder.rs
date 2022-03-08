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
enum ColumnBuilderType<T> {
    Undecided(Option<RleColumnBuilder<T>>), // by default we start building an rle column to evaluate memory consumption
    VectorColumn(Vec<T>),
    RleColumn(RleColumnBuilder<T>),
}

impl<T> Default for ColumnBuilderType<T> {
    fn default() -> Self {
        Self::Undecided(Some(RleColumnBuilder::new()))
    }
}

/// Implementation of [`ColumnBuilder`] that may adaptively decide for the
/// best possible column implementation for the given data.
#[derive(Debug, Default)]
pub struct AdaptiveColumnBuilder<T> {
    builder: ColumnBuilderType<T>,
}

impl<T: Debug + Copy + TryFrom<i64> + PartialOrd> AdaptiveColumnBuilder<T>
where
    i64: From<T>,
    <T as TryFrom<i64>>::Error: Debug,
{
    /// Constructor.
    pub fn new() -> AdaptiveColumnBuilder<T> {
        AdaptiveColumnBuilder {
            builder: ColumnBuilderType::Undecided(Some(RleColumnBuilder::new())),
        }
    }

    fn decide_column_type(&mut self) {
        if let ColumnBuilderType::Undecided(rle_builder_opt) = &mut self.builder {
            let rle_builder = rle_builder_opt.take().unwrap();

            if rle_builder.avg_length_of_rle_elements() > TARGET_MIN_LENGTH_FOR_RLE_ELEMENTS {
                self.builder = ColumnBuilderType::RleColumn(rle_builder);
            } else {
                let rle_column = rle_builder.finalize();
                self.builder = ColumnBuilderType::VectorColumn(rle_column.iter().collect());
            }
        }
    }
}

impl<'a, T: 'a + Debug + Copy + Ord + TryFrom<i64>> ColumnBuilder<'a, T>
    for AdaptiveColumnBuilder<T>
where
    i64: From<T>,
    <T as TryFrom<i64>>::Error: Debug,
{
    fn add(&mut self, value: T) {
        if let ColumnBuilderType::Undecided(Some(rle_builder)) = &self.builder {
            if rle_builder.number_of_rle_elements() > COLUMN_IMPL_DECISION_THRESHOLD {
                self.decide_column_type();
            }
        }

        match &mut self.builder {
            ColumnBuilderType::VectorColumn(vec) => vec.push(value),
            ColumnBuilderType::RleColumn(rle_builder) => rle_builder.add(value),

            // we only build the rle if still undecided and then evaluate the compression ration by
            // the length of the rle elements
            ColumnBuilderType::Undecided(Some(rle_builder)) => rle_builder.add(value),
            ColumnBuilderType::Undecided(None) => {
                panic!("In undecided case None should only briefly be used in decide_column_type.")
            }
        }
    }

    fn finalize(mut self) -> Box<dyn Column<T> + 'a> {
        self.decide_column_type();

        match self.builder {
            ColumnBuilderType::VectorColumn(vec) => Box::new(VectorColumn::new(vec)),
            ColumnBuilderType::RleColumn(rle_builder) => rle_builder.finalize(),
            ColumnBuilderType::Undecided(_) => panic!("column type should have been decided here"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{AdaptiveColumnBuilder, ColumnBuilder, ColumnBuilderType};
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
        let builder = acb.builder;

        assert!(matches!(builder, ColumnBuilderType::VectorColumn(_)));
    }

    #[test]
    fn test_column_type_decision_for_rle_column() {
        let mut acb: AdaptiveColumnBuilder<u32> = construct_presumable_rle_column();

        acb.decide_column_type();
        let builder = acb.builder;

        assert!(matches!(builder, ColumnBuilderType::RleColumn(_)));
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

        let rlec = acb.finalize();
        assert_eq!(rlec.len(), 4);
        assert_eq!(rlec.get(0), 1);
        assert_eq!(rlec.get(1), 2);
        assert_eq!(rlec.get(2), 3);
        assert_eq!(rlec.get(3), 4);
    }
}
