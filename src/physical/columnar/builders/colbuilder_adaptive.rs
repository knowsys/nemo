use crate::physical::{
    columnar::columns::{ColBuilderRle, Column, ColumnEnum, ColumnVector},
    datatypes::{ColumnDataType, DataTypeName, DataValueT, Double, Float},
};
use std::fmt::Debug;

use super::ColBuilder;

// Number of rle elements in rle column builder after which to decide which column type to use.
const COLUMN_IMPL_DECISION_THRESHOLD: usize = 5; // 5

// The average minimum length of the elements in the incremental RLE
const TARGET_MIN_LENGTH_FOR_RLE_ELEMENTS: usize = 3; // 3

#[derive(Debug, PartialEq)]
enum ColBuilderType<T> {
    Undecided(Option<ColBuilderRle<T>>), // by default we start building an rle column to evaluate memory consumption
    ColumnVector(Vec<T>),
    ColumnRle(ColBuilderRle<T>),
}

impl<T> Default for ColBuilderType<T> {
    fn default() -> Self {
        Self::Undecided(Some(ColBuilderRle::new()))
    }
}

/// Implementation of [`ColBuilder`] that may adaptively decide for the
/// best possible column implementation for the given data.
#[derive(Debug, Default, PartialEq)]
pub struct ColBuilderAdaptive<T> {
    builder: ColBuilderType<T>,
}

impl<T> ColBuilderAdaptive<T>
where
    T: ColumnDataType + Default,
{
    /// Constructor.
    pub fn new() -> ColBuilderAdaptive<T> {
        ColBuilderAdaptive {
            builder: ColBuilderType::Undecided(Some(ColBuilderRle::new())),
        }
    }

    fn decide_column_type(&mut self) {
        if let ColBuilderType::Undecided(rle_builder_opt) = &mut self.builder {
            let rle_builder = rle_builder_opt
                .take()
                .expect("this is only None from this take() call until the end of this method");

            if rle_builder.avg_length_of_rle_elements() > TARGET_MIN_LENGTH_FOR_RLE_ELEMENTS {
                self.builder = ColBuilderType::ColumnRle(rle_builder);
            } else {
                let rle_column = rle_builder.finalize();
                self.builder = ColBuilderType::ColumnVector(rle_column.iter().collect());
            }
        }
    }
}

impl<'a, T> ColBuilder<'a, T> for ColBuilderAdaptive<T>
where
    T: 'a + ColumnDataType + Default,
{
    type Col = ColumnEnum<T>;

    fn add(&mut self, value: T) {
        if let ColBuilderType::Undecided(Some(rle_builder)) = &self.builder {
            if rle_builder.number_of_rle_elements() > COLUMN_IMPL_DECISION_THRESHOLD {
                self.decide_column_type();
            }
        }

        match &mut self.builder {
            ColBuilderType::ColumnVector(vec) => vec.push(value),
            ColBuilderType::ColumnRle(rle_builder) => rle_builder.add(value),

            // we only build the rle if still undecided and then evaluate the compression ration by
            // the length of the rle elements
            ColBuilderType::Undecided(builder) => builder
                .as_mut()
                .expect("In undecided case None should only briefly be used in decide_column_type.")
                .add(value),
        }
    }

    fn finalize(mut self) -> Self::Col {
        self.decide_column_type();

        match self.builder {
            ColBuilderType::ColumnVector(vec) => Self::Col::ColumnVector(ColumnVector::new(vec)),
            ColBuilderType::ColumnRle(rle_builder) => Self::Col::ColumnRle(rle_builder.finalize()),
            ColBuilderType::Undecided(_) => {
                unreachable!("column type should have been decided here")
            }
        }
    }

    fn count(&self) -> usize {
        match &self.builder {
            ColBuilderType::ColumnVector(vec) => vec.len(),
            ColBuilderType::ColumnRle(rle_builder) => rle_builder.count(),
            ColBuilderType::Undecided(builder) => builder
                .as_ref()
                .expect("Should never be None on the outside")
                .count(),
        }
    }
}

impl<A> FromIterator<A> for ColBuilderAdaptive<A>
where
    A: ColumnDataType + Default,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = A>,
    {
        let mut builder = Self::new();
        for item in iter {
            builder.add(item);
        }
        builder
    }
}

/// Enum for ColBuilderAdaptive with different underlying datatypes
#[derive(Debug)]
pub enum ColBuilderAdaptiveT {
    /// Case u64
    U64(ColBuilderAdaptive<u64>),
    /// Case Float
    Float(ColBuilderAdaptive<Float>),
    /// Case Double
    Double(ColBuilderAdaptive<Double>),
}

impl ColBuilderAdaptiveT {
    /// Creates a new empty ColBuilderAdaptiveT for the given DataTypeName
    pub fn new(dtn: DataTypeName) -> Self {
        match dtn {
            DataTypeName::U64 => Self::U64(ColBuilderAdaptive::new()),
            DataTypeName::Float => Self::Float(ColBuilderAdaptive::new()),
            DataTypeName::Double => Self::Double(ColBuilderAdaptive::new()),
        }
    }

    /// Adds value of arbitrary type to the column builder
    pub fn add(&mut self, value: DataValueT) {
        match self {
            Self::U64(cb) => {
                if let DataValueT::U64(v) = value {
                    cb.add(v);
                } else {
                    panic!("value does not match AdaptiveColumn type");
                }
            }
            Self::Float(cb) => {
                if let DataValueT::Float(v) = value {
                    cb.add(v);
                } else {
                    panic!("value does not match AdaptiveColumn type");
                }
            }
            Self::Double(cb) => {
                if let DataValueT::Double(v) = value {
                    cb.add(v);
                } else {
                    panic!("value does not match AdaptiveColumn type");
                }
            }
        }
    }

    /// Return the number of elements that were already added.
    pub fn count(&self) -> usize {
        match self {
            Self::U64(cb) => cb.count(),
            Self::Float(cb) => cb.count(),
            Self::Double(cb) => cb.count(),
        }
    }
}

impl FromIterator<DataValueT> for ColBuilderAdaptiveT {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = DataValueT>,
    {
        let mut peekable = iter.into_iter().peekable();
        let mut builder = Self::new(
            peekable
                .peek()
                .map(|dv| dv.get_type())
                .unwrap_or(DataTypeName::U64),
        );
        for item in peekable {
            builder.add(item);
        }
        builder
    }
}

#[cfg(test)]
mod test {
    use super::{ColBuilder, ColBuilderAdaptive, ColBuilderType};
    use crate::physical::{
        columnar::columns::Column,
        datatypes::{Double, Float},
    };
    use test_log::test;

    fn construct_presumable_vector_column() -> ColBuilderAdaptive<u32> {
        let mut acb: ColBuilderAdaptive<u32> = ColBuilderAdaptive::new();
        acb.add(1);
        acb.add(2);
        acb.add(3);

        acb
    }

    fn construct_presumable_rle_column() -> ColBuilderAdaptive<u32> {
        let mut acb: ColBuilderAdaptive<u32> = ColBuilderAdaptive::new();
        acb.add(1);
        acb.add(2);
        acb.add(3);
        acb.add(4);

        acb
    }

    #[test]
    fn test_column_type_decision_for_vector_column() {
        let mut acb: ColBuilderAdaptive<u32> = construct_presumable_vector_column();

        acb.decide_column_type();
        let builder = acb.builder;

        assert!(matches!(builder, ColBuilderType::ColumnVector(_)));
    }

    #[test]
    fn test_column_type_decision_for_rle_column() {
        let mut acb: ColBuilderAdaptive<u32> = construct_presumable_rle_column();

        acb.decide_column_type();
        let builder = acb.builder;

        assert!(matches!(builder, ColBuilderType::ColumnRle(_)));
    }

    #[test]
    fn test_build_u32_vector_column() {
        let acb: ColBuilderAdaptive<u32> = construct_presumable_vector_column();

        let vc = acb.finalize();
        assert_eq!(vc.len(), 3);
        assert_eq!(vc.get(0), 1);
        assert_eq!(vc.get(1), 2);
        assert_eq!(vc.get(2), 3);
    }

    #[test]
    fn test_build_u32_rle_column() {
        let acb: ColBuilderAdaptive<u32> = construct_presumable_rle_column();

        let rlec = acb.finalize();
        assert_eq!(rlec.len(), 4);
        assert_eq!(rlec.get(0), 1);
        assert_eq!(rlec.get(1), 2);
        assert_eq!(rlec.get(2), 3);
        assert_eq!(rlec.get(3), 4);
    }

    #[test]
    fn test_build_empty_u32_column() {
        let acb: ColBuilderAdaptive<u32> = ColBuilderAdaptive::new();

        let c = acb.finalize();
        assert!(c.is_empty());
    }

    #[test]
    fn test_build_u64_column() {
        let mut acb: ColBuilderAdaptive<u64> = ColBuilderAdaptive::new();
        acb.add(1);
        acb.add(2);
        acb.add(3);
        acb.add(4);

        let col = acb.finalize();
        assert_eq!(col.len(), 4);
        assert_eq!(col.get(0), 1);
        assert_eq!(col.get(1), 2);
        assert_eq!(col.get(2), 3);
        assert_eq!(col.get(3), 4);
    }

    #[test]
    fn test_build_float_column() {
        let mut acb: ColBuilderAdaptive<Float> = ColBuilderAdaptive::new();
        acb.add(Float::from_number(1.0));
        acb.add(Float::from_number(2.0));
        acb.add(Float::from_number(3.0));
        acb.add(Float::from_number(4.0));

        let col = acb.finalize();
        assert_eq!(col.len(), 4);
        assert_eq!(col.get(0), Float::from_number(1.0));
        assert_eq!(col.get(1), Float::from_number(2.0));
        assert_eq!(col.get(2), Float::from_number(3.0));
        assert_eq!(col.get(3), Float::from_number(4.0));
    }

    #[test]
    fn test_build_double_column() {
        let mut acb: ColBuilderAdaptive<Double> = ColBuilderAdaptive::new();
        acb.add(Double::from_number(1.0));
        acb.add(Double::from_number(2.0));
        acb.add(Double::from_number(3.0));
        acb.add(Double::from_number(4.0));

        let col = acb.finalize();
        assert_eq!(col.len(), 4);
        assert_eq!(col.get(0), Double::from_number(1.0));
        assert_eq!(col.get(1), Double::from_number(2.0));
        assert_eq!(col.get(2), Double::from_number(3.0));
        assert_eq!(col.get(3), Double::from_number(4.0));
    }
}
