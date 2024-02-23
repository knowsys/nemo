//! This module defines an adaptive column builder,
//! which dynamically chooses the most efficient representation
//! for the constructed column.

use std::fmt::Debug;

use crate::{
    columnar::column::{vector::ColumnVector, Column, ColumnEnum},
    datatypes::{ColumnDataType, RunLengthEncodable},
};

use super::{rle::ColumnBuilderRle, ColumnBuilder};

/// Number of rle elements in rle column builder after which to decide which column type to use.
#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum ColumnImplDecisionThreshold {
    /// Number of RleElements after that a decision shall be made
    NumberOfRleElements(usize),
    /// Decide only when builder is finalized
    _OnFinalize,
}

impl Default for ColumnImplDecisionThreshold {
    fn default() -> Self {
        Self::NumberOfRleElements(100)
    }
}

/// The average minimum length of the elements in the incremental RLE
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) struct TargetMinLengthForRleElements(usize);

impl Default for TargetMinLengthForRleElements {
    fn default() -> Self {
        Self(50)
    }
}

#[derive(Debug, PartialEq)]
enum ColumnBuilderType<T: RunLengthEncodable> {
    // by default we start building an rle column to evaluate memory consumption
    // for at least ColumnImplDecisionThreshold many elements
    ColumnRle(ColumnBuilderRle<T>),
    // if the elements' lengths are below TargetMinLengthForRleElements, switch to ColumnVector
    ColumnVector(Vec<T>),
}

impl<T> Default for ColumnBuilderType<T>
where
    T: ColumnDataType + Default,
{
    fn default() -> Self {
        Self::ColumnRle(ColumnBuilderRle::new())
    }
}

/// Implementation of [`ColumnBuilder`] that may adaptively decide for the
/// best possible column implementation for the given data.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ColumnBuilderAdaptive<T>
where
    T: ColumnDataType,
{
    builder: ColumnBuilderType<T>,
    decision_threshold: ColumnImplDecisionThreshold,
    target_min_length_for_rle_elements: TargetMinLengthForRleElements,
}

impl<T> ColumnBuilderAdaptive<T>
where
    T: ColumnDataType + Default,
{
    /// Constructor.
    pub(crate) fn _new(
        decision_threshold: ColumnImplDecisionThreshold,
        target_min_length_for_rle_elements: TargetMinLengthForRleElements,
    ) -> ColumnBuilderAdaptive<T> {
        ColumnBuilderAdaptive {
            builder: ColumnBuilderType::ColumnRle(ColumnBuilderRle::new()),
            decision_threshold,
            target_min_length_for_rle_elements,
        }
    }

    fn expand_sparse_rle_columns(&mut self) {
        let ColumnBuilderType::ColumnRle(rle_builder) = &self.builder else {
            return;
        };
        let avg_len = rle_builder.avg_length_of_rle_elements();

        if self.target_min_length_for_rle_elements.0 > avg_len {
            let new_builder =
                ColumnBuilderType::ColumnVector(Vec::with_capacity(rle_builder.count()));
            let ColumnBuilderType::ColumnRle(rle_builder) =
                std::mem::replace(&mut self.builder, new_builder)
            else {
                unreachable!("ColumnBuilderType checked in let-else expression above");
            };

            let rle_column = rle_builder.finalize();
            self.builder = ColumnBuilderType::ColumnVector(rle_column.iter().collect());
        }
    }
}

impl<'a, T> ColumnBuilder<'a, T> for ColumnBuilderAdaptive<T>
where
    T: 'a + ColumnDataType + Default,
{
    type Col = ColumnEnum<T>;

    fn add(&mut self, value: T) {
        if let ColumnImplDecisionThreshold::NumberOfRleElements(threshold) = self.decision_threshold
        {
            if let ColumnBuilderType::ColumnRle(rle_builder) = &mut self.builder {
                if rle_builder.number_of_rle_elements() > threshold {
                    self.expand_sparse_rle_columns();
                }
            }
        }

        match &mut self.builder {
            ColumnBuilderType::ColumnVector(vec) => vec.push(value),
            ColumnBuilderType::ColumnRle(rle_builder) => rle_builder.add(value),
        }
    }

    fn finalize(mut self) -> Self::Col {
        self.expand_sparse_rle_columns();

        match self.builder {
            ColumnBuilderType::ColumnVector(vec) => Self::Col::ColumnVector(ColumnVector::new(vec)),
            ColumnBuilderType::ColumnRle(rle_builder) => {
                Self::Col::ColumnRle(rle_builder.finalize())
            }
        }
    }

    fn count(&self) -> usize {
        match &self.builder {
            ColumnBuilderType::ColumnVector(vec) => vec.len(),
            ColumnBuilderType::ColumnRle(rle_builder) => rle_builder.count(),
        }
    }
}

impl<A> FromIterator<A> for ColumnBuilderAdaptive<A>
where
    A: ColumnDataType + Default,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = A>,
    {
        let mut builder = Self::default();
        for item in iter {
            builder.add(item);
        }
        builder
    }
}

#[cfg(test)]
mod test {
    use super::ColumnBuilderAdaptive;
    use crate::{
        columnar::{
            column::{Column, ColumnEnum},
            columnbuilder::ColumnBuilder,
        },
        datatypes::{Double, Float},
    };
    use test_log::test;

    fn construct_presumable_vector_column() -> ColumnBuilderAdaptive<u32> {
        let mut acb = ColumnBuilderAdaptive::<u32>::default();
        acb.add(1);
        acb.add(2);
        acb.add(3);

        acb
    }

    fn construct_presumable_rle_column() -> ColumnBuilderAdaptive<u32> {
        let mut acb = ColumnBuilderAdaptive::<u32>::default();

        for i in 1..100 {
            acb.add(i)
        }

        acb
    }

    fn construct_pathological_column() -> ColumnBuilderAdaptive<u32> {
        let mut acb = ColumnBuilderAdaptive::<u32>::default();

        for i in 1..100 {
            acb.add(i)
        }

        for _ in 0..1000 {
            acb.add(rand::random())
        }

        acb
    }

    #[test]
    fn test_column_type_decision_for_vector_column() {
        let acb: ColumnBuilderAdaptive<u32> = construct_presumable_vector_column();

        let column = acb.finalize();

        assert!(matches!(column, ColumnEnum::ColumnVector(_)));
    }

    #[test]
    fn test_column_type_decision_for_rle_column() {
        let acb: ColumnBuilderAdaptive<u32> = construct_presumable_rle_column();

        let column = acb.finalize();

        assert!(matches!(column, ColumnEnum::ColumnRle(_)));
    }

    #[test]
    fn test_column_type_decision_for_pathological_column() {
        let acb = construct_pathological_column();

        let column = acb.finalize();

        assert!(matches!(column, ColumnEnum::ColumnVector(_)));
    }

    #[test]
    fn test_build_u32_vector_column() {
        let acb: ColumnBuilderAdaptive<u32> = construct_presumable_vector_column();

        let vc = acb.finalize();
        assert_eq!(vc.len(), 3);
        assert_eq!(vc.get(0), 1);
        assert_eq!(vc.get(1), 2);
        assert_eq!(vc.get(2), 3);
    }

    #[test]
    fn test_build_u32_rle_column() {
        let acb: ColumnBuilderAdaptive<u32> = construct_presumable_rle_column();

        let rlec = acb.finalize();
        assert_eq!(rlec.len(), 99);
        assert_eq!(rlec.get(0), 1);
        assert_eq!(rlec.get(1), 2);
        assert_eq!(rlec.get(2), 3);
        assert_eq!(rlec.get(3), 4);
    }

    #[test]
    fn test_build_empty_u32_column() {
        let acb = ColumnBuilderAdaptive::<u32>::default();

        let c = acb.finalize();
        assert!(c.is_empty());
    }

    #[test]
    fn test_build_u64_column() {
        let mut acb = ColumnBuilderAdaptive::<u64>::default();
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
        let mut acb = ColumnBuilderAdaptive::<Float>::default();
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
        let mut acb = ColumnBuilderAdaptive::<Double>::default();
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
