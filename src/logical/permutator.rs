//! Holds the [Permutator] struct, which allows one to define a logical permutation of the content of index-based data structures

use crate::physical::datatypes::{Field, FloorToUsize};
use std::cmp::Ordering;
use std::fmt::Debug;

use crate::{
    error::Error,
    physical::{
        columns::{Column, ColumnBuilder, ColumnEnum, ColumnT},
        datatypes::data_value::VecT,
    },
};

/// Allows one to define a logical permutation of content of index-based data structures
#[derive(Debug, Clone)]
pub struct Permutator {
    sort_vec: Vec<usize>,
    offset: usize,
}

impl Permutator {
    /// Creates a [`Permutator`] based on one slice of sort-able data
    pub fn sort_from_vec<T>(data: &[T]) -> Permutator
    where
        T: PartialEq + PartialOrd + Ord + Eq,
    {
        Self::sort_from_vec_with_offset(data, 0)
    }

    /// Creates a [`Permutator`] based on a slice, with a given offset.
    pub fn sort_from_vec_with_offset<T>(data: &[T], offset: usize) -> Permutator
    where
        T: PartialEq + PartialOrd + Ord + Eq,
    {
        let mut vec = (0..data.len()).collect::<Vec<usize>>();
        vec.sort_by_key(|&i| &data[i]);
        Permutator {
            sort_vec: vec,
            offset,
        }
    }

    /// Creates [`Permutator`] based on a [`Column`][crate::physical::columns::column::Column]
    pub fn sort_from_column<T>(data: &ColumnEnum<T>) -> Permutator
    where
        T: Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field + Ord,
    {
        let mut vec = (0..data.len()).collect::<Vec<usize>>();
        vec.sort_by_key(|&i| data.get(i));
        Permutator {
            sort_vec: vec,
            offset: 0,
        }
    }

    /// Creates a [`Permutator`] based on a slice of [`ColumnT`][crate::physical::columns::column::ColumnT] elements.
    pub fn sort_from_columns(data_vec: &[ColumnT]) -> Result<Permutator, Error> {
        let len = if !data_vec.is_empty() {
            let len = data_vec[0].len();
            if data_vec.iter().any(|val| val.len() != len) {
                return Err(Error::PermutationSortLen(
                    data_vec.iter().map(|val| val.len()).collect::<Vec<usize>>(),
                ));
            }
            len
        } else {
            0
        };
        let mut vec: Vec<usize> = (0..len).collect::<Vec<usize>>();
        vec.sort_by(|a, b| Self::compare_multiple_column(*a, *b, data_vec));
        Ok(Permutator {
            sort_vec: vec,
            offset: 0,
        })
    }

    /// Creates a [`Permutator`] based on one a list of [`VecT`][crate::physical::datatypes::data_value::VecT].
    ///
    /// The sorting of values is done by taking the first [`VecT`][crate::physical::datatypes::data_value::VecT] and sort according to these values.
    /// If two elements are the equal to each other, the next [`VecT`][crate::physical::datatypes::data_value::VecT] will be taken to check if the comparison is different.
    /// If all [`VecT`][crate::physical::datatypes::data_value::VecT] comparisons result in equality, the original order of the two values is preserved.
    ///
    /// Returns an Error, if the length of the given [`VecT`][crate::physical::datatypes::data_value::VecT] are different.
    pub fn sort_from_multiple_vec(data_vec: &[VecT]) -> Result<Permutator, Error> {
        Self::sort_from_multiple_vec_with_offset(data_vec, 0)
    }

    /// Creates a [`Permutator`] based on one a list of [`VecT`][crate::physical::datatypes::data_value::VecT], with a given offset.
    ///
    /// The sorting of values is done by taking the first [`VecT`][crate::physical::datatypes::data_value::VecT] and sort according to these values.
    /// If two elements are the equal to each other, the next [`VecT`][crate::physical::datatypes::data_value::VecT] will be taken to check if the comparison is different.
    /// If all [`VecT`][crate::physical::datatypes::data_value::VecT] comparisons result in equality, the original order of the two values is preserved.
    ///
    /// Returns an Error, if the length of the given [`VecT`][crate::physical::datatypes::data_value::VecT] are different.
    pub fn sort_from_multiple_vec_with_offset(
        data_vec: &[VecT],
        offset: usize,
    ) -> Result<Permutator, Error> {
        let len = if !data_vec.is_empty() {
            let len = data_vec[0].len();
            if data_vec.iter().any(|val| val.len() != len) {
                return Err(Error::PermutationSortLen(
                    data_vec.iter().map(|val| val.len()).collect::<Vec<usize>>(),
                ));
            }
            len
        } else {
            0
        };
        let mut vec: Vec<usize> = (0..len).collect::<Vec<usize>>();
        vec.sort_by(|a, b| Self::compare_multiple_vec(*a, *b, data_vec));
        Ok(Permutator {
            sort_vec: vec,
            offset,
        })
    }

    fn compare_multiple_vec(a: usize, b: usize, data_vec: &[VecT]) -> Ordering {
        match data_vec.iter().try_for_each(|vec| {
            match vec
                .compare_idx(a, b)
                .expect("This function should only be used by correct indices")
            {
                Ordering::Less => Err(Ordering::Less),
                Ordering::Equal => Ok(()),
                Ordering::Greater => Err(Ordering::Greater),
            }
        }) {
            Ok(_) => Ordering::Equal,
            Err(ord) => ord,
        }
    }

    fn compare_multiple_column(a: usize, b: usize, data_vec: &[ColumnT]) -> Ordering {
        match data_vec
            .iter()
            .try_for_each(|vec| match vec.get(a).compare(&vec.get(b)).expect("Both values are from the same ColumnT, therefore they need to have the same inner type") {
                Ordering::Less => Err(Ordering::Less),
                Ordering::Equal => Ok(()),
                Ordering::Greater => Err(Ordering::Greater),
            }) {
            Ok(_) => Ordering::Equal,
            Err(ord) => ord,
        }
    }

    /// Returns the value at a given index or [`None`] if not applicable
    pub fn value_at(&self, idx: usize) -> Option<usize> {
        self.sort_vec.get(idx.checked_sub(self.offset)?).copied()
    }

    /// Returns the value at a given index or the identity otherwise
    pub fn value_id(&self, idx: usize) -> usize {
        self.value_at(idx).unwrap_or(idx)
    }

    /// Permutates a given slice of data with the computed sort-order
    pub fn permutate<T>(&self, data: &[T]) -> Result<Vec<T>, Error>
    where
        T: Clone,
    {
        if data.len() < (self.sort_vec.len() + self.offset) {
            Err(Error::PermutationApplyWrongLen(
                data.len(),
                self.sort_vec.len(),
                self.offset,
            ))
        } else {
            let x = (0..self.offset)
                .chain(self.sort_vec.iter().map(|&idx| idx + self.offset))
                .chain((self.offset + self.sort_vec.len())..data.len());
            Ok(x.map(|idx| data[idx].clone()).collect::<Vec<_>>())
        }
    }

    /// Applies the permutator to a given column by using a provided [`ColumnBuilder`].
    ///
    /// *Returns* either a ['Column'] or an [Error][Error::Permutation]
    pub fn apply_column<'a, T, U>(
        &self,
        column: &ColumnEnum<T>,
        mut cb: U,
    ) -> Result<ColumnEnum<T>, Error>
    where
        T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
        U: ColumnBuilder<'a, T, Col = ColumnEnum<T>>,
    {
        if column.len() < (self.sort_vec.len() + self.offset) {
            Err(Error::PermutationApplyWrongLen(
                column.len(),
                self.sort_vec.len(),
                self.offset,
            ))
        } else {
            let iter = (0..self.offset)
                .chain(self.sort_vec.iter().map(|&idx| idx + self.offset))
                .chain((self.offset + self.sort_vec.len())..column.len());
            iter.for_each(|idx| {
                cb.add(column.get(idx));
            });
            Ok(cb.finalize())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::physical::{
        columns::{AdaptiveColumnBuilder, ColumnBuilder, VectorColumn},
        datatypes::{Double, Float},
    };
    use quickcheck_macros::quickcheck;
    use test_log::test;

    fn apply_sort_permutator<T>(data: &[T])
    where
        T: PartialEq + PartialOrd + Ord + Eq + Clone + Copy + Debug,
    {
        let mut vector = data.to_vec();

        let permutator = Permutator::sort_from_vec(data);
        vector.sort_unstable();
        assert_eq!(
            vector,
            permutator
                .permutate(data)
                .expect("Expect that sorting works in this test-case")
        );
    }
    #[quickcheck]
    #[cfg_attr(miri, ignore)]
    fn sort_from_vec(
        vec_u64: Vec<u64>,
        vec_i64: Vec<i64>,
        vec_f64: Vec<f64>,
        vec_f32: Vec<f32>,
    ) -> bool {
        log::debug!(
            "used values:\nvec_u64: {:?}\nvec_i64: {:?}\nvec_f64: {:?}\n vec_f32: {:?}",
            vec_u64,
            vec_i64,
            vec_f64,
            vec_f32
        );
        // Removing NaN values
        let vec_double = vec_f64
            .iter()
            .filter_map(|&val| Double::new(val).ok())
            .collect::<Vec<Double>>();
        let vec_float = vec_f32
            .iter()
            .filter_map(|&val| Float::new(val).ok())
            .collect::<Vec<Float>>();

        let mut vec_usize = vec_u64.clone();

        let permutator = Permutator::sort_from_vec(&vec_usize);
        vec_usize.sort_unstable();
        assert_eq!(
            vec_usize,
            permutator
                .permutate(&vec_u64)
                .expect("Expect that sorting works")
        );

        apply_sort_permutator(&vec_u64);
        apply_sort_permutator(&vec_i64);
        apply_sort_permutator(&vec_float);
        apply_sort_permutator(&vec_double);
        true
    }

    #[test]
    fn sort_multiple_from_one_vec() {
        let vec = vec![10, 5, 1, 9, 2, 3, 5, 4, 7, 8, 6, 0];
        let vec2 = vec![0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

        let permutator = Permutator::sort_from_vec(&vec);
        assert_eq!(
            vec![0, 1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10],
            permutator
                .permutate(&vec)
                .expect("Expect that sorting works in this test-case")
        );
        assert_eq!(
            vec![11, 2, 4, 5, 7, 1, 6, 10, 8, 9, 3, 0],
            permutator
                .permutate(&vec2)
                .expect("Expect that sorting works in this test-case")
        );
    }
    #[test]
    fn sort_multiple_from_one_interval() {
        let vec = vec![10, 5, 1, 9, 2, 3, 5, 4, 7, 8, 6, 0];
        let vec2 = vec![0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2];

        let permutator = Permutator::sort_from_vec_with_offset(&vec[3..5], 3);
        assert_eq!(
            vec![0, 1, 2, 4, 3, 5, 6, 7, 8, 9, 10, 11, 2],
            permutator
                .permutate(&vec2)
                .expect("Expect that sorting works in this test-case")
        );

        let permutator = Permutator::sort_from_vec_with_offset(&vec[3..8], 3);
        assert_eq!(
            vec![0, 1, 2, 4, 5, 7, 6, 3, 8, 9, 10, 11, 2],
            permutator
                .permutate(&vec2)
                .expect("Expect that sorting works in this test-case")
        );

        let permutator = Permutator::sort_from_vec_with_offset(&vec[4..11], 3);
        assert_eq!(
            vec![0, 1, 2, 3, 4, 6, 5, 9, 7, 8, 10, 11, 2],
            permutator
                .permutate(&vec2)
                .expect("Expect that sorting works in this test-case")
        );
    }

    #[test]
    fn error_with_permutator() {
        let vec = vec![10, 5, 1, 9, 2, 3, 5, 4, 7, 8, 6, 0];
        let vec2 = vec![0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2];
        let permutator = Permutator::sort_from_vec(&vec2);
        let result = permutator.permutate(&vec);
        assert!(result.is_err());
        let err = result.expect_err("Just checked that this is an error");
        match err {
            Error::PermutationApplyWrongLen(_, _, _) => (),
            _ => panic!("wrong error returned"),
        }
    }

    #[test]
    fn multiple_vec_sort() {
        let vec1 = vec![0u64, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3];
        let vec2 = vec![
            Double::new(1.0).expect("1 is not NaN"),
            2.0.try_into().expect("2 is not NaN"),
            3.0.try_into().expect("3 is not NaN"),
            3.0.try_into().expect("3 is not NaN"),
            2.0.try_into().expect("2 is not NaN"),
            1.0.try_into().expect("1 is not NaN"),
            4.0.try_into().expect("4 is not NaN"),
            4.0.try_into().expect("4 is not NaN"),
            4.0.try_into().expect("4 is not NaN"),
            2.0.try_into().expect("2 is not NaN"),
            1.0.try_into().expect("1 is not NaN"),
            1.0.try_into().expect("1 is not NaN"),
        ];
        let vec3 = vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9, 11, 11, 11];
        let checker = vec![10, 5, 1, 9, 2, 3, 5, 4, 7, 8, 6, 0];

        let permutator = Permutator::sort_from_multiple_vec_with_offset(
            &[
                VecT::VecU64(vec1),
                VecT::VecDouble(vec2),
                VecT::VecU64(vec3),
            ],
            0,
        )
        .expect("Sort should work in this test case");
        assert_eq!(
            vec![10, 5, 1, 3, 2, 9, 5, 4, 7, 6, 0, 8],
            permutator
                .permutate(&checker)
                .expect("Applying permutation should work in this test-case")
        );
    }

    #[quickcheck]
    #[cfg_attr(miri, ignore)]
    fn from_rnd_column(vec: Vec<u32>) -> bool {
        log::debug!("used vector: {:?}", vec);
        if vec.is_empty() {
            // TODO: Remove if corresponding bug is fixed
            return true;
        }
        let mut builder: AdaptiveColumnBuilder<u32> = AdaptiveColumnBuilder::new();
        let mut vec_cpy = vec.clone();
        vec_cpy.sort_unstable();
        vec.iter().for_each(|&elem| builder.add(elem));
        let column = builder.finalize();

        let permutator = Permutator::sort_from_column(&column);
        let sort_vec = permutator
            .permutate(&vec)
            .expect("Applying permutation should work in this test-case");
        assert_eq!(sort_vec, vec_cpy);
        true
    }

    #[quickcheck]
    #[cfg_attr(miri, ignore)]
    fn from_rnd_columns(vector1: Vec<f32>, vector2: Vec<f64>) -> bool {
        // remove NaN
        let mut vec1 = vector1
            .iter()
            .cloned()
            .filter_map(|val| Float::new(val).ok())
            .collect::<Vec<Float>>();
        let mut vec2 = vector2
            .iter()
            .filter_map(|val| Double::new(*val).ok())
            .collect::<Vec<Double>>();
        let len = vec1.len().min(vec2.len());
        vec1.resize(len, Float::new(1.0).expect("1.0 is not NaN"));
        vec2.resize(len, Double::new(1.0).expect("1.0 is not NaN"));
        let mut vec1_cpy = vec1.clone();
        let vec1_to_sort = vec1.clone();
        let column1: VectorColumn<Float> = VectorColumn::new(vec1);
        let column2: VectorColumn<Double> = VectorColumn::new(vec2.clone());
        let columnset: Vec<ColumnT> = vec![
            ColumnT::ColumnFloat(ColumnEnum::VectorColumn(column1)),
            ColumnT::ColumnDouble(ColumnEnum::VectorColumn(column2)),
        ];
        let permutator = Permutator::sort_from_columns(&columnset)
            .expect("Length has been adjusted when generating test-data");
        vec1_cpy.sort_unstable();
        assert_eq!(
            vec1_cpy,
            permutator
                .permutate(&vec1_to_sort)
                .expect("Test case should be sortable")
        );

        let column2: VectorColumn<Double> = VectorColumn::new(vec2);
        let column_sort = permutator.apply_column(
            &ColumnEnum::VectorColumn(column2),
            AdaptiveColumnBuilder::new(),
        );
        assert!(column_sort.is_ok());
        true
    }

    #[test]
    fn multi_column_sort() {
        let vec1: Vec<u64> = vec![0, 0, 2, 2, 1, 1, 4, 4, 3, 3];
        let vec2: Vec<f32> = vec![1.0, 0.0, 0.4, 0.5, 0.8, 0.077, 4.0, 3.0, 1.0, 2.0];
        let vec3: Vec<u64> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let vec2 = vec2
            .iter()
            .map(|elem| Float::new(*elem).expect("value needs to be valid"))
            .collect::<Vec<Float>>();

        let mut acb = AdaptiveColumnBuilder::new();
        vec1.iter().for_each(|elem| acb.add(*elem));
        let column1 = acb.finalize();
        let mut acb = AdaptiveColumnBuilder::new();
        vec2.iter().for_each(|elem| acb.add(*elem));
        let column2 = acb.finalize();
        let mut acb = AdaptiveColumnBuilder::new();
        vec3.iter().for_each(|elem| acb.add(*elem));
        let column3 = acb.finalize();

        let columnset: Vec<ColumnT> =
            vec![ColumnT::ColumnU64(column1), ColumnT::ColumnFloat(column2)];
        let permutator = Permutator::sort_from_columns(&columnset).expect("Sorting should work");
        let column_sort = permutator
            .apply_column(&column3, AdaptiveColumnBuilder::new())
            .expect("application of sorting should work");
        let column_sort_vec: Vec<u64> = column_sort.iter().collect();
        assert_eq!(column_sort_vec, vec![1, 0, 5, 4, 2, 3, 8, 9, 7, 6]);
    }
}
