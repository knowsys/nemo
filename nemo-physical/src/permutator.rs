// TODO: Check if still needed;
// only use seems to be in variable_order

//! Holds the [Permutator] struct, which allows one to define a logical permutation of the content of index-based data structures

use crate::{
    columnar::{
        column::{Column, ColumnEnum},
        columnbuilder::ColumnBuilder,
    },
    datatypes::{ColumnDataType, StorageTypeName, StorageValueT},
};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::Range;

use crate::{datatypes::storage_value::VecT, error::Error};

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
    pub(crate) fn sort_from_vec_with_offset<T>(data: &[T], offset: usize) -> Permutator
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

    /// TODO: Test if this works
    /// Creates [`Permutator`] based on [`data`][ColumnEnum]
    pub(crate) fn sort_from_column_range<T>(
        data: &ColumnEnum<T>,
        ranges: &[Range<usize>],
    ) -> Permutator
    where
        T: ColumnDataType + Ord,
    {
        let mut vec = ranges.iter().flat_map(|r| r.clone()).collect::<Vec<_>>();

        vec.sort_by_key(|&i| data.get(i));
        Permutator {
            sort_vec: vec,
            offset: 0,
        }
    }

    /// Creates [`Permutator`] based on [`data`][ColumnEnum]
    pub(crate) fn sort_from_column<T>(data: &ColumnEnum<T>) -> Permutator
    where
        T: ColumnDataType + Ord,
    {
        Permutator::sort_from_column_range(data, &[(0..data.len()); 1])
    }

    /// Create a [`Permutator`] that sorts the given slice of [`VecT`] lexicographically.
    ///
    /// Returns [`Error::PermutationSortLen`] if the given [`VecT`] differ in length.
    pub fn sort_from_multiple_vec(data_vec: &[VecT]) -> Result<Permutator, Error> {
        Self::sort_from_multiple_vec_with_offset(data_vec, 0)
    }

    /// Create a [`Permutator`] that sorts the given slice of [`VecT`] lexicographically, with the given `offset`.
    ///
    /// Returns [`Error::PermutationSortLen`] if the given [`VecT`] differ in length.
    pub(crate) fn sort_from_multiple_vec_with_offset(
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

    /// Returns the vector which contains the sorted indices
    pub(crate) fn get_sort_vec(&self) -> &Vec<usize> {
        &self.sort_vec
    }

    /// Returns the value at a given index or [`None`] if not applicable
    pub(crate) fn value_at(&self, idx: usize) -> Option<usize> {
        self.sort_vec.get(idx.checked_sub(self.offset)?).copied()
    }

    /// Returns the value at a given index or the identity otherwise
    pub(crate) fn value_id(&self, idx: usize) -> usize {
        self.value_at(idx).unwrap_or(idx)
    }

    /// Permutes a given slice of data with the computed sort-order
    pub fn permute<'a, T>(&'a self, data: &'a [T]) -> Result<impl Iterator<Item = T> + 'a, Error>
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
            Ok(x.map(|idx| data[idx].clone()))
        }
    }

    /// Streams the supplied vec in with the computed sort-order
    pub fn permute_streaming(&self, data: VecT) -> Result<PermutatorStream<'_>, Error> {
        if data.len() < (self.sort_vec.len() + self.offset) {
            Err(Error::PermutationApplyWrongLen(
                data.len(),
                self.sort_vec.len(),
                self.offset,
            ))
        } else {
            Ok(PermutatorStream {
                index: 0,
                permutation: self,
                data,
            })
        }
    }

    /// Applies the permutator to a given column by using a provided [`ColumnBuilder`].
    ///
    /// *Returns* either a ['Column'] or an [Error][Error::PermutationApplyWrongLen]
    pub(crate) fn apply_column<'a, T, U>(
        &self,
        column: &ColumnEnum<T>,
        mut cb: U,
    ) -> Result<ColumnEnum<T>, Error>
    where
        T: 'a + ColumnDataType,
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

/// Iterates over data applying a permutation
#[derive(Debug)]
pub(crate) struct PermutatorStream<'a> {
    index: usize,
    permutation: &'a Permutator,
    data: VecT,
}

impl PermutatorStream<'_> {
    /// Returns the type of the iterated elements as [`StorageTypeName`]
    pub(crate) fn get_type(&self) -> StorageTypeName {
        self.data.get_type()
    }
}

impl Iterator for PermutatorStream<'_> {
    type Item = StorageValueT;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.data.len() {
            return None;
        }

        let mut index = self.index;
        self.index += 1;

        if index >= self.permutation.offset {
            index = self
                .permutation
                .sort_vec
                .get(index - self.permutation.offset)
                .map(|i| *i + self.permutation.offset)
                .unwrap_or(index)
        }

        debug_assert!(index < self.data.len());
        Some(self.data.get(index).unwrap())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.data.len() - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for PermutatorStream<'_> {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        columnar::columnbuilder::adaptive::ColumnBuilderAdaptive,
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
                .permute(data)
                .expect("Expect that sorting works in this test-case")
                .collect::<Vec<_>>()
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
                .permute(&vec_u64)
                .expect("Expect that sorting works")
                .collect::<Vec<_>>()
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
                .permute(&vec)
                .expect("Expect that sorting works in this test-case")
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![11, 2, 4, 5, 7, 1, 6, 10, 8, 9, 3, 0],
            permutator
                .permute(&vec2)
                .expect("Expect that sorting works in this test-case")
                .collect::<Vec<_>>()
        );
    }
    #[test]
    fn sort_multiple_from_one_interval() {
        let vec = [10, 5, 1, 9, 2, 3, 5, 4, 7, 8, 6, 0];
        let vec2 = vec![0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2];

        let permutator = Permutator::sort_from_vec_with_offset(&vec[3..5], 3);
        assert_eq!(
            vec![0, 1, 2, 4, 3, 5, 6, 7, 8, 9, 10, 11, 2],
            permutator
                .permute(&vec2)
                .expect("Expect that sorting works in this test-case")
                .collect::<Vec<_>>()
        );

        let permutator = Permutator::sort_from_vec_with_offset(&vec[3..8], 3);
        assert_eq!(
            vec![0, 1, 2, 4, 5, 7, 6, 3, 8, 9, 10, 11, 2],
            permutator
                .permute(&vec2)
                .expect("Expect that sorting works in this test-case")
                .collect::<Vec<_>>()
        );

        let permutator = Permutator::sort_from_vec_with_offset(&vec[4..11], 3);
        assert_eq!(
            vec![0, 1, 2, 3, 4, 6, 5, 9, 7, 8, 10, 11, 2],
            permutator
                .permute(&vec2)
                .expect("Expect that sorting works in this test-case")
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn error_with_permutator() {
        let vec = vec![10, 5, 1, 9, 2, 3, 5, 4, 7, 8, 6, 0];
        let vec2 = vec![0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2];
        let permutator = Permutator::sort_from_vec(&vec2);
        let result = permutator.permute(&vec);
        assert!(result.is_err());
        let Err(err) = result else {
            unreachable!("checked above")
        };
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
            &[VecT::Id64(vec1), VecT::Double(vec2), VecT::Id64(vec3)],
            0,
        )
        .expect("Sort should work in this test case");
        assert_eq!(
            vec![10, 5, 1, 3, 2, 9, 5, 4, 7, 6, 0, 8],
            permutator
                .permute(&checker)
                .expect("Applying permutation should work in this test-case")
                .collect::<Vec<_>>()
        );
    }

    #[quickcheck]
    #[cfg_attr(miri, ignore)]
    fn from_rnd_column(vec: Vec<u32>) -> bool {
        log::debug!("used vector: {:?}", vec);

        let mut builder = ColumnBuilderAdaptive::<u32>::default();
        let mut vec_cpy = vec.clone();
        vec_cpy.sort_unstable();
        vec.iter().for_each(|&elem| builder.add(elem));
        let column = builder.finalize();

        let permutator = Permutator::sort_from_column(&column);
        let sort_vec = permutator
            .permute(&vec)
            .expect("Applying permutation should work in this test-case")
            .collect::<Vec<_>>();
        assert_eq!(sort_vec, vec_cpy);
        true
    }

    // #[quickcheck]
    // #[cfg_attr(miri, ignore)]
    // fn from_rnd_columns(vector1: Vec<f32>, vector2: Vec<f64>) -> bool {
    //     // remove NaN
    //     let mut vec1 = vector1
    //         .iter()
    //         .cloned()
    //         .filter_map(|val| Float::new(val).ok())
    //         .collect::<Vec<Float>>();
    //     let mut vec2 = vector2
    //         .iter()
    //         .filter_map(|val| Double::new(*val).ok())
    //         .collect::<Vec<Double>>();
    //     let len = vec1.len().min(vec2.len());
    //     vec1.resize(len, Float::new(1.0).expect("1.0 is not NaN"));
    //     vec2.resize(len, Double::new(1.0).expect("1.0 is not NaN"));
    //     let mut vec1_cpy = vec1.clone();
    //     let vec1_to_sort = vec1.clone();
    //     let column1: ColumnVector<Float> = ColumnVector::new(vec1);
    //     let column2: ColumnVector<Double> = ColumnVector::new(vec2.clone());
    //     let columnset: Vec<ColumnT> = vec![
    //         ColumnT::Float(ColumnEnum::ColumnVector(column1)),
    //         ColumnT::Double(ColumnEnum::ColumnVector(column2)),
    //     ];
    //     let permutator = Permutator::sort_from_columns(&columnset)
    //         .expect("Length has been adjusted when generating test-data");
    //     vec1_cpy.sort_unstable();
    //     assert_eq!(
    //         vec1_cpy,
    //         permutator
    //             .permute(&vec1_to_sort)
    //             .expect("Test case should be sortable")
    //             .collect::<Vec<_>>()
    //     );

    //     let column2: ColumnVector<Double> = ColumnVector::new(vec2);
    //     let column_sort = permutator.apply_column(
    //         &ColumnEnum::ColumnVector(column2),
    //         ColumnBuilderAdaptive::default(),
    //     );
    //     assert!(column_sort.is_ok());
    //     true
    // }

    // #[test]
    // fn multi_column_sort() {
    //     let vec1: Vec<u64> = vec![0, 0, 2, 2, 1, 1, 4, 4, 3, 3];
    //     let vec2: Vec<f32> = vec![1.0, 0.0, 0.4, 0.5, 0.8, 0.077, 4.0, 3.0, 1.0, 2.0];
    //     let vec3: Vec<u64> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

    //     let vec2 = vec2
    //         .iter()
    //         .map(|elem| Float::new(*elem).expect("value needs to be valid"))
    //         .collect::<Vec<Float>>();

    //     let mut acb = ColumnBuilderAdaptive::default();
    //     vec1.iter().for_each(|elem| acb.add(*elem));
    //     let column1 = acb.finalize();
    //     let mut acb = ColumnBuilderAdaptive::default();
    //     vec2.iter().for_each(|elem| acb.add(*elem));
    //     let column2 = acb.finalize();
    //     let mut acb = ColumnBuilderAdaptive::default();
    //     vec3.iter().for_each(|elem| acb.add(*elem));
    //     let column3 = acb.finalize();

    //     let columnset: Vec<ColumnT> = vec![ColumnT::Id64(column1), ColumnT::Float(column2)];
    //     let permutator = Permutator::sort_from_columns(&columnset).expect("Sorting should work");
    //     let column_sort = permutator
    //         .apply_column(&column3, ColumnBuilderAdaptive::default())
    //         .expect("application of sorting should work");
    //     let column_sort_vec: Vec<u64> = column_sort.iter().collect();
    //     assert_eq!(column_sort_vec, vec![1, 0, 5, 4, 2, 3, 8, 9, 7, 6]);
    // }
}
