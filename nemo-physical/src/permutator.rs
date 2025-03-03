// TODO: Check if still needed;
// only use seems to be in variable_order

//! Holds the [Permutator] struct, which allows one to define a logical permutation of the content of index-based data structures

use crate::{
    columnar::{
        column::{Column, ColumnEnum},
        columnbuilder::ColumnBuilder,
    },
    storagevalues::ColumnDataType,
};
use std::fmt::Debug;

use crate::error::Error;

/// Allows one to define a logical permutation of content of index-based data structures
#[derive(Debug, Clone)]
pub struct Permutator {
    sort_vec: Vec<usize>,
    offset: usize,
}

impl Permutator {
    /// Creates a [Permutator] based on one slice of sort-able data
    pub fn sort_from_vec<T>(data: &[T]) -> Permutator
    where
        T: PartialEq + PartialOrd + Ord + Eq,
    {
        Self::sort_from_vec_with_offset(data, 0)
    }

    /// Creates a [Permutator] based on a slice, with a given offset.
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

    /// Applies the permutator to a given column by using a provided [ColumnBuilder].
    ///
    /// *Returns* either a ['Column'] or an [Error][Error::PermutationApplyWrongLen]
    pub(crate) fn _apply_column<'a, T, U>(
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::storagevalues::{Double, Float};
    use quickcheck_macros::quickcheck;
    #[cfg(not(miri))]
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
}
