//! Holds the [Permutator] struct, which allows one to define a logical permutation of the content of index-based data structures

use std::cmp::Ordering;

use crate::{error::Error, physical::datatypes::data_value::VecT};

/// Allows one to define a logical permutation of content of index-based data structures
#[derive(Debug, Clone)]
pub struct Permutator {
    sort_vec: Vec<usize>,
    interval_offset: usize,
}

impl Permutator {
    /// Creates a [`Permutator`] based on one slice of sort-able data
    pub fn sort_from_vec<T>(data: &[T]) -> Permutator
    where
        T: PartialEq + PartialOrd + Ord + Eq,
    {
        Self::sort_from_interval(data, 0)
    }

    /// Creates a [`Permutator`] based on a slice, with a given offset.
    pub fn sort_from_interval<T>(data: &[T], offset: usize) -> Permutator
    where
        T: PartialEq + PartialOrd + Ord + Eq,
    {
        let mut vec: Vec<usize> = (0..data.len()).collect::<Vec<usize>>();
        vec.sort_by_key(|&i| &data[i]);
        Permutator {
            sort_vec: vec,
            interval_offset: offset,
        }
    }

    /// Creates a [`Permutator`] based on one a list of [`VecT`][crate::physical::datatypes::data_value::VecT].
    ///
    /// The sorting of values is done by taking the first [`VecT`][crate::physical::datatypes::data_value::VecT] and sort according to these values.
    /// If two elements are the equal to each other, the next [`VecT`][crate::physical::datatypes::data_value::VecT] will be taken to check if the comparison is different.
    /// If all [`VecT`][crate::physical::datatypes::data_value::VecT] comparisons result in equality, the original order of the two values is preserved.
    ///
    /// Returns an Error, if the length of the given [`VecT`][crate::physical::datatypes::data_value::VecT] are different.
    pub fn sort_from_multipe_vec(data_vec: &[VecT]) -> Result<Permutator, Error> {
        Self::sort_from_multipe_vec_interval(data_vec, 0)
    }

    /// Creates a [`Permutator`] based on one a list of [`VecT`][crate::physical::datatypes::data_value::VecT], with a given offset.
    ///
    /// The sorting of values is done by taking the first [`VecT`][crate::physical::datatypes::data_value::VecT] and sort according to these values.
    /// If two elements are the equal to each other, the next [`VecT`][crate::physical::datatypes::data_value::VecT] will be taken to check if the comparison is different.
    /// If all [`VecT`][crate::physical::datatypes::data_value::VecT] comparisons result in equality, the original order of the two values is preserved.
    ///
    /// Returns an Error, if the length of the given [`VecT`][crate::physical::datatypes::data_value::VecT] are different.
    pub fn sort_from_multipe_vec_interval(
        data_vec: &[VecT],
        offset: usize,
    ) -> Result<Permutator, Error> {
        let len = if !data_vec.is_empty() {
            let len = data_vec[0].len();
            if data_vec.iter().any(|val| val.len() != len) {
                return Err(Error::Permutation(
                    "The different data-slices have not the same length".to_string(),
                ));
            }
            len
        } else {
            0
        };
        let mut vec: Vec<usize> = (0..len).collect::<Vec<usize>>();
        vec.sort_by(|a, b| Self::compare_multiple(*a, *b, data_vec));
        Ok(Permutator {
            sort_vec: vec,
            interval_offset: offset,
        })
    }

    fn compare_multiple(a: usize, b: usize, data_vec: &[VecT]) -> Ordering {
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

    /// Returns the value at a given index or [`None`] if not applicable
    pub fn value_at(&self, idx: usize) -> Option<usize> {
        let idx = match idx.checked_sub(self.interval_offset) {
            Some(val) => val,
            None => return None,
        };
        self.sort_vec.get(idx).copied()
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
        if data.len() < (self.sort_vec.len() + self.interval_offset) {
            Err(Error::Permutation(format!(
                "Permutation data length ({0}) is smaller than the sort_vec length ({1}) + the offset of {2}",
                data.len(),
                self.sort_vec.len(),
		self.interval_offset,
            )))
        } else {
            let x = (0..self.interval_offset)
                .chain(self.sort_vec.iter().map(|&idx| idx + self.interval_offset))
                .chain((self.interval_offset + self.sort_vec.len())..data.len());
            Ok(x.map(|idx| data[idx].clone()).collect::<Vec<_>>())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::physical::datatypes::{Double, Float};
    use quickcheck_macros::quickcheck;
    use test_log::test;

    fn apply_sort_permutator<T>(data: &[T])
    where
        T: PartialEq + PartialOrd + Ord + Eq + Clone + Copy + std::fmt::Debug,
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
    fn sort_from_vec(
        vec_u64: Vec<u64>,
        vec_i64: Vec<i64>,
        vec_f64: Vec<f64>,
        vec_f32: Vec<f32>,
    ) -> bool {
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

        let permutator = Permutator::sort_from_interval(&vec[3..5], 3);
        assert_eq!(
            vec![0, 1, 2, 4, 3, 5, 6, 7, 8, 9, 10, 11, 2],
            permutator
                .permutate(&vec2)
                .expect("Expect that sorting works in this test-case")
        );

        let permutator = Permutator::sort_from_interval(&vec[3..8], 3);
        assert_eq!(
            vec![0, 1, 2, 4, 5, 7, 6, 3, 8, 9, 10, 11, 2],
            permutator
                .permutate(&vec2)
                .expect("Expect that sorting works in this test-case")
        );

        let permutator = Permutator::sort_from_interval(&vec[4..11], 3);
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
            Error::Permutation(_) => (),
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

        let permutator = Permutator::sort_from_multipe_vec_interval(
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
}
