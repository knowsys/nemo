// TODO: Check if still needed;
// only use seems to be in variable_order

//! Holds the [Permutator] struct, which allows one to define a logical permutation of the content of index-based data structures

use std::{cmp::Ordering, ops::Range};

use nemo_physical::{
    columnar::{
        column::{Column, ColumnEnum},
        columnbuilder::ColumnBuilder,
    },
    datatypes::{ColumnDataType, StorageTypeName, StorageValueT},
};

use super::trie::VecT;

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
    pub fn sort_from_multiple_vec(data_vec: &[VecT]) -> Option<Permutator> {
        Self::sort_from_multiple_vec_with_offset(data_vec, 0)
    }

    /// Create a [`Permutator`] that sorts the given slice of [`VecT`] lexicographically, with the given `offset`.
    ///
    /// Returns [`Error::PermutationSortLen`] if the given [`VecT`] differ in length.
    pub(crate) fn sort_from_multiple_vec_with_offset(
        data_vec: &[VecT],
        offset: usize,
    ) -> Option<Permutator> {
        let len = if !data_vec.is_empty() {
            let len = data_vec[0].len();
            if data_vec.iter().any(|val| val.len() != len) {
                return None;
            }
            len
        } else {
            0
        };
        let mut vec: Vec<usize> = (0..len).collect::<Vec<usize>>();
        vec.sort_by(|a, b| Self::compare_multiple_vec(*a, *b, data_vec));
        Some(Permutator {
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
    pub fn permute<'a, T>(&'a self, data: &'a [T]) -> Option<impl Iterator<Item = T> + 'a>
    where
        T: Clone,
    {
        if data.len() < (self.sort_vec.len() + self.offset) {
            None
        } else {
            let x = (0..self.offset)
                .chain(self.sort_vec.iter().map(|&idx| idx + self.offset))
                .chain((self.offset + self.sort_vec.len())..data.len());
            Some(x.map(|idx| data[idx].clone()))
        }
    }

    /// Streams the supplied vec in with the computed sort-order
    pub fn permute_streaming(&self, data: VecT) -> Option<PermutatorStream<'_>> {
        if data.len() < (self.sort_vec.len() + self.offset) {
            None
        } else {
            Some(PermutatorStream {
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
    ) -> Option<ColumnEnum<T>>
    where
        T: 'a + ColumnDataType,
        U: ColumnBuilder<'a, T, Col = ColumnEnum<T>>,
    {
        if column.len() < (self.sort_vec.len() + self.offset) {
            None
        } else {
            let iter = (0..self.offset)
                .chain(self.sort_vec.iter().map(|&idx| idx + self.offset))
                .chain((self.offset + self.sort_vec.len())..column.len());
            iter.for_each(|idx| {
                cb.add(column.get(idx));
            });
            Some(cb.finalize())
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
