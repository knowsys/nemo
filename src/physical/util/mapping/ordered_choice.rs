//! Module for defining a function that represents an ordered choosing from a sorted collection.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    ptr,
};

use super::{permutation::Permutation, traits::NatMapping};

/// Function that representes an ordered choosing from a sorted collection.
/// In a mathematical sense, may be viewed as an partial function [n] -> [n] that is injective.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct SortedChoice {
    /// Function is represented by a [`HashMap`] mapping the input `i` to `map.get(i)`.
    /// Every input not present in this map is considered not part of this function's domain.
    map: HashMap<usize, usize>,
    /// Size of the domain. All inputs must be smaller than this.
    domain_size: usize,
}

impl SortedChoice {
    /// Return an instance of the function from a vector representation where the input `vec[i]` is mapped to `i`.
    pub fn from_vector(vec: Vec<usize>, domain_size: usize) -> Self {
        let mut map = HashMap::<usize, usize>::new();
        for (value, input) in vec.into_iter().enumerate() {
            map.insert(input, value);
        }

        let result = Self { map, domain_size };

        debug_assert!(result.is_valid());

        result
    }

    /// Return an instance of the function from a hash map representation where the input `i` is mapped to `map.get(i)`.
    pub fn from_map(map: HashMap<usize, usize>, domain_size: usize) -> Self {
        let result = Self { map, domain_size };
        debug_assert!(result.is_valid());

        result
    }

    /// Derive a [`SortedChoice`] that would transform a vector of elements into another.
    /// I.e. `this.permute(source) = target`
    /// For example `from_transformation([x, y, z, w], [z, w, x]) = {0->2, 2->0, 3->1}`.
    pub fn from_transformation<T: PartialEq>(source: &[T], target: &[T]) -> Self {
        debug_assert!(source.len() >= target.len());

        let mut map = HashMap::<usize, usize>::new();
        for (target_index, target_value) in target.iter().enumerate() {
            let source_index = source
                .iter()
                .position(|s| *target_value == *s)
                .expect("We expect that target only uses elements from source.");

            map.insert(source_index, target_index);
        }

        Self::from_map(map, source.len())
    }

    /// Return a vector representation of the function
    /// such that the ith entry in the vector contains that element which gets mapped to position i by this function.
    /// E.g. `{10 -> 0, 20 -> 1}` will result in `[10, 20]`
    pub fn to_vector(&self) -> Vec<usize> {
        let mut result = vec![0; self.map.len()];

        for (input, value) in self.iter() {
            result[*value] = *input;
        }

        result
    }

    /// Return an iterator over all input/value pairs that are mapped in this funciton.
    pub fn iter(&self) -> impl Iterator<Item = (&usize, &usize)> {
        self.map.iter()
    }

    /// Check whether this partial function is a permutation.
    /// This is equivalent to checking whether every input is assigned to a value.
    pub fn is_permutation(&self) -> bool {
        self.map.len() == self.domain_size
    }

    /// Turn this function into a permutation.
    /// All inputs outside of this domain will be mapped to themselves.
    pub fn into_permutation(&self) -> Permutation {
        debug_assert!(self.is_permutation());
        Permutation::from_map(self.map.clone())
    }

    /// Transform the given vector of elements according to this mapping.
    /// This will avoid copying the underlying data.
    pub fn transform_consumed<T: Default>(&self, mut vec: Vec<T>) -> Vec<T> {
        let mut result = std::iter::repeat_with(|| T::default())
            .take(self.map.len())
            .collect::<Vec<_>>();

        for (&input, &value) in self.iter() {
            unsafe {
                let input_element: *mut T = &mut vec[input];
                let output_element: *mut T = &mut result[value];

                ptr::swap(input_element, output_element);
            }
        }

        result
    }

    /// Transform the given slice according to this mapping.
    pub fn transform<T: Clone>(&self, vec: &[T]) -> Vec<T> {
        let mut result: Vec<T> = vec.iter().take(self.map.len()).cloned().collect();
        for (input, value) in self.map.iter() {
            result[*value] = vec[*input].clone();
        }

        result
    }

    /// In order for the internal representation to be valid
    /// no two inputs must be mapped to the same value, i.e. the vector must only contain distinct elements.
    /// Also there may not be any function value that is higher than the size of the domain.
    fn is_valid(&self) -> bool {
        let mut value_set = HashSet::<usize>::new();
        for (_, value) in self.iter() {
            if !value_set.insert(*value) {
                return false;
            }

            if *value >= self.map.len() {
                return false;
            }
        }

        true
    }
}

impl NatMapping for SortedChoice {
    fn get_partial(&self, input: usize) -> Option<usize> {
        self.map.get(&input).cloned()
    }

    fn chain_permutation(&self, permutation: &Permutation) -> Self {
        let mut result_map = self.map.clone();
        for (_, value) in result_map.iter_mut() {
            *value = permutation.get(*value);
        }

        Self::from_map(result_map, self.domain_size)
    }

    fn domain_contains(&self, input: usize) -> bool {
        self.map.contains_key(&input)
    }

    fn is_identity(&self) -> bool {
        if !self.is_permutation() {
            return false;
        }

        self.map.iter().all(|(i, v)| *i == *v)
    }
}

impl Display for SortedChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;

        for (input, value) in &self.map {
            write!(f, "{input}->{value} ")?
        }

        write!(f, "]")
    }
}
