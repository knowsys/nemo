//! Module for representing an injective function mapping a finite subset of the set of natural numbers the first n natural numbers.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use super::{permutation::Permutation, traits::NatMapping};

/// Injective function mapping a finite subset of the set of natural numbers the first n natural numbers.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FiniteInjective {
    /// Function is represented by a [`HashMap`] mapping the input `i` to `map.get(i)`.
    /// Every input not present in this map is considered not part of this function's domain.
    map: HashMap<usize, usize>,
}

impl FiniteInjective {
    /// Return an instance of the function from a vector representation where the input `vec[i]` is mapped to `i`.
    pub fn from_vector(vec: Vec<usize>) -> Self {
        let mut map = HashMap::<usize, usize>::new();
        for (value, input) in vec.into_iter().enumerate() {
            map.insert(input, value);
        }

        let result = Self { map };

        debug_assert!(result.is_valid());

        result
    }

    /// Return an instance of the function from a hash map representation where the input `i` is mapped to `map.get(i)`.
    pub fn from_map(map: HashMap<usize, usize>) -> Self {
        let result = Self { map };
        debug_assert!(result.is_valid());

        result
    }

    /// Return an iterator over all input/value pairs that are mapped in this funciton.
    pub fn iter(&self) -> impl Iterator<Item = (&usize, &usize)> {
        self.map.iter()
    }

    /// Check whether this function is a permu
    pub fn is_permutation(&self) -> bool {
        // Since this function is finite and injective, it is also surjective.
        // It remains to be checked whether the input domain is the same is the range.
        self.iter().all(|(input, _)| *input < self.map.len())
    }

    /// Turn this function into a permutation.
    /// All inputs outside of this domain will be mapped to themselves.
    pub fn into_permutation(&self) -> Permutation {
        debug_assert!(self.is_permutation());
        Permutation::from_map(self.map.clone())
    }

    /// Transform the given slice according to this function.
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

impl NatMapping for FiniteInjective {
    fn get(&self, input: usize) -> usize {
        *self.map.get(&input).unwrap()
    }

    fn get_many(&self, inputs: &[usize]) -> Vec<usize> {
        inputs.iter().map(|&i| self.get(i)).collect()
    }

    fn chain_permutation(&self, permutation: &Permutation) -> Self {
        let mut result_map = self.map.clone();
        for (_, value) in result_map.iter_mut() {
            *value = permutation.get(*value);
        }

        Self::from_map(result_map)
    }

    fn domain_contains(&self, input: usize) -> bool {
        self.map.contains_key(&input)
    }

    fn is_identity(&self) -> bool {
        self.map.iter().all(|(i, v)| *i == *v)
    }
}

impl Display for FiniteInjective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;

        for (input, value) in &self.map {
            write!(f, "{input}->{value} ")?
        }

        write!(f, "]")
    }
}
