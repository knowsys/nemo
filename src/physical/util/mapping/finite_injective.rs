//! Module for representing an injective function mapping the first n natural numbers to the set of natural numbers.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use super::traits::NatMapping;

/// Injective function mapping the first n natural numbers to the set of natural numbers.
#[derive(Debug, Eq, PartialEq)]
pub struct FiniteInjective {
    // Function is represented by a vector mapping the value `i` to `map[i]`
    vec: Vec<usize>,
}

impl FiniteInjective {
    // In order for the internal representation to be valid
    // no two inputs must be mapped to the same value, i.e. the vector must only contain distinct elements.
    fn is_valid(&self) -> bool {
        let value_set: HashSet<&usize> = self.vec.iter().collect();
        value_set.len() == self.vec.len()
    }
}

impl NatMapping for FiniteInjective {
    fn from_vector(vec: Vec<usize>) -> Self {
        let result = Self { vec };
        debug_assert!(result.is_valid());

        result
    }

    fn from_map(map: HashMap<usize, usize>) -> Self {
        let mut vec = Vec::<usize>::new();

        for input in 0..map.len() {
            vec.push(*map.get(&input).expect(
                "Function assumes that the map contains a mapping for every input in the domain.",
            ));
        }

        let result = Self { vec };
        debug_assert!(result.is_valid());

        result
    }

    fn get(&self, input: usize) -> usize {
        self.vec[input]
    }

    fn get_many(&self, inputs: &[usize]) -> Vec<usize> {
        inputs.iter().map(|&i| self.get(i)).collect()
    }

    fn chain_permutation(&self, permutation: &super::permutation::Permutation) -> Self {
        let result_vec = permutation.get_many(&self.vec);
        Self::from_vector(result_vec)
    }

    fn domain_contains(&self, value: usize) -> bool {
        value < self.vec.len()
    }

    fn is_identity(&self) -> bool {
        self.vec.iter().enumerate().all(|(i, v)| i == *v)
    }
}

impl Display for FiniteInjective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.vec)
    }
}
