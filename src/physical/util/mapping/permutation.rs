//! Module for representing a permutation on natural numbers

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    hash::{Hash, Hasher},
};

use super::traits::NatMapping;

/// Represents a permutation on the set of natural numbers.
#[derive(Debug, Default, Eq, Clone)]
pub struct Permutation {
    // The function represented by this object will map `i` to `map.get(i)`.
    // All inputs that are not keys in this map are implicitly mapped to themselves.
    map: HashMap<usize, usize>,
}

impl Permutation {
    /// Return an instance of the function from a vector representation where the input `vec[i]` is mapped to `i`.
    pub fn from_vector(vec: Vec<usize>) -> Self {
        let mut map = HashMap::<usize, usize>::new();

        for (value, input) in vec.into_iter().enumerate() {
            if input == value {
                // Values that map to themselves will be represented implicitly
                // This also allows us to have a canonical representation such that we can easily check for equality
                continue;
            }

            map.insert(input, value);
        }

        let result = Self { map };
        debug_assert!(result.is_valid());

        result
    }

    /// Return an instance of the function from a hash map representation where the input `i` is mapped to `map.get(i)`.
    pub fn from_map(mut map: HashMap<usize, usize>) -> Self {
        // Values that map to themselves will be represented implicitly
        // This also allows us to have a canonical representation such that we can easily check for equality
        map.retain(|i, v| *i != *v);

        let result = Self { map };
        debug_assert!(result.is_valid());

        result
    }

    /// Return a [`Permutation`] that when applied to the given slice of values would put them in ascending order.
    pub fn from_unsorted<T: Ord>(values: &[T]) -> Self {
        let mut result: Vec<usize> = (0..values.len()).collect();
        result.sort_by(|&a, &b| values[a].cmp(&values[b]));

        Self::from_vector(result)
    }

    /// Return the largest input value that is not mapped to itself.
    pub fn last_mapped(&self) -> Option<usize> {
        self.map.keys().max().copied()
    }

    /// In order for the internal representation to be a valid permutation we need to check
    /// that no two inputs map to the same value.
    fn is_valid(&self) -> bool {
        let mut values = HashSet::<usize>::new();

        for value in self.map.values() {
            // Value has been explicitly mapped twice
            if !values.insert(*value) {
                return false;
            }

            // Value has been implicitly mapped twice
            if !self.map.contains_key(value) {
                return false;
            }
        }

        true
    }

    /// Return a list of all cycles of length greater than 1.
    /// A cycle is a list of inputs such that the (i+1)th results from applying the permutation to the ith element.
    /// and the first can be obtained by applying it to the last.
    fn find_cycles(&self) -> Vec<Vec<usize>> {
        let mut result = Vec::<Vec<usize>>::new();

        let mut input_set: HashSet<usize> = self.map.keys().cloned().collect();
        while let Some(start) = input_set.iter().next().cloned() {
            let mut current_cycle = Vec::<usize>::new();
            current_cycle.push(start);

            input_set.remove(&start);

            let mut current_value = start;
            loop {
                let next_value = self.get(current_value);
                input_set.remove(&next_value);

                current_value = next_value;

                if current_value == start {
                    break;
                }

                current_cycle.push(current_value);
            }

            result.push(current_cycle);
        }

        result
    }

    /// Return a vector represenation of the permutation.
    fn to_vector(&self) -> Vec<usize> {
        let mut result = Vec::<usize>::new();

        let mut used_map_keys = 0usize;
        let mut current_input = 0usize;
        while used_map_keys < self.map.len() {
            if let Some(value) = self.map.get(&current_input) {
                result.push(*value);
                used_map_keys += 1;
            } else {
                result.push(current_input);
            }

            current_input += 1;
        }

        result
    }

    /// Derive a [`Permutation`] that would transform a vector of elements into another.
    /// I.e. `this.permute(source) = target`
    /// For example `from_transformation([x, y, z, w], [z, w, y, x]) = {0->3, 1->2, 2->0, 3->1}`.
    pub fn from_transformation<T: PartialEq>(source: &[T], target: &[T]) -> Self {
        debug_assert!(source.len() == target.len());

        let mut map = HashMap::<usize, usize>::new();
        for (input_index, source_value) in source.iter().enumerate() {
            let target_index = target
                .iter()
                .position(|t| *source_value == *t)
                .expect("We expect that target only uses elements from source.");

            map.insert(input_index, target_index);
        }

        Self::from_map(map)
    }

    /// Return a new [`Permutation`] that is the inverse of this.
    pub fn invert(&self) -> Self {
        let mut map = HashMap::<usize, usize>::new();

        for (input, value) in &self.map {
            map.insert(*value, *input);
        }

        Self { map }
    }

    /// Apply this permutation to a slice of things.
    pub fn permute<T: Clone>(&self, vec: &[T]) -> Vec<T> {
        let mut result: Vec<T> = vec.to_vec();
        for (input, value) in self.map.iter() {
            result[*value] = vec[*input].clone();
        }

        result
    }

    /// Return the value of the function for a given input.
    pub fn get(&self, input: usize) -> usize {
        self.get_partial(input)
            .expect("Permutations map each value")
    }
}

impl NatMapping for Permutation {
    fn get_partial(&self, input: usize) -> Option<usize> {
        Some(*self.map.get(&input).unwrap_or(&input))
    }

    fn chain_permutation(&self, permutation: &Permutation) -> Self {
        let mut result_map = HashMap::<usize, usize>::new();

        for (input, value) in &self.map {
            let chained_value = permutation.get(*value);
            if chained_value == *input {
                continue;
            }

            result_map.insert(*input, chained_value);
        }

        for (input, value) in &permutation.map {
            if self.map.contains_key(input) {
                continue;
            }

            result_map.insert(*input, *value);
        }

        Self { map: result_map }
    }

    fn domain_contains(&self, _value: usize) -> bool {
        true
    }

    fn is_identity(&self) -> bool {
        self.map.is_empty()
    }
}

impl Display for Permutation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cycles = self.find_cycles();

        write!(f, "[")?;
        for (cycle_index, cycle) in cycles.into_iter().enumerate() {
            let last_cycle = cycle_index < cycle.len() - 1;

            if cycle.len() == 2 {
                write!(f, "{}<->{}", cycle[0], cycle[1])?;
            } else {
                let first_element = cycle[0];

                for element in cycle {
                    write!(f, "{element}->")?;
                }

                write!(f, "{first_element}")?;
            }

            if !last_cycle {
                write!(f, ", ")?
            }
        }

        write!(f, "]")
    }
}

impl Hash for Permutation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_vector().hash(state);
    }
}

impl PartialEq for Permutation {
    fn eq(&self, other: &Self) -> bool {
        self.to_vector() == other.to_vector()
    }
}
