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
    // This allows us to have a canonical representation such that we can easily check for equality
    map: HashMap<usize, usize>,
}

impl Permutation {
    /// Return an instance of the function from a vector representation where the input `vec[i]` is mapped to `i`.
    pub fn from_vector(vec: Vec<usize>) -> Self {
        let mut map = HashMap::<usize, usize>::new();

        for (value, input) in vec.into_iter().enumerate() {
            if input == value {
                // Values that map to themselves will be represented implicitly
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
        // Values that map to themselves will not be stored explicitly
        map.retain(|i, v| *i != *v);

        let result = Self { map };
        debug_assert!(result.is_valid());

        result
    }

    /// Return a [Permutation] that when applied to the given slice of values would put them in ascending order.
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

        for (&input, &value) in &self.map {
            for new_index in result.len()..=value {
                result.push(new_index);
            }

            result[value] = input;
        }

        result
    }

    /// Derive a [Permutation] that would transform a vector of elements into another.
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

    /// Compute a [Permutation] that when chained to this [Permutation]
    /// would result in the given target [Permutation].
    pub fn permute_into(&self, target: &Self) -> Self {
        let max_key_source = self.map.keys().max().cloned().unwrap_or(0);
        let max_key_target = target.map.keys().max().cloned().unwrap_or(0);
        let max_key = max_key_source.max(max_key_target);

        let mut result_map = HashMap::new();

        for input in 0..max_key {
            let source_output = self.get(input);
            let target_output = target.get(input);

            result_map.insert(source_output, target_output);
        }

        Self::from_map(result_map)
    }

    /// Return a new [Permutation] that is the inverse of this.
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
        let mut cycles = self.find_cycles();
        for cycle in &mut cycles {
            cycle.sort();
        }
        cycles.sort_by(|c_a, c_b| c_a[0].cmp(&c_b[0]));

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

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{datatypes::Float, util::mapping::traits::NatMapping};

    use super::Permutation;

    #[test]
    fn test_canonical_representation() {
        let map_1 = HashMap::<usize, usize>::from([(0, 0), (1, 1), (2, 2)]);
        let map_2 = HashMap::<usize, usize>::from([(0, 0), (1, 1)]);
        assert_eq!(Permutation::from_map(map_1), Permutation::from_map(map_2));

        let map_1 = HashMap::<usize, usize>::from([(0, 0), (1, 1), (2, 2)]);
        let map_2 = HashMap::<usize, usize>::new();
        assert_eq!(Permutation::from_map(map_1), Permutation::from_map(map_2));

        let map_1 = HashMap::<usize, usize>::from([(0, 0), (1, 2), (2, 1)]);
        let map_2 = HashMap::<usize, usize>::from([(1, 2), (2, 1), (4, 4)]);
        assert_eq!(Permutation::from_map(map_1), Permutation::from_map(map_2));

        let map_1 = HashMap::<usize, usize>::from([(0, 1), (1, 2), (2, 0), (5, 4), (4, 5)]);
        let map_2 = HashMap::<usize, usize>::from([(4, 5), (1, 2), (2, 0), (3, 3), (0, 1), (5, 4)]);
        assert_eq!(Permutation::from_map(map_1), Permutation::from_map(map_2));

        let map_1 = HashMap::<usize, usize>::from([(0, 1), (1, 2), (2, 0), (5, 4), (4, 5)]);
        let map_2 = HashMap::<usize, usize>::from([(0, 1), (1, 2), (2, 0)]);
        assert_ne!(Permutation::from_map(map_1), Permutation::from_map(map_2));

        let map_1 = HashMap::<usize, usize>::from([(0, 1), (1, 0)]);
        let map_2 = HashMap::<usize, usize>::new();
        assert_ne!(Permutation::from_map(map_1), Permutation::from_map(map_2));

        let map_1 = HashMap::<usize, usize>::from([(0, 1), (1, 2), (2, 0), (5, 4), (4, 5)]);
        let map_2 =
            HashMap::<usize, usize>::from([(4, 5), (1, 2), (2, 0), (7, 8), (0, 1), (5, 4), (8, 7)]);
        assert_ne!(Permutation::from_map(map_1), Permutation::from_map(map_2));
    }

    #[test]
    fn test_from_vector() {
        let vector = vec![0, 2, 1];
        let map = HashMap::<usize, usize>::from([(0, 0), (1, 2), (2, 1)]);
        assert_eq!(Permutation::from_vector(vector), Permutation::from_map(map));

        let vector = vec![4, 2, 0, 1, 3];
        let map = HashMap::<usize, usize>::from([(0, 2), (1, 3), (2, 1), (3, 4), (4, 0)]);
        assert_eq!(Permutation::from_vector(vector), Permutation::from_map(map));

        let vector = vec![0, 1, 2];
        let map = HashMap::<usize, usize>::new();
        assert_eq!(Permutation::from_vector(vector), Permutation::from_map(map));

        let vector = vec![2, 0, 3, 1];
        let map = HashMap::<usize, usize>::from([(0, 1), (1, 3), (2, 0), (3, 2)]);
        assert_eq!(Permutation::from_vector(vector), Permutation::from_map(map));
    }

    #[test]
    fn test_from_unsorted() {
        let vec = vec!['A', 'D', 'B', 'C'];
        let map = HashMap::<usize, usize>::from([(1, 3), (3, 2), (2, 1)]);
        assert_eq!(Permutation::from_unsorted(&vec), Permutation::from_map(map));

        let vec = vec![
            Float::new(9.0).unwrap(),
            Float::new(5.2).unwrap(),
            Float::new(1.3).unwrap(),
            Float::new(3.1).unwrap(),
        ];
        let map = HashMap::<usize, usize>::from([(2, 0), (3, 1), (1, 2), (0, 3)]);
        assert_eq!(Permutation::from_unsorted(&vec), Permutation::from_map(map));

        let vec = vec![-8, 12, -100, 1000, 423];
        let map = HashMap::<usize, usize>::from([(2, 0), (0, 1), (1, 2), (4, 3), (3, 4)]);
        assert_eq!(Permutation::from_unsorted(&vec), Permutation::from_map(map));
    }

    #[test]
    fn test_last_mapped() {
        let map = HashMap::<usize, usize>::from([(7, 8), (8, 7), (10, 10), (100, 100)]);
        assert_eq!(Permutation::from_map(map).last_mapped(), Some(8));

        let map = HashMap::<usize, usize>::from([(8, 8), (7, 7), (10, 10), (100, 100)]);
        assert_eq!(Permutation::from_map(map).last_mapped(), None);
    }

    #[test]
    fn test_is_valid() {
        let map = HashMap::<usize, usize>::from([(7, 8), (8, 7), (6, 8)]);
        let perm = Permutation { map };
        assert!(!perm.is_valid());

        let map = HashMap::<usize, usize>::from([(7, 8)]);
        let perm = Permutation { map };
        assert!(!perm.is_valid());
    }

    fn sort_cycles(mut cycles: Vec<Vec<usize>>) -> Vec<Vec<usize>> {
        for cycle in &mut cycles {
            cycle.sort();
        }

        cycles.sort_by(|c_a, c_b| c_a[0].cmp(&c_b[0]));
        cycles
    }

    #[test]
    fn test_find_cycles() {
        let map = HashMap::<usize, usize>::from([(0, 1), (1, 2), (2, 0)]);
        let cycles = sort_cycles(Permutation::from_map(map).find_cycles());
        let expected = vec![vec![0, 1, 2]];
        assert_eq!(cycles, expected);

        let map = HashMap::<usize, usize>::from([(0, 1), (1, 2), (2, 0), (5, 6), (6, 5)]);
        let cycles = sort_cycles(Permutation::from_map(map).find_cycles());
        let expected = vec![vec![0, 1, 2], vec![5, 6]];
        assert_eq!(cycles, expected);

        let map = HashMap::<usize, usize>::from([(0, 2), (2, 1), (1, 4), (4, 3), (3, 0)]);
        let cycles = sort_cycles(Permutation::from_map(map).find_cycles());
        let expected = vec![vec![0, 1, 2, 3, 4]];
        assert_eq!(cycles, expected);
    }

    #[test]
    fn test_to_vector() {
        let vector = vec![0, 2, 1];
        let perm = Permutation::from_vector(vector.clone());
        assert_eq!(perm.to_vector(), vector);

        let vector = vec![4, 2, 0, 1, 3];
        let perm = Permutation::from_vector(vector.clone());
        assert_eq!(perm.to_vector(), vector);

        let vector = vec![0, 3, 1, 2, 4, 5, 6];
        let perm = Permutation::from_vector(vector);
        assert_eq!(perm.to_vector(), vec![0, 3, 1, 2]);

        let vector = vec![0, 1, 2, 4, 3, 5, 6, 9, 7, 8, 10, 11];
        let perm = Permutation::from_vector(vector);
        assert_eq!(perm.to_vector(), vec![0, 1, 2, 4, 3, 5, 6, 9, 7, 8]);
    }

    #[test]
    fn test_from_transformation() {
        let source = vec!['B', 'C', 'A', 'D'];
        let target = vec!['C', 'D', 'B', 'A'];
        let expected = HashMap::<usize, usize>::from([(0, 2), (1, 0), (2, 3), (3, 1)]);
        assert_eq!(
            Permutation::from_transformation(&source, &target),
            Permutation::from_map(expected)
        );

        let source = vec![-10, -100, 5, 20, 90, 50];
        let target = vec![-10, 90, 50, -100, 5, 20];
        let expected =
            HashMap::<usize, usize>::from([(0, 0), (1, 3), (2, 4), (3, 5), (4, 1), (5, 2)]);
        assert_eq!(
            Permutation::from_transformation(&source, &target),
            Permutation::from_map(expected)
        );
    }

    #[test]
    fn test_permute() {
        let vector = vec!['C', 'B', 'A', 'D'];
        let expected = vec!['A', 'B', 'C', 'D'];
        let map = HashMap::<usize, usize>::from([(2, 0), (0, 2)]);
        assert_eq!(Permutation::from_map(map).permute(&vector), expected);

        let vector = vec!['C', 'E', 'B', 'A', 'D'];
        let expected = vec!['A', 'B', 'C', 'D', 'E'];
        let map = HashMap::<usize, usize>::from([(3, 0), (2, 1), (0, 2), (4, 3), (1, 4)]);
        assert_eq!(Permutation::from_map(map).permute(&vector), expected);
    }

    #[test]
    fn test_chain_permutation() {
        let map_a = HashMap::<usize, usize>::from([(3, 0), (2, 1), (0, 2), (4, 3), (1, 4)]);
        let map_b = HashMap::<usize, usize>::from([
            (1, 5),
            (3, 1),
            (5, 4),
            (4, 2),
            (2, 3),
            (0, 7),
            (7, 0),
            (8, 9),
            (9, 8),
        ]);
        let expected = HashMap::<usize, usize>::from([
            (0, 3),
            (1, 2),
            (2, 5),
            (3, 7),
            (4, 1),
            (5, 4),
            (7, 0),
            (8, 9),
            (9, 8),
        ]);
        assert_eq!(
            Permutation::from_map(map_a).chain_permutation(&Permutation::from_map(map_b)),
            Permutation::from_map(expected)
        );
    }

    #[test]
    fn test_invert() {
        let perm = HashMap::from([(1, 2), (2, 0), (0, 1), (4, 5), (5, 4)]);
        let inv = HashMap::from([(2, 1), (0, 2), (1, 0), (4, 5), (5, 4)]);
        assert_eq!(
            Permutation::from_map(perm).invert(),
            Permutation::from_map(inv)
        );
    }
}
