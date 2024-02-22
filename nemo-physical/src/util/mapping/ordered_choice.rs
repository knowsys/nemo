//! Module for defining a function that represents an ordered choosing from a sorted collection.

use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
};

use super::{permutation::Permutation, traits::NatMapping};

/// Function that representes an ordered choosing from a sorted collection.
/// In a mathematical sense, may be viewed as a partial function \[n\] -> \[n\] that is injective.
#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct SortedChoice {
    /// Function is represented by a [`HashMap`] mapping the input `i` to `map.get(i)`.
    /// Every input not present in this map is considered not part of this function's domain.
    map: HashMap<usize, usize>,
    /// Size of the domain. All inputs must be smaller than this.
    domain_size: usize,
}

impl SortedChoice {
    /// Return an instance of the function from a hash map representation where the input `i` is mapped to `map.get(i)`.
    pub(crate) fn from_map(map: HashMap<usize, usize>, domain_size: usize) -> Self {
        let result = Self { map, domain_size };
        debug_assert!(result.is_valid());

        result
    }

    /// Derive a [`SortedChoice`] that would transform a vector of elements into another.
    /// I.e. `this.permute(source) = target`
    /// For example `from_transformation([x, y, z, w], [z, w, x]) = {0->2, 2->0, 3->1}`.
    ///
    /// # Panics
    /// Panics if there is a source element that does not appear in the target.
    pub(crate) fn from_transformation<T: PartialEq>(source: &[T], target: &[T]) -> Self {
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

    /// Return the size of the domain.
    pub(crate) fn domain_size(&self) -> usize {
        self.domain_size
    }

    /// Return a vector representation of the function
    /// such that the ith entry in the vector contains that element which gets mapped to position i by this function.
    /// E.g. `{10 -> 0, 20 -> 1}` will result in `[10, 20]`
    #[cfg(test)]
    pub(crate) fn to_vector(&self) -> Vec<usize> {
        let mut result = vec![0; self.map.len()];

        for (input, value) in self.iter() {
            result[*value] = *input;
        }

        result
    }

    /// Return an iterator over all input/value pairs that are mapped in this funciton.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&usize, &usize)> {
        self.map.iter()
    }

    /// Check whether this partial function is a permutation.
    /// This is equivalent to checking whether every input is assigned to a value.
    pub(crate) fn is_permutation(&self) -> bool {
        self.map.len() == self.domain_size
    }

    /// Turn this function into a permutation.
    /// All inputs outside of this domain will be mapped to themselves.
    #[cfg(test)]
    pub(crate) fn into_permutation(&self) -> Permutation {
        debug_assert!(self.is_permutation());
        Permutation::from_map(self.map.clone())
    }

    /// Transform the given slice according to this mapping.
    #[allow(dead_code)]
    pub(crate) fn transform<T: Clone + Debug>(&self, vec: &[T]) -> Vec<T> {
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
        for (&input, &value) in self.iter() {
            if !value_set.insert(value) {
                return false;
            }

            if value >= self.map.len() {
                return false;
            }

            if input >= self.domain_size {
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

#[cfg(test)]
impl SortedChoice {
    /// Return an instance of the function from a vector representation where the input `vec[i]` is mapped to `i`.

    pub(crate) fn from_vector(vec: Vec<usize>, domain_size: usize) -> Self {
        let mut map = HashMap::<usize, usize>::new();
        for (value, input) in vec.into_iter().enumerate() {
            map.insert(input, value);
        }

        let result = Self { map, domain_size };

        debug_assert!(result.is_valid());

        result
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::util::mapping::{permutation::Permutation, traits::NatMapping};

    use super::SortedChoice;

    #[test]
    fn test_from_vector() {
        let vector = vec![0, 2, 1];
        let map = HashMap::<usize, usize>::from([(0, 0), (1, 2), (2, 1)]);
        assert_eq!(
            SortedChoice::from_vector(vector, 3),
            SortedChoice::from_map(map, 3)
        );

        let vector = vec![3, 1, 2];
        let map = HashMap::<usize, usize>::from([(3, 0), (1, 1), (2, 2)]);
        assert_eq!(
            SortedChoice::from_vector(vector, 5),
            SortedChoice::from_map(map, 5)
        );

        let vector = vec![0, 1, 2];
        let map = HashMap::<usize, usize>::from([(0, 0), (1, 1), (2, 2)]);
        assert_eq!(
            SortedChoice::from_vector(vector, 3),
            SortedChoice::from_map(map, 3)
        );
    }

    #[test]
    fn test_from_transformation() {
        let source = vec!['A', 'B', 'C', 'D'];
        let target = vec!['B', 'A', 'D'];
        let expected = HashMap::<usize, usize>::from([(0, 1), (1, 0), (3, 2)]);
        assert_eq!(
            SortedChoice::from_transformation(&source, &target),
            SortedChoice::from_map(expected, 4)
        );

        let source = vec![1.0, 2.0, 3.0];
        let target = vec![3.0, 1.0, 2.0];
        let expected = HashMap::<usize, usize>::from([(0, 1), (1, 2), (2, 0)]);
        assert_eq!(
            SortedChoice::from_transformation(&source, &target),
            SortedChoice::from_map(expected, 3)
        );
    }

    #[test]
    fn test_to_vector() {
        let vector = vec![0, 2, 1];
        let choice = SortedChoice::from_vector(vector.clone(), 3);
        assert_eq!(vector, choice.to_vector());

        let vector = vec![3, 1, 2];
        let choice = SortedChoice::from_vector(vector.clone(), 4);
        assert_eq!(vector, choice.to_vector());

        let vector = vec![0, 1, 2];
        let choice = SortedChoice::from_vector(vector.clone(), 4);
        assert_eq!(vector, choice.to_vector());
    }

    #[test]
    fn test_is_permutation() {
        let choice = SortedChoice::from_vector(vec![2, 0, 1], 3);
        assert!(choice.is_permutation());

        let choice = SortedChoice::from_vector(vec![2, 0, 1], 4);
        assert!(!choice.is_permutation());
    }

    #[test]
    fn test_into_permutation() {
        let vector = vec![2, 0, 1];
        let choice = SortedChoice::from_vector(vector.clone(), vector.len());
        let permutation = Permutation::from_vector(vector);
        assert_eq!(permutation, choice.into_permutation());

        let vector = vec![2, 0, 1, 4, 3];
        let choice = SortedChoice::from_vector(vector.clone(), vector.len());
        let permutation = Permutation::from_vector(vector);
        assert_eq!(permutation, choice.into_permutation());
    }

    #[test]
    fn test_transform() {
        let vector = vec!['A', 'B', 'C', 'D'];
        let choice = SortedChoice::from_vector(vec![2, 3, 0], 4);
        assert_eq!(choice.transform(&vector), vec!['C', 'D', 'A']);

        let vector = vec![5.0, 1.0, -3.0];
        let choice = SortedChoice::from_vector(vec![2, 1, 0], 4);
        assert_eq!(choice.transform(&vector), vec![-3.0, 1.0, 5.0]);
    }

    #[test]
    fn test_is_valid() {
        let map = HashMap::<usize, usize>::from([(3, 2)]);
        let choice = SortedChoice {
            map,
            domain_size: 4,
        };
        assert!(!choice.is_valid());

        let map = HashMap::<usize, usize>::from([(2, 1), (1, 0), (2, 1)]);
        let choice = SortedChoice {
            map,
            domain_size: 2,
        };
        assert!(!choice.is_valid());

        let map = HashMap::<usize, usize>::from([(1, 0), (0, 0)]);
        let choice = SortedChoice {
            map,
            domain_size: 2,
        };
        assert!(!choice.is_valid());

        let map = HashMap::<usize, usize>::from([(1, 0), (0, 1), (5, 2)]);
        let choice = SortedChoice {
            map,
            domain_size: 2,
        };
        assert!(!choice.is_valid());
    }

    #[test]
    fn test_chain_permutation() {
        let choice = SortedChoice::from_map(HashMap::from([(0, 1), (1, 2), (2, 0)]), 3);
        let permutation = Permutation::from_map(HashMap::from([(0, 1), (1, 2), (2, 0)]));
        let expected = SortedChoice::from_map(HashMap::from([(0, 2), (1, 0), (2, 1)]), 3);
        assert_eq!(choice.chain_permutation(&permutation), expected);

        let choice = SortedChoice::from_map(HashMap::from([(1, 0), (2, 1)]), 4);
        let permutation = Permutation::from_map(HashMap::from([(0, 1), (1, 0), (3, 4), (4, 3)]));
        let expected = SortedChoice::from_map(HashMap::from([(1, 1), (2, 0)]), 4);
        assert_eq!(choice.chain_permutation(&permutation), expected);
    }
}
