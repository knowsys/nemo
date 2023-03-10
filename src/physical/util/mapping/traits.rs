//! Module defining traits for representing functions.

use std::collections::HashMap;
use std::fmt::{Debug, Display};

use super::permutation::Permutation;

/// Trait that represents a function from a subset of natural numbers to the set of natural numbers.
pub trait NatMapping: Debug + Display + PartialEq + Eq {
    /// Return an instance of the function from a vector representation where the input `i` is mapped to `vec[i]`.
    fn from_vector(vec: Vec<usize>) -> Self;

    /// Return an instance of the function from a hash map representation where the input `i` is mapped to `map.get(i)`.
    fn from_map(map: HashMap<usize, usize>) -> Self;

    /// Return the value of the function for a given input.
    /// Panics if the input is outside of the function's domain.
    fn get(&self, input: usize) -> usize;

    /// Apply the function to a list of inputs.
    fn get_many(&self, inputs: &[usize]) -> Vec<usize>;

    /// Return the function which results from chaining `self` with a given [`Permutation`].
    fn chain_permutation(&self, permutation: &Permutation) -> Self;

    /// Check whether a given input is part of the domain of the function.
    fn domain_contains(&self, value: usize) -> bool;

    /// Returns true iff the function maps each input in its domain to itself.
    fn is_identity(&self) -> bool;
}
