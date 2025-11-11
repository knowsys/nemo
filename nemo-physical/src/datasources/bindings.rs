//! This module defines a data structure for sets of variable bindings.

use itertools::Itertools;

use crate::datavalues::AnyDataValue;

/// A set of bindings for some positions
#[derive(Debug, Clone)]
pub struct Bindings {
    positions: Vec<usize>,
    bindings: Vec<Vec<AnyDataValue>>,
    count: usize,
}

impl Bindings {
    /// Creates a new set of bindings for the given positions
    pub fn new(positions: &[usize], bindings: &[Vec<AnyDataValue>]) -> Self {
        Self {
            positions: positions.to_vec(),
            bindings: bindings.to_vec(),
            count: bindings.len(),
        }
    }

    /// Creates a new, empty binding set for the given positions.
    pub fn empty(positions: &[usize]) -> Self {
        Self {
            positions: positions.to_vec(),
            bindings: Vec::new(),
            count: 0,
        }
    }

    /// Creates a new binding set from the union of the two given binding sets.
    fn union(left: &Bindings, right: &Bindings) -> Self {
        debug_assert_eq!(left.positions(), right.positions());

        let mut bindings = left.bindings.clone();
        bindings.extend(right.bindings.clone());

        Self {
            positions: left.positions.clone(),
            bindings,
            count: left.count + right.count,
        }
    }

    /// Split the bindings set into chunks not greater than the given size.
    pub fn chunks(&self, size: usize) -> impl Iterator<Item = Self> {
        let mut result = Vec::new();

        for chunk in &self.bindings.iter().chunks(size) {
            let bindings = chunk.cloned().collect::<Vec<_>>();

            result.push(Self::new(&self.positions, &bindings))
        }

        result.into_iter()
    }

    /// Checks whether this binding set is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the number of bindings.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Returns the bound positions.
    pub fn positions(&self) -> &[usize] {
        &self.positions
    }

    /// Returns the bindings.
    pub fn bindings(&self) -> &[Vec<AnyDataValue>] {
        &self.bindings
    }
}

/// A cartesian product of several sets of bindings
#[derive(Debug)]
pub struct ProductBindings {
    bindings: Vec<(Bindings, Bindings)>,
}

impl ProductBindings {
    /// Creates a new product of bindings from a single factor.
    pub fn new(bindings: &Bindings) -> Self {
        Self {
            bindings: vec![(Bindings::empty(bindings.positions()), bindings.clone())],
        }
    }

    /// Creates a new product of bindings from the given factors. Each
    /// factor is split into old and new bindings.
    pub fn product(bindings: impl IntoIterator<Item = (Bindings, Bindings)>) -> Self {
        Self {
            bindings: bindings.into_iter().collect(),
        }
    }

    /// Returns all combinations of factors with at least one new binding.
    pub fn combinations(&self) -> impl Iterator<Item = Vec<Bindings>> {
        let mut result = Vec::new();

        for (idx, (_old, new)) in self.bindings.iter().enumerate() {
            if new.is_empty() {
                continue;
            }

            let mut bindings = Vec::new();
            bindings.push(new.clone());

            for (idx_other, (old_other, new_other)) in self.bindings.iter().enumerate() {
                if idx == idx_other || (old_other.is_empty() && new_other.is_empty()) {
                    continue;
                }

                bindings.push(Bindings::union(old_other, new_other));
            }

            result.push(bindings);
        }

        result.into_iter()
    }

    /// Returns the total number of bindings over all combinations
    /// involving new bindings.
    pub fn count(&self) -> usize {
        self.combinations()
            .map(|combination| {
                combination
                    .iter()
                    .map(|bindings| bindings.count())
                    .product::<usize>()
            })
            .sum()
    }
}
