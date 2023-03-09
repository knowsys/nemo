use std::fmt;

use crate::physical::util::Reordering;

/// Type which represents the order of a table.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ColumnOrder(Vec<usize>);

impl ColumnOrder {
    /// Transform itself into a canonical representation
    fn reduce(mut self) -> Self {
        let mut num_cuts = 0;

        for (index, &item) in self.0.iter().enumerate().rev() {
            if item == index {
                num_cuts += 1;
            } else {
                break;
            }
        }

        for _ in 0..num_cuts {
            self.0.pop();
        }

        self
    }

    /// Construct new [`ColumnOrder`].
    pub fn new(order: Vec<usize>) -> Self {
        debug_assert!(order.is_empty() || *order.iter().max().unwrap() == order.len() - 1);

        Self(order).reduce()
    }

    /// Construct new [`ColumnOrder`] from a given [`Reordering`].
    /// The reordering must be a permutation
    pub fn from_reordering(reorder: Reordering) -> Self {
        debug_assert!(reorder.is_permutation());

        Self(reorder.to_vector()).reduce()
    }

    /// Returns whether this is the default column order
    pub fn is_default(&self) -> bool {
        self.0.iter().enumerate().all(|(i, &j)| i == j)
    }

    /// Helper function that expands
    fn expand(&self, arity: usize) -> Vec<usize> {
        let mut result = self.0.clone();
        for new_entry in self.0.len()..arity {
            result.push(new_entry);
        }

        result
    }

    /// Return the [`ColumnOrder`] that results from applying the given [`Reordering`].
    /// Assumes the the [`Reordering`] is a permutation.
    pub fn apply_reorder(&self, reorder: &Reordering) -> Self {
        let result = reorder.apply_to(&self.expand(reorder.len_source()));

        Self(result).reduce()
    }

    /// Return [`Reordering`] corresponding to this order.
    pub fn as_reordering(&self, arity: usize) -> Reordering {
        let reordering = self.expand(arity);
        Reordering::new(reordering, arity)
    }

    fn get(&self, index: usize) -> usize {
        if index < self.0.len() {
            self.0[index]
        } else {
            index
        }
    }

    fn find(&self, value: usize) -> usize {
        if let Some(found) = self.0.iter().position(|&o| o == value) {
            found
        } else {
            value
        }
    }

    /// Provides a measure of how "difficult" it is to transform a column with this order into another.
    /// Say, self = [2, 1, 0] and other = [1, 0, 2].
    /// Starting from position 0 one needs to skip one layer to reach the 2 in other (+1).
    /// Then we need to go back two layers to reach the one (+2)
    /// Finally, we go one layer down to reach 0 (+-0).
    /// This gives us an overall score of 3.
    /// Returned value is 0 if and only if self == other.
    pub fn distance(&self, to: &ColumnOrder) -> usize {
        let mut current_score: usize = 0;
        let mut current_position: usize = 0;

        let max_len = self.0.len().max(to.0.len());

        for current_index in 0..max_len {
            let current_value = self.get(current_index);

            let position_other = to.find(current_value);
            let difference: isize = (position_other as isize) - (current_position as isize);

            let penalty: usize = if difference <= 0 {
                difference.unsigned_abs()
            } else {
                // Taking one forward step should not be punished
                (difference - 1) as usize
            };

            current_score += penalty;
            current_position = position_other;
        }

        current_score
    }

    /// Return the [`Reordering`] that is needed to transform this oder into the given one.
    pub fn reorder_to(&self, to: &ColumnOrder, arity: usize) -> Reordering {
        let mut reorder = Vec::<usize>::new();
        for index in 0..arity {
            reorder.push(to.find(self.get(index)));
        }

        Reordering::new(reorder, arity)
    }
}

impl Default for ColumnOrder {
    fn default() -> Self {
        Self(vec![])
    }
}

impl fmt::Debug for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
