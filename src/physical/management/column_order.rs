use std::{fmt, ops::Index};

use crate::physical::util::Reordering;

/// Type which represents the order of a table.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ColumnOrder(Vec<usize>);

impl ColumnOrder {
    /// Construct new [`ColumnOrder`].
    pub fn new(order: Vec<usize>) -> Self {
        debug_assert!(!order.is_empty());
        debug_assert!(order.iter().all(|&r| r < order.len()));

        Self(order)
    }

    /// Construct the default [`ColumnOrder`].
    pub fn default(arity: usize) -> Self {
        Self::new((0..arity).collect())
    }

    /// Return the arity of the reordered table.
    pub fn arity(&self) -> usize {
        self.0.len()
    }

    /// Return an iterator over the contents of this order.
    pub fn iter(&self) -> impl Iterator<Item = &usize> {
        self.0.iter()
    }

    /// Returns whether this is the default column order
    pub fn is_default(&self) -> bool {
        self.iter()
            .enumerate()
            .all(|(index, element)| index == *element)
    }

    /// Returns a view into ordering vector.
    pub fn as_slice(&self) -> &[usize] {
        &self.0
    }

    /// Return the [`ColumnOrder`] that results from applying the given [`Reordering`].
    pub fn reorder(&self, reorder: &Reordering) -> Self {
        Self::new(reorder.apply_to(&self.0))
    }

    /// Provides a measure of how "difficult" it is to transform a column with this order into another.
    /// Say, self = [2, 1, 0] and other = [1, 0, 2].
    /// Starting from position 0 one needs to skip one layer to reach the 2 in other (+1).
    /// Then we need to go back two layers to reach the one (+2)
    /// Finally, we go one layer down to reach 0 (+-0).
    /// This gives us an overall score of 3.
    /// Returned value is 0 if and only if self == other.
    fn distance(&self, to: &ColumnOrder) -> usize {
        debug_assert!(to.arity() >= self.arity());

        let mut current_score: usize = 0;
        let mut current_position: usize = 0;

        for &current_value in self.iter() {
            let position_other = to.iter().position(|&o| o == current_value).expect(
                "If both objects are well-formed then other must contain every value of self.",
            );

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
}

impl Index<usize> for ColumnOrder {
    type Output = usize;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl fmt::Debug for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Reordering> for ColumnOrder {
    fn from(reordering: Reordering) -> Self {
        ColumnOrder::new(reordering.iter().copied().collect())
    }
}

impl From<ColumnOrder> for Vec<usize> {
    fn from(order: ColumnOrder) -> Self {
        order.0
    }
}
