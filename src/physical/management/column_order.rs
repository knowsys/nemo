use std::fmt;

use crate::physical::util::Reordering;

/// Type which represents the order of a table.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ColumnOrder {
    /// The default ordering.
    Default,
    /// Ordering that switches certain columns (relative to the default ordering).
    Reordered(Vec<usize>),
}

impl ColumnOrder {
    /// Construct new [`ColumnOrder`].
    pub fn reordered(reorder: Reordering) -> Self {
        debug_assert!(reorder.is_permutation());

        if reorder.is_identity() {
            Self::Default
        } else {
            Self::Reordered(reorder.to_vector())
        }
    }

    /// Returns whether this is the default column order
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Default)
    }

    /// Return the [`ColumnOrder`] that results from applying the given [`Reordering`].
    /// Assumes the the [`Reordering`] is a permutation.
    pub fn reorder(&self, reorder: &Reordering) -> Self {
        debug_assert!(reorder.is_permutation());

        if reorder.is_identity() {
            return self.clone();
        }

        let source = &match self {
            ColumnOrder::Default => (0..reorder.len_source()).collect(),
            ColumnOrder::Reordered(column_reorder) => column_reorder.clone(),
        };

        let result = reorder.apply_to(source);

        if result
            .iter()
            .enumerate()
            .all(|(index, element)| index == *element)
        {
            Self::Default
        } else {
            Self::Reordered(result)
        }
    }

    /// Return [`Reordering`] corresponding to this order.
    pub fn as_reordering(&self, arity: usize) -> Reordering {
        match self {
            ColumnOrder::Default => Reordering::default(arity),
            ColumnOrder::Reordered(reorder) => {
                debug_assert!(arity == reorder.len());
                Reordering::new(reorder.clone(), arity)
            }
        }
    }

    fn get(&self, index: usize) -> usize {
        match self {
            ColumnOrder::Default => index,
            ColumnOrder::Reordered(reorder) => reorder[index],
        }
    }

    fn find(&self, value: usize) -> Option<usize> {
        match self {
            ColumnOrder::Default => Some(value),
            ColumnOrder::Reordered(reorder) => reorder.iter().position(|&o| o == value),
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
        if matches!(self, Self::Default) && matches!(self, Self::Default) {
            return 0;
        }

        let arity = if let Self::Reordered(reorder) = self {
            reorder.len()
        } else {
            match to {
                ColumnOrder::Default => unreachable!(),
                ColumnOrder::Reordered(reorder) => reorder.len(),
            }
        };

        let mut current_score: usize = 0;
        let mut current_position: usize = 0;

        for current_index in 0..arity {
            let current_value = self.get(current_index);

            let position_other = to.find(current_value).expect(
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

impl Default for ColumnOrder {
    fn default() -> Self {
        Self::Default
    }
}

impl fmt::Debug for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnOrder::Default => f.write_str("[default]"),
            ColumnOrder::Reordered(reorder) => reorder.fmt(f),
        }
    }
}
