//! This module contains miscellaneous functionality used accross different management submodules.

use super::execution_plan::ColumnOrder;

/// For a given [ColumnOrder] searches a given list of [ColumnOrder]s
/// and returns the one that is "closest" to it.
///
/// If the the [ColumnOrder] is present in the list then it will be returned.
///
/// Returns `None` if the list of orders is empty.
pub(crate) fn closest_order<'a, OrderIter: Iterator<Item = &'a ColumnOrder>>(
    orders: OrderIter,
    order: &ColumnOrder,
) -> Option<(usize, &'a ColumnOrder)> {
    /// Provides a measure of how "difficult" it is to transform a column with this order into another.
    /// Say, `from = {0->2, 1->1, 2->0}` and `to = {0->1, 1->0, 2->2}`.
    /// Starting from position 0 in "from" one needs to skip one layer to reach the 2 in "to" (+1).
    /// Then we need to go back two layers to reach the 1 (+2)
    /// Finally, we go one layer down to reach 0 (+-0).
    /// This gives us an overall score of 3.
    /// Returned value is 0 if and only if from == to.
    #[allow(clippy::cast_possible_wrap)]
    fn distance(from: &ColumnOrder, to: &ColumnOrder) -> usize {
        let max_len = from.last_mapped().max(to.last_mapped()).unwrap_or(0);

        let to_inverted = to.invert();

        let mut current_score: usize = 0;
        let mut current_position_from: isize = -1;

        for position_to in 0..=max_len {
            let current_value = to_inverted.get(position_to);

            let position_from = from.get(current_value);
            let difference: isize = (position_from as isize) - current_position_from;

            let penalty: usize = if difference <= 0 {
                difference.unsigned_abs()
            } else {
                // Taking one forward step should not be punished
                (difference - 1) as usize
            };

            current_score += penalty;
            current_position_from = position_from as isize;
        }

        current_score
    }

    orders
        .enumerate()
        .min_by(|(_, x), (_, y)| distance(x, order).cmp(&distance(y, order)))
}
