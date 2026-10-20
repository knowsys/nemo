//! Column orders, as permutations of a table's columns.

/// A column order: a permutation that maps each target position to the source column it holds.
///
/// `order.get(t) = s` means that column `t` of the rows in this order
/// carries column `s` of the table.
///
/// # Example
///
/// ```
/// use nemo_physical::management::ColumnOrder;
///
/// // The columns (a, b, c) of a table, laid out as (c, a, b).
/// let order = ColumnOrder::new(&[2, 0, 1]).expect("a permutation");
/// assert_eq!(order.get(0), 2);
/// assert_eq!(order.indices(), [2, 0, 1]);
/// assert_eq!(ColumnOrder::identity(3).indices(), [0, 1, 2]);
///
/// // Each column appears exactly once.
/// assert!(ColumnOrder::new(&[0, 0]).is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnOrder(Box<[usize]>);

impl ColumnOrder {
    /// Returns the identity order of the given arity.
    pub fn identity(arity: usize) -> Self {
        Self((0..arity).collect())
    }

    /// Creates an order from target-to-source indices,
    /// or returns `None` if they are not a permutation of `0..len`.
    pub fn new(order: &[usize]) -> Option<Self> {
        let arity = order.len();
        let mut seen = vec![false; arity];

        for &source in order {
            if source >= arity || seen[source] {
                return None;
            }
            seen[source] = true;
        }

        Some(Self(order.into()))
    }

    /// Returns the number of positions.
    pub fn arity(&self) -> usize {
        self.0.len()
    }

    /// Returns the source column held at a target position.
    pub fn get(&self, target: usize) -> usize {
        self.0[target]
    }

    /// Returns the target position that holds a source column.
    pub(crate) fn position_of(&self, source: usize) -> usize {
        self.0
            .iter()
            .position(|&held| held == source)
            .expect("permutations cover every source column")
    }

    /// Returns the target-to-source indices:
    /// entry `t` is [get](Self::get) of `t`.
    pub fn indices(&self) -> &[usize] {
        &self.0
    }

    /// Returns the indices that rearrange columns laid out in the `source` order into this order:
    /// entry `t` is the position, in the source layout, of the column this order holds at `t`.
    pub(crate) fn reorder_from(&self, source: &Self) -> Vec<usize> {
        debug_assert_eq!(self.arity(), source.arity(), "orders describe one table");
        self.0
            .iter()
            .map(|&column| source.position_of(column))
            .collect()
    }

    /// Returns the length of the longest common prefix of two orders.
    pub(crate) fn shared_prefix(&self, other: &Self) -> usize {
        self.0
            .iter()
            .zip(&other.0)
            .take_while(|(left, right)| left == right)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construction_validates_permutations() {
        assert!(ColumnOrder::new(&[2, 0, 1]).is_some());
        assert!(ColumnOrder::new(&[]).is_some());
        assert!(ColumnOrder::new(&[0, 0]).is_none());
        assert!(ColumnOrder::new(&[1, 2]).is_none());
        assert_eq!(ColumnOrder::identity(3).indices(), [0, 1, 2]);
    }

    #[test]
    fn reordering_between_layouts() {
        // Source layout (2, 0, 1); target layout (0, 1, 2): the identity
        // wants column 0 first, which the source stores at position 1.
        let source = ColumnOrder::new(&[2, 0, 1]).expect("valid");
        let target = ColumnOrder::identity(3);
        assert_eq!(target.reorder_from(&source), vec![1, 2, 0]);
        assert_eq!(source.reorder_from(&source), vec![0, 1, 2]);
    }

    #[test]
    fn shared_prefixes_count_equal_leading_positions() {
        let base = ColumnOrder::new(&[0, 1, 2, 3]).expect("valid");
        let close = ColumnOrder::new(&[0, 1, 3, 2]).expect("valid");
        let far = ColumnOrder::new(&[3, 2, 1, 0]).expect("valid");
        assert_eq!(base.shared_prefix(&close), 2);
        assert_eq!(base.shared_prefix(&far), 0);
        assert_eq!(base.shared_prefix(&base), 4);
    }
}
