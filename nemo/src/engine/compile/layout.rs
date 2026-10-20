//! How a predicate's columns map from an import source to its table.

/// How columns get from an import source to a predicate's table.
///
/// The *source columns* are the import's full column list.
/// A directive *keeps* some of them (a `skip` format drops the rest),
/// and the kept columns are the predicate's columns as the program sees them,
/// the ones its rules, facts, and declaration refer to.
/// The [import plan](super::import_plan::ImportPlan) *prunes* kept columns no rule reads,
/// and what is left is *delivered* to the table.
///
/// Every consumer asks this object for its widths and positions,
/// so no two of them can disagree on what a position means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnLayout {
    /// Per source column, whether the directive keeps it.
    kept: Vec<bool>,
    /// Per kept column, whether the plan prunes it.
    pruned: Vec<bool>,
}

impl ColumnLayout {
    /// Creates a layout keeping the given source columns.
    ///
    /// The pruning applies only if its length is the kept width.
    /// Otherwise the predicate is used at another arity, which is reported elsewhere,
    /// and nothing is pruned.
    pub(super) fn new(kept: Vec<bool>, pruned: Option<&[bool]>) -> Self {
        let width = kept.iter().filter(|&&keep| keep).count();
        let pruned = match pruned {
            Some(pruned) if pruned.len() == width => pruned.to_vec(),
            _ => vec![false; width],
        };
        Self { kept, pruned }
    }

    /// Creates a layout of the given width with every column kept.
    pub(super) fn whole(width: usize, pruned: Option<&[bool]>) -> Self {
        Self::new(vec![true; width], pruned)
    }

    /// Returns the source width: the number of source columns.
    pub(crate) fn source_width(&self) -> usize {
        self.kept.len()
    }

    /// Returns the kept width: the predicate's columns as the program sees
    /// them.
    pub(crate) fn kept(&self) -> usize {
        self.pruned.len()
    }

    /// Returns the delivered width: the table's arity.
    pub(crate) fn delivered(&self) -> usize {
        self.pruned.iter().filter(|&&pruned| !pruned).count()
    }

    /// Returns whether the plan prunes any column.
    pub(crate) fn prunes(&self) -> bool {
        self.pruned.contains(&true)
    }

    /// Returns whether a kept column, by its position among the kept ones, is
    /// pruned.
    pub(crate) fn is_pruned(&self, kept: usize) -> bool {
        self.pruned.get(kept).copied().unwrap_or(false)
    }

    /// Returns the position a kept column takes among the delivered ones,
    /// or `None` if it is pruned.
    pub(crate) fn delivered_position(&self, kept: usize) -> Option<usize> {
        if self.is_pruned(kept) {
            return None;
        }
        Some(
            self.pruned
                .iter()
                .take(kept)
                .filter(|&&pruned| !pruned)
                .count(),
        )
    }

    /// Returns the position among the kept columns of a delivered column,
    /// or `None` if fewer columns are delivered.
    pub(crate) fn kept_position(&self, delivered: usize) -> Option<usize> {
        self.pruned
            .iter()
            .enumerate()
            .filter(|&(_, &pruned)| !pruned)
            .nth(delivered)
            .map(|(kept, _)| kept)
    }

    /// Returns whether a source column reaches the table: kept and not pruned.
    pub(crate) fn delivers(&self, column: usize) -> bool {
        self.delivered_source(column).is_some()
    }

    /// Returns whether the directive skips a source column.
    pub(crate) fn skips(&self, column: usize) -> bool {
        !self.kept.get(column).copied().unwrap_or(false)
    }

    /// Returns the position a source column takes among the delivered ones,
    /// or `None` if the directive skips it or the plan prunes it.
    pub(crate) fn delivered_source(&self, column: usize) -> Option<usize> {
        if !self.kept.get(column).copied().unwrap_or(false) {
            return None;
        }
        let kept = self.kept[..column].iter().filter(|&&keep| keep).count();
        self.delivered_position(kept)
    }

    /// Returns, per source column, whether it is delivered.
    pub(crate) fn delivered_columns(&self) -> Vec<bool> {
        (0..self.source_width())
            .map(|column| self.delivers(column))
            .collect()
    }

    /// Returns a list over the kept columns reduced to the delivered ones.
    ///
    /// A list of another width is returned unchanged:
    /// it belongs to a use of the predicate at another arity, which is reported elsewhere.
    pub(crate) fn deliver<T>(&self, kept: Vec<T>) -> Vec<T> {
        if kept.len() != self.kept() {
            return kept;
        }
        kept.into_iter()
            .zip(&self.pruned)
            .filter(|(_, pruned)| !**pruned)
            .map(|(item, _)| item)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_and_pruning_compose_over_the_source_columns() {
        // Four source columns: the directive skips the second, and the
        // plan prunes the first of the three kept.
        let layout = ColumnLayout::new(vec![true, false, true, true], Some(&[true, false, false]));
        assert_eq!(layout.source_width(), 4);
        assert_eq!(layout.kept(), 3);
        assert_eq!(layout.delivered(), 2);
        assert_eq!(layout.delivered_columns(), [false, false, true, true]);
        assert_eq!(layout.delivered_position(0), None);
        assert_eq!(layout.delivered_position(2), Some(1));
        assert_eq!(layout.kept_position(0), Some(1));
        assert_eq!(layout.kept_position(1), Some(2));
        assert_eq!(layout.kept_position(2), None);
        assert_eq!(layout.deliver(vec!['a', 'b', 'c']), ['b', 'c']);
    }

    #[test]
    fn pruning_at_another_width_does_not_apply() {
        let layout = ColumnLayout::whole(2, Some(&[true, false, false]));
        assert!(!layout.prunes());
        assert_eq!(layout.delivered(), 2);
        assert_eq!(layout.deliver(vec![1, 2, 3]), [1, 2, 3]);
    }
}
